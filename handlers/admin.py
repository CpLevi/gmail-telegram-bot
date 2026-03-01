"""
EarnX Gmail Bot — Admin Handlers
Admin panel, Gmail queue, withdrawal queue, user management, broadcast, stats, wallet ops.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    ADMIN_ID, ADMIN_USERS_PER_PAGE,
    ADMIN_WITHDRAWALS_PER_PAGE, USER_SEARCH, BROADCAST_MSG,
    WALLET_AMOUNT, WALLET_REASON, ADMIN_SET_PRICE,
)
from database import get_db
from utils import (
    validate_page, round_decimal, log_audit, notify_user,
    safe_edit_or_reply, mask_email, get_gmail_rate, set_gmail_rate,
)

logger = logging.getLogger(__name__)


# ==================== ADMIN PANEL ====================

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all admin callbacks."""
    q = update.callback_query
    await q.answer()
    d = q.data

    if q.from_user.id != ADMIN_ID:
        return

    # ── ADMIN HOME ──
    if d == "admin":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            users = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='pending'")
            pg = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
            pw = list(c.fetchone().values())[0]

        text = (
            f"🔐 <b>Admin Panel</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Users: <b>{users}</b>\n"
            f"📧 Pending Gmail: <b>{pg}</b>\n"
            f"💸 Pending Withdrawals: <b>{pw}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )

        kb = [
            [InlineKeyboardButton("📧 Gmail Queue", callback_data="gmail_queue")],
            [InlineKeyboardButton("💸 Withdrawals", callback_data="withdrawal_queue")],
            [InlineKeyboardButton("👥 User Management", callback_data="user_mgmt")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── GMAIL QUEUE ──
    elif d == "gmail_queue" or d.startswith("gmail_queue_"):
        page = validate_page(d.split("_")[-1]) if "_" in d else 0
        offset = page * ADMIN_USERS_PER_PAGE

        with get_db() as conn:
            c = conn.cursor()

            # Count totals
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='pending'")
            total_pending = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='in_review'")
            total_in_review = list(c.fetchone().values())[0]
            total_all = total_pending + total_in_review

            # Users with pending/in_review submissions
            c.execute("""SELECT DISTINCT u.user_id, u.first_name, u.username, COUNT(g.id) as cnt
                         FROM gmail g JOIN users u ON g.user_id = u.user_id
                         WHERE g.status IN ('pending', 'in_review')
                         GROUP BY u.user_id, u.first_name, u.username
                         ORDER BY cnt DESC LIMIT %s OFFSET %s""", (ADMIN_USERS_PER_PAGE, offset))
            users_pending = c.fetchall()

            c.execute("SELECT COUNT(DISTINCT user_id) FROM gmail WHERE status IN ('pending', 'in_review')")
            total_users = list(c.fetchone().values())[0]

        if total_all == 0:
            await safe_edit_or_reply(
                q, "✅ <b>No pending submissions</b>\n\nAll caught up! 🎉",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]])
            )
            return

        total_pages = max(1, (total_users + ADMIN_USERS_PER_PAGE - 1) // ADMIN_USERS_PER_PAGE)
        text = (
            f"📧 <b>Gmail Review Queue</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📬 Total Pending: <b>{total_all}</b>\n"
            f"👥 Users: <b>{total_users}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<i>Tap a user to review their submissions:</i>\n\n"
        )

        kb = []
        for row in users_pending:
            uid, name, username, cnt = row['user_id'], row['first_name'], row['username'], row['cnt']
            text += f"📧 {name} (@{username or 'N/A'}) — <b>{cnt}</b>\n"
            kb.append([InlineKeyboardButton(f"📧 {name} — {cnt} pending", callback_data=f"review_user_{uid}_0")])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"gmail_queue_{page - 1}"))
        if offset + ADMIN_USERS_PER_PAGE < total_users:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"gmail_queue_{page + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── ONE-CARD REVIEW (per user) ──
    elif d.startswith("review_user_"):
        parts = d.split("_")
        uid = int(parts[2])
        idx = validate_page(parts[3]) if len(parts) > 3 else 0  # index within user's pending list

        with get_db() as conn:
            c = conn.cursor()

            # Get total count for this user
            c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status IN ('pending', 'in_review')", (uid,))
            total_count = list(c.fetchone().values())[0]

            if total_count == 0:
                await q.answer("✅ All reviewed for this user!", show_alert=True)
                q.data = "gmail_queue_0"
                await admin_callback(update, context)
                return

            # Clamp index
            if idx >= total_count:
                idx = total_count - 1

            # Get the specific submission
            c.execute("""SELECT id, email, password, reward, submit_date, status,
                        task_id, assigned_first_name, assigned_last_name, assigned_dob,
                        assigned_gender, assigned_email, assigned_password
                        FROM gmail WHERE user_id=%s AND status IN ('pending', 'in_review')
                        ORDER BY submit_date ASC
                        LIMIT 1 OFFSET %s""", (uid, idx))
            gmail = c.fetchone()

            # Get user info
            c.execute("SELECT first_name, username FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()

        if not gmail or not user_info:
            await q.answer("Not found", show_alert=True)
            return

        gid = gmail['id']
        name = user_info['first_name']
        username = user_info['username'] or 'N/A'

        # Build the review card with side-by-side comparison
        text = (
            f"📋 <b>Review #{gid}</b>  ({idx + 1}/{total_count})\n"
            f"👤 {name} (@{username}) • ID: {uid}\n\n"
        )

        # Show assigned vs submitted comparison
        has_task = gmail.get('assigned_first_name')
        if has_task:
            text += (
                f"━━━━ 📝 <b>ASSIGNED TASK</b> ━━━━\n"
                f"👤 Name: <code>{gmail['assigned_first_name']} {gmail.get('assigned_last_name', '')}</code>\n"
                f"🎂 DOB: <code>{gmail.get('assigned_dob', 'N/A')}</code>\n"
                f"⚧️ Gender: <code>{'Male' if gmail.get('assigned_gender') == 'M' else 'Female'}</code>\n"
                f"📧 Email: <code>{gmail.get('assigned_email', 'N/A')}</code>\n"
                f"🔑 Password: <code>{gmail.get('assigned_password', 'N/A')}</code>\n\n"
                f"━━━━ ✅ <b>SUBMITTED</b> ━━━━━━━\n"
                f"📧 Email: <code>{gmail['email']}</code>\n"
                f"🔑 Password: <code>{gmail['password']}</code>\n\n"
            )

            # Auto-check if they match
            assigned_email = (gmail.get('assigned_email') or '').lower().strip()
            submitted_email = (gmail.get('email') or '').lower().strip()
            assigned_pwd = gmail.get('assigned_password') or ''
            submitted_pwd = gmail.get('password') or ''

            match_email = assigned_email == submitted_email
            match_pwd = assigned_pwd == submitted_pwd

            text += (
                f"━━━━ 🔍 <b>MATCH CHECK</b> ━━━━\n"
                f"📧 Email: {'✅ Match' if match_email else '❌ Mismatch'}\n"
                f"🔑 Password: {'✅ Match' if match_pwd else '❌ Mismatch'}\n"
            )
        else:
            # Legacy submission (no task details)
            text += (
                f"━━━━ 📧 <b>SUBMISSION</b> ━━━━━\n"
                f"📧 Email: <code>{gmail['email']}</code>\n"
                f"🔑 Password: <code>{gmail['password']}</code>\n"
                f"<i>⚠️ Legacy submission (no task data)</i>\n"
            )

        text += (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Reward: <b>₹{float(gmail['reward']):.2f}</b>\n"
            f"📅 Submitted: {(gmail.get('submit_date') or '')[:16]}\n"
        )

        # Action buttons
        kb = [
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{gid}_{uid}_{idx}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{gid}_{uid}_{idx}"),
            ],
        ]

        # Navigation within user's submissions
        nav = []
        if idx > 0:
            nav.append(InlineKeyboardButton(f"⬅️ Prev", callback_data=f"review_user_{uid}_{idx - 1}"))
        if idx < total_count - 1:
            nav.append(InlineKeyboardButton(f"Next ➡️", callback_data=f"review_user_{uid}_{idx + 1}"))
        if nav:
            kb.append(nav)

        # Batch actions
        if total_count > 1:
            kb.append([
                InlineKeyboardButton(f"✅ Approve All ({total_count})", callback_data=f"approve_all_{uid}"),
                InlineKeyboardButton(f"❌ Reject All ({total_count})", callback_data=f"reject_all_{uid}"),
            ])

        kb.append([InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue_0")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── APPROVE SINGLE ──
    elif d.startswith("approve_") and not d.startswith("approve_all_"):
        parts = d.split("_")
        gid = int(parts[1])
        uid = int(parts[2]) if len(parts) > 2 else None
        idx = validate_page(parts[3]) if len(parts) > 3 else 0

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='approved', review_date=%s
                    WHERE id=%s AND status IN ('pending', 'in_review')
                    RETURNING user_id, reward, email
                """, (datetime.now().isoformat(), gid))
                result = c.fetchone()

                if not result:
                    await q.answer("Already processed", show_alert=True)
                    return

                uid_db, reward, email = result['user_id'], round_decimal(result['reward']), result['email']
                uid = uid if uid else uid_db

                # Check first approval for referral
                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='approved'", (uid,))
                approval_count = list(c.fetchone().values())[0]
                is_first = (approval_count == 1)

                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+1 WHERE user_id=%s",
                          (reward, uid))

                # Referral reward
                if is_first:
                    c.execute("""
                        UPDATE referrals SET rewarded=1
                        WHERE referred_id=%s AND rewarded=0
                        RETURNING referrer_id, reward
                    """, (uid,))
                    ref = c.fetchone()
                    if ref:
                        c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s",
                                  (round_decimal(ref['reward']), ref['referrer_id']))
                        c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
                        rname = c.fetchone()['first_name']
                        await notify_user(context, ref['referrer_id'],
                            f"🎉 <b>Referral Bonus!</b>\n\n"
                            f"<b>{rname}</b> completed their first task.\n"
                            f"₹{float(ref['reward']):.2f} credited!")

                conn.commit()
                log_audit("approve_gmail", ADMIN_ID, uid, f"#{gid} — ₹{float(reward):.2f}")

                await notify_user(context, uid,
                    f"✅ <b>Gmail Verified!</b>\n\n"
                    f"📧 {email}\n"
                    f"💰 ₹{float(reward):.2f} credited!")

                await q.answer(f"✅ Approved — ₹{float(reward):.2f}", show_alert=True)
                # Navigate to next card (same idx since current one was removed)
                q.data = f'review_user_{uid}_{idx}'
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error approving {gid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── REJECT SINGLE ──
    elif d.startswith("reject_") and not d.startswith("reject_all_"):
        parts = d.split("_")
        gid = int(parts[1])
        uid = int(parts[2]) if len(parts) > 2 else None
        idx = validate_page(parts[3]) if len(parts) > 3 else 0

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='rejected', review_date=%s, rejection_reason=%s
                    WHERE id=%s AND status IN ('pending', 'in_review')
                    RETURNING user_id, email
                """, (datetime.now().isoformat(), "Wrong Password or Invalid Account", gid))
                result = c.fetchone()

                if not result:
                    await q.answer("Already processed", show_alert=True)
                    return

                uid_db, email = result['user_id'], result['email']
                uid = uid if uid else uid_db
                conn.commit()

                log_audit("reject_gmail", ADMIN_ID, uid, f"#{gid}")

                await notify_user(context, uid,
                    f"❌ <b>Gmail Rejected</b>\n\n"
                    f"📧 {email}\n"
                    f"Reason: Wrong Password or Invalid Account\n\n"
                    f"<i>Please create accounts exactly as shown in tasks.</i>")

                await q.answer("❌ Rejected", show_alert=True)
                # Navigate to next card (same idx since current one was removed)
                q.data = f'review_user_{uid}_{idx}'
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error rejecting {gid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── APPROVE ALL ──
    elif d.startswith("approve_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT id, reward, email FROM gmail WHERE user_id=%s AND status IN ('pending', 'in_review')", (uid,))
                gmails = c.fetchall()

                if not gmails:
                    await q.answer("No pending", show_alert=True)
                    return

                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='approved'", (uid,))
                is_first = list(c.fetchone().values())[0] == 0

                total_reward = sum(round_decimal(g['reward']) for g in gmails)
                count = len(gmails)

                c.execute("""
                    UPDATE gmail SET status='approved', review_date=%s
                    WHERE user_id=%s AND status IN ('pending', 'in_review')
                """, (datetime.now().isoformat(), uid))

                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+%s WHERE user_id=%s",
                          (total_reward, count, uid))

                if is_first:
                    c.execute("""
                        UPDATE referrals SET rewarded=1
                        WHERE referred_id=%s AND rewarded=0
                        RETURNING referrer_id, reward
                    """, (uid,))
                    ref = c.fetchone()
                    if ref:
                        c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s",
                                  (round_decimal(ref['reward']), ref['referrer_id']))
                        c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
                        rname = c.fetchone()['first_name']
                        await notify_user(context, ref['referrer_id'],
                            f"🎉 <b>Referral Bonus!</b>\n\n{rname} completed their first task.\n₹{float(ref['reward']):.2f} credited!")

                conn.commit()
                log_audit("approve_all_gmail", ADMIN_ID, uid, f"{count} — ₹{float(total_reward):.2f}")

                email_list = "\n".join([f"• {mask_email(g['email'])}" for g in gmails[:5]])
                if len(gmails) > 5:
                    email_list += f"\n• ...and {len(gmails) - 5} more"

                await notify_user(context, uid,
                    f"✅ <b>All Gmail Verified!</b>\n\n"
                    f"Total: {count} accounts\n"
                    f"₹{float(total_reward):.2f} credited!\n\n{email_list}")

                await q.answer(f"✅ {count} approved — ₹{float(total_reward):.2f}", show_alert=True)

                await safe_edit_or_reply(q,
                    f"✅ <b>Batch Approved</b>\n\nUser: {uid}\nCount: {count}\nTotal: ₹{float(total_reward):.2f}",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue_0")]]))

        except Exception as e:
            logger.error(f"Error approve all {uid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── REJECT ALL ──
    elif d.startswith("reject_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status IN ('pending', 'in_review')", (uid,))
                count = list(c.fetchone().values())[0]

                if count == 0:
                    await q.answer("No pending", show_alert=True)
                    return

                c.execute("""
                    UPDATE gmail SET status='rejected', review_date=%s, rejection_reason=%s
                    WHERE user_id=%s AND status IN ('pending', 'in_review')
                """, (datetime.now().isoformat(), "Quality issues", uid))
                conn.commit()

                log_audit("reject_all_gmail", ADMIN_ID, uid, f"{count} rejected")

                await notify_user(context, uid,
                    f"❌ <b>Gmail Rejected</b>\n\n"
                    f"Total: {count} accounts\n"
                    f"Reason: Quality issues\n\n"
                    f"<i>Ensure accounts match assigned task details exactly.</i>")

                await q.answer(f"❌ {count} rejected", show_alert=True)

                await safe_edit_or_reply(q,
                    f"❌ <b>Batch Rejected</b>\n\nUser: {uid}\nCount: {count}",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue_0")]]))

        except Exception as e:
            logger.error(f"Error reject all {uid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── WITHDRAWAL QUEUE ──
    elif d == "withdrawal_queue" or d.startswith("withdrawal_queue_"):
        page = validate_page(d.split("_")[-1]) if "_" in d else 0

        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
            total_pending = list(c.fetchone().values())[0]

            if total_pending == 0:
                await safe_edit_or_reply(q,
                    "✅ <b>No pending withdrawals</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]]))
                return

            if page < 0:
                page = 0
            elif page >= total_pending:
                page = total_pending - 1

            c.execute("""SELECT w.id, w.amount, w.fee, w.final_amount, w.method, w.payment_info, w.request_date,
                         u.first_name, u.username, u.user_id
                         FROM withdrawals w JOIN users u ON w.user_id = u.user_id
                         WHERE w.status='pending' ORDER BY w.request_date LIMIT 1 OFFSET %s""", (page,))
            w = c.fetchone()

        if w:
            text = (
                f"💸 <b>Withdrawal #{w['id']}</b>\n"
                f"<i>Position {page + 1} of {total_pending}</i>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 User: {w['first_name']} (@{w['username'] or 'N/A'})\n"
                f"🆔 User ID: {w['user_id']}\n"
                f"💰 Amount: <b>₹{float(w['amount']):.2f}</b>\n"
                f"📊 Fee: ₹{float(w['fee']):.2f}\n"
                f"💵 Final: <b>₹{float(w['final_amount']):.2f}</b>\n"
                f"💳 Method: {w['method'].upper()}\n"
                f"📋 Payment: <code>{w['payment_info']}</code>\n"
                f"📅 Date: {w['request_date'][:16]}\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )

            kb = [
                [InlineKeyboardButton("✅ Approve", callback_data=f"withdraw_approve_{w['id']}_{page}"),
                 InlineKeyboardButton("❌ Reject", callback_data=f"withdraw_reject_{w['id']}_{page}")],
            ]
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"withdrawal_queue_{page - 1}"))
            if page < total_pending - 1:
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"withdrawal_queue_{page + 1}"))
            if nav:
                kb.append(nav)
            kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin")])
            await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── WITHDRAW APPROVE CONFIRM ──
    elif d.startswith("withdraw_approve_") and not d.startswith("withdraw_approve_confirm_"):
        parts = d.split("_")
        wid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0

        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT w.amount, w.final_amount, w.method, w.payment_info,
                         u.first_name, u.username, u.user_id
                         FROM withdrawals w JOIN users u ON w.user_id = u.user_id
                         WHERE w.id=%s AND w.status='pending'""", (wid,))
            result = c.fetchone()

        if not result:
            await q.answer("Already processed", show_alert=True)
            return

        text = (
            f"⚠️ <b>Confirm Approval</b>\n\n"
            f"Withdrawal #{wid}\n"
            f"User: {result['first_name']}\n"
            f"Amount: ₹{float(result['amount']):.2f}\n"
            f"Final: ₹{float(result['final_amount']):.2f}\n"
            f"Method: {result['method'].upper()}\n"
            f"Payment: {result['payment_info']}\n\n"
            f"⚠️ <i>This cannot be undone.</i>"
        )

        kb = [
            [InlineKeyboardButton("✅ Confirm", callback_data=f"withdraw_approve_confirm_{wid}_{page}"),
             InlineKeyboardButton("❌ Cancel", callback_data=f"withdrawal_queue_{page}")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── WITHDRAW APPROVE EXECUTE ──
    elif d.startswith("withdraw_approve_confirm_"):
        parts = d.split("_")
        wid = int(parts[3])
        page = int(parts[4]) if len(parts) > 4 else 0

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE withdrawals SET status='approved', processed_date=%s
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, amount, final_amount
                """, (datetime.now().isoformat(), wid))
                result = c.fetchone()

                if not result:
                    await q.answer("Already processed", show_alert=True)
                    return

                uid = result['user_id']
                conn.commit()
                log_audit("approve_withdrawal", ADMIN_ID, uid, f"#{wid} — ₹{float(result['amount']):.2f}")

                await notify_user(context, uid,
                    f"✅ <b>Withdrawal Approved!</b>\n\n"
                    f"ID: #{wid}\n"
                    f"Amount: ₹{float(result['amount']):.2f}\n"
                    f"Final: ₹{float(result['final_amount']):.2f}\n\n"
                    f"Payment processed. Check your account!")

                await q.answer("✅ Approved", show_alert=True)
                q.data = f"withdrawal_queue_{page}"
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error: {e}")
            await q.answer("Error", show_alert=True)

    # ── WITHDRAW REJECT CONFIRM ──
    elif d.startswith("withdraw_reject_") and not d.startswith("withdraw_reject_confirm_"):
        parts = d.split("_")
        wid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0

        text = (
            f"⚠️ <b>Confirm Rejection</b>\n\n"
            f"Withdrawal #{wid}\n"
            f"💰 Amount will be refunded.\n\n"
            f"Choose reason:"
        )

        kb = [
            [InlineKeyboardButton("❌ Invalid Payment Info", callback_data=f"withdraw_reject_confirm_{wid}_{page}_invalid")],
            [InlineKeyboardButton("❌ Duplicate Request", callback_data=f"withdraw_reject_confirm_{wid}_{page}_duplicate")],
            [InlineKeyboardButton("❌ Suspicious Activity", callback_data=f"withdraw_reject_confirm_{wid}_{page}_suspicious")],
            [InlineKeyboardButton("❌ Other", callback_data=f"withdraw_reject_confirm_{wid}_{page}_other")],
            [InlineKeyboardButton("🔙 Cancel", callback_data=f"withdrawal_queue_{page}")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── WITHDRAW REJECT EXECUTE ──
    elif d.startswith("withdraw_reject_confirm_"):
        parts = d.split("_")
        wid = int(parts[3])
        page = int(parts[4])
        reason_code = parts[5] if len(parts) > 5 else "other"

        reason_map = {
            "invalid": "Invalid payment information",
            "duplicate": "Duplicate withdrawal request",
            "suspicious": "Suspicious activity detected",
            "other": "Does not meet requirements",
        }
        rejection_reason = reason_map.get(reason_code, "Does not meet requirements")

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE withdrawals SET status='rejected', processed_date=%s, rejection_reason=%s
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, amount
                """, (datetime.now().isoformat(), rejection_reason, wid))
                result = c.fetchone()

                if not result:
                    await q.answer("Already processed", show_alert=True)
                    return

                uid, amount = result['user_id'], round_decimal(result['amount'])
                c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", (amount, uid))
                conn.commit()

                log_audit("reject_withdrawal", ADMIN_ID, uid, f"#{wid} — ₹{float(amount):.2f} refunded")

                await notify_user(context, uid,
                    f"❌ <b>Withdrawal Rejected</b>\n\n"
                    f"ID: #{wid}\n"
                    f"Amount: ₹{float(amount):.2f}\n"
                    f"Reason: {rejection_reason}\n\n"
                    f"₹{float(amount):.2f} refunded to your balance.")

                await q.answer("❌ Rejected & refunded", show_alert=True)
                q.data = f"withdrawal_queue_{page}"
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error: {e}")
            await q.answer("Error", show_alert=True)

    # ── USER MANAGEMENT ──
    elif d == "user_mgmt":
        await safe_edit_or_reply(
            q,
            "👥 <b>User Management</b>\n\nSend user ID:\n\n/cancel to abort",
        )
        return USER_SEARCH

    # ── BROADCAST ──
    elif d == "broadcast":
        await safe_edit_or_reply(
            q,
            "📢 <b>Broadcast</b>\n\nSend message to all users:\n\n/cancel to abort",
        )
        return BROADCAST_MSG

    # ── STATS ──
    elif d == "stats":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            total_users = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='approved'")
            approved = list(c.fetchone().values())[0]
            c.execute("SELECT SUM(balance) FROM users")
            total_bal = float(list(c.fetchone().values())[0] or 0)
            c.execute("SELECT SUM(reward) FROM gmail WHERE status='approved'")
            paid = float(list(c.fetchone().values())[0] or 0)
            c.execute("SELECT COUNT(*) FROM referrals WHERE rewarded=1")
            refs = list(c.fetchone().values())[0]
            c.execute("SELECT SUM(reward) FROM referrals WHERE rewarded=1")
            ref_paid = float(list(c.fetchone().values())[0] or 0)
            c.execute("SELECT SUM(final_amount) FROM withdrawals WHERE status='approved'")
            withdrawn = float(list(c.fetchone().values())[0] or 0)
            c.execute("SELECT SUM(fee) FROM withdrawals WHERE status='approved'")
            fees = float(list(c.fetchone().values())[0] or 0)

        text = (
            f"📊 <b>Statistics</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Users: <b>{total_users}</b>\n"
            f"✅ Approved Gmail: <b>{approved}</b>\n"
            f"👥 Referrals: <b>{refs}</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 User Balance: <b>₹{total_bal:.2f}</b>\n"
            f"📧 Gmail Paid: ₹{paid:.2f}\n"
            f"👥 Referral Paid: ₹{ref_paid:.2f}\n"
            f"📊 Total Paid: <b>₹{paid + ref_paid:.2f}</b>\n"
            f"💸 Withdrawn: ₹{withdrawn:.2f}\n"
            f"📊 Fees Collected: <b>₹{fees:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )

        kb = [[InlineKeyboardButton("🔙 Back", callback_data="admin")]]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── ADMIN SETTINGS ──
    elif d == "admin_settings":
        current_rate = float(get_gmail_rate())
        text = (
            f"⚙️ <b>Bot Settings</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Gmail Rate:</b> ₹{current_rate:.0f}/account\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"This rate applies to <b>all users</b>.\n"
            f"Change it anytime below."
        )
        kb = [
            [InlineKeyboardButton(f"💰 Change Price (₹{current_rate:.0f})", callback_data="set_price")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── SET PRICE (entry point) ──
    elif d == "set_price":
        current_rate = float(get_gmail_rate())
        await safe_edit_or_reply(
            q,
            f"💰 <b>Change Gmail Rate</b>\n\n"
            f"Current rate: <b>₹{current_rate:.0f}</b>/account\n\n"
            f"Send the new price (in ₹):\n\n"
            f"<i>Examples: 15, 20, 25, 30, 50</i>\n\n"
            f"/cancel to abort",
        )
        return ADMIN_SET_PRICE

    # ── BLOCK / UNBLOCK ──
    elif d.startswith("block_"):
        uid = int(d.split("_")[1])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("UPDATE users SET is_blocked = 1 - is_blocked WHERE user_id=%s", (uid,))
                c.execute("SELECT is_blocked FROM users WHERE user_id=%s", (uid,))
                blocked = list(c.fetchone().values())[0]
                conn.commit()

            log_audit("block_user" if blocked else "unblock_user", ADMIN_ID, uid, "")
            await q.answer(f"{'🚫 Blocked' if blocked else '✅ Unblocked'}", show_alert=True)

            try:
                msg = "⛔ Your account has been blocked" if blocked else "✅ Your account has been unblocked"
                await context.bot.send_message(uid, msg, parse_mode="HTML")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error: {e}")
            await q.answer("Error", show_alert=True)

    # ── WALLET CONFIRM ──
    elif d.startswith("wallet_confirm_"):
        parts = d.split("_")
        uid = int(parts[2])

        action = context.user_data.get('wallet_action')
        amount = context.user_data.get('wallet_amount')
        reason = context.user_data.get('wallet_reason')
        balance_before = context.user_data.get('wallet_current_balance')

        if not all([action, amount, reason, balance_before is not None]):
            await q.answer("Session expired", show_alert=True)
            context.user_data.clear()
            return

        amount = round_decimal(amount)
        balance_before = round_decimal(balance_before)

        if action == "add":
            balance_after = balance_before + amount
        else:
            balance_after = balance_before - amount
            if balance_after < 0:
                await q.answer("Insufficient balance", show_alert=True)
                context.user_data.clear()
                return

        balance_after = round_decimal(balance_after)

        try:
            with get_db() as conn:
                c = conn.cursor()
                if action == "add":
                    c.execute("""UPDATE users SET balance=balance+%s WHERE user_id=%s RETURNING balance""",
                              (amount, uid))
                else:
                    c.execute("""UPDATE users SET balance=balance-%s WHERE user_id=%s AND balance>=%s RETURNING balance""",
                              (amount, uid, amount))

                result = c.fetchone()
                if not result:
                    await q.answer("Update failed", show_alert=True)
                    context.user_data.clear()
                    return

                final_balance = float(result['balance'])

                c.execute("""INSERT INTO admin_wallet_logs
                    (admin_id, user_id, action, amount, reason, balance_before, balance_after, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (ADMIN_ID, uid, action.upper(), amount, reason, balance_before, balance_after,
                     datetime.now().isoformat()))

                conn.commit()
                log_audit(f"wallet_{action}", ADMIN_ID, uid, f"₹{float(amount):.2f} | {reason}")

                action_word = "added to" if action == "add" else "deducted from"
                await notify_user(context, uid,
                    f"💰 ₹{float(amount):.2f} {action_word} your wallet.\nReason: {reason}")

                await q.answer("✅ Done", show_alert=True)
                await q.message.reply_text(
                    f"✅ <b>Balance Updated</b>\n\n"
                    f"User: {uid}\n"
                    f"Action: {action.upper()}\n"
                    f"Amount: ₹{float(amount):.2f}\n"
                    f"Before: ₹{float(balance_before):.2f}\n"
                    f"After: ₹{final_balance:.2f}\n"
                    f"Reason: {reason}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="admin")]]),
                    parse_mode="HTML"
                )
                context.user_data.clear()
        except Exception as e:
            logger.error(f"Error wallet: {e}")
            await q.answer("Error", show_alert=True)
            context.user_data.clear()

    # ── WALLET CANCEL ──
    elif d == "wallet_cancel":
        context.user_data.clear()
        await q.answer("Cancelled", show_alert=True)
        await q.message.reply_text(
            "Cancelled.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin", callback_data="admin")]]),
            parse_mode="HTML"
        )


# ==================== MESSAGE HANDLERS (Admin) ====================

async def start_wallet_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for wallet add/deduct operations."""
    q = update.callback_query
    await q.answer()

    if q.from_user.id != ADMIN_ID:
        await q.answer("Unauthorized", show_alert=True)
        return ConversationHandler.END

    d = q.data
    parts = d.split("_")
    action = parts[1]
    uid = int(parts[2])

    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT first_name, balance FROM users WHERE user_id=%s", (uid,))
        result = c.fetchone()

    if not result:
        await q.answer("User not found", show_alert=True)
        return ConversationHandler.END

    context.user_data['wallet_action'] = action
    context.user_data['wallet_target_user'] = uid
    context.user_data['wallet_target_name'] = result['first_name']
    context.user_data['wallet_current_balance'] = float(result['balance'])

    action_text = "ADD" if action == "add" else "DEDUCT"
    await q.message.reply_text(
        f"💰 <b>Balance {action_text}</b>\n\n"
        f"User: {result['first_name']} (ID: {uid})\n"
        f"Current: ₹{float(result['balance']):.2f}\n\n"
        f"Enter amount (₹):\n\n/cancel to abort",
        parse_mode="HTML"
    )
    return WALLET_AMOUNT


async def receive_user_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user search by ID."""
    user_input = update.message.text.strip()

    if not user_input.isdigit() or len(user_input) > 15:
        await update.message.reply_text("❌ Invalid user ID format.", parse_mode="HTML")
        return USER_SEARCH

    try:
        uid = int(user_input)

        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT username, first_name, balance, total_gmail, approved_gmail,
                         is_blocked, joined_date FROM users WHERE user_id=%s""", (uid,))
            result = c.fetchone()

        if result:
            status = "🚫 Blocked" if result['is_blocked'] else "✅ Active"
            text = (
                f"👤 <b>User Info</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 ID: <code>{uid}</code>\n"
                f"👤 Name: {result['first_name']}\n"
                f"📱 Username: @{result['username'] or 'N/A'}\n"
                f"📊 Status: {status}\n\n"
                f"💰 Balance: ₹{float(result['balance']):.2f}\n"
                f"📧 Gmail: {result['approved_gmail']}/{result['total_gmail']}\n"
                f"📅 Joined: {result['joined_date'][:10]}\n"
                f"━━━━━━━━━━━━━━━━━━━━"
            )

            kb = [
                [InlineKeyboardButton("➕ Add Balance", callback_data=f"wallet_add_{uid}"),
                 InlineKeyboardButton("➖ Deduct", callback_data=f"wallet_deduct_{uid}")],
                [InlineKeyboardButton("🚫 Block" if not result['is_blocked'] else "✅ Unblock",
                                      callback_data=f"block_{uid}")],
                [InlineKeyboardButton("🔙 Back", callback_data="admin")],
            ]
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
        else:
            await update.message.reply_text("❌ User not found", parse_mode="HTML")

        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("⚠️ Error occurred", parse_mode="HTML")
        return ConversationHandler.END


async def receive_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send broadcast message to all users."""
    msg = update.message.text

    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE is_blocked=0")
            users = c.fetchall()

        sent = 0
        failed = 0
        for row in users:
            try:
                await context.bot.send_message(
                    row['user_id'],
                    f"📢 <b>Announcement</b>\n\n{msg}",
                    parse_mode="HTML"
                )
                sent += 1
            except Exception:
                failed += 1

        log_audit("broadcast", ADMIN_ID, None, f"Sent: {sent}, Failed: {failed}")

        kb = [[InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
        await update.message.reply_text(
            f"📢 <b>Broadcast Complete</b>\n\nSent: {sent}\nFailed: {failed}\nTotal: {len(users)}",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("⚠️ Error occurred", parse_mode="HTML")
        return ConversationHandler.END


async def receive_wallet_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive amount for wallet add/deduct."""
    try:
        amount = Decimal(update.message.text.strip())

        if amount <= 0:
            await update.message.reply_text("❌ Amount must be > 0.\n\n/cancel to abort", parse_mode="HTML")
            return WALLET_AMOUNT

        if amount.as_tuple().exponent < -2:
            await update.message.reply_text("❌ Max 2 decimal places.\n\n/cancel to abort", parse_mode="HTML")
            return WALLET_AMOUNT

        action = context.user_data.get('wallet_action')
        current = context.user_data.get('wallet_current_balance', 0)

        if action == 'deduct' and amount > Decimal(str(current)):
            await update.message.reply_text(
                f"❌ Cannot deduct ₹{float(amount):.2f} (balance: ₹{current:.2f})\n\n/cancel to abort",
                parse_mode="HTML"
            )
            return WALLET_AMOUNT

        context.user_data['wallet_amount'] = float(amount)

        await update.message.reply_text(
            f"💰 <b>Amount: ₹{float(amount):.2f}</b>\n\nEnter reason (5+ chars):\n\n/cancel to abort",
            parse_mode="HTML"
        )
        return WALLET_REASON

    except (ValueError, InvalidOperation):
        await update.message.reply_text("❌ Invalid amount.\n\n/cancel to abort", parse_mode="HTML")
        return WALLET_AMOUNT


async def receive_wallet_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive reason for wallet add/deduct."""
    reason = update.message.text.strip()

    if len(reason) < 5:
        await update.message.reply_text("❌ Reason must be 5+ chars.\n\n/cancel to abort", parse_mode="HTML")
        return WALLET_REASON

    if len(reason) > 200:
        await update.message.reply_text("❌ Reason must be <200 chars.\n\n/cancel to abort", parse_mode="HTML")
        return WALLET_REASON

    context.user_data['wallet_reason'] = reason

    action = context.user_data.get('wallet_action')
    amount = context.user_data.get('wallet_amount')
    uid = context.user_data.get('wallet_target_user')
    name = context.user_data.get('wallet_target_name')

    text = (
        f"⚠️ <b>Confirm Balance Update</b>\n\n"
        f"User: {name} (ID: {uid})\n"
        f"Action: {action.upper()}\n"
        f"Amount: ₹{amount:.2f}\n"
        f"Reason: {reason}\n\n"
        f"⚠️ <i>This cannot be undone.</i>"
    )

    kb = [
        [InlineKeyboardButton("✅ Confirm", callback_data=f"wallet_confirm_{uid}"),
         InlineKeyboardButton("❌ Cancel", callback_data="wallet_cancel")],
    ]

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END


async def receive_new_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sets a new fixed Gmail rate."""
    try:
        new_price = Decimal(update.message.text.strip())

        if new_price <= 0:
            await update.message.reply_text(
                "❌ Price must be greater than 0.\n\n/cancel to abort", parse_mode="HTML")
            return ADMIN_SET_PRICE

        if new_price > 500:
            await update.message.reply_text(
                "❌ Price cannot exceed ₹500.\n\n/cancel to abort", parse_mode="HTML")
            return ADMIN_SET_PRICE

        if new_price.as_tuple().exponent < -2:
            await update.message.reply_text(
                "❌ Max 2 decimal places.\n\n/cancel to abort", parse_mode="HTML")
            return ADMIN_SET_PRICE

        old_rate = float(get_gmail_rate())

        if set_gmail_rate(new_price):
            log_audit("change_gmail_rate", ADMIN_ID, None,
                      f"₹{old_rate:.0f} → ₹{float(new_price):.0f}")

            kb = [[InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
                   InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
            await update.message.reply_text(
                f"✅ <b>Gmail Rate Updated!</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Old: ₹{old_rate:.0f}/account\n"
                f"New: <b>₹{float(new_price):.0f}/account</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"All new tasks will use the new rate.\n"
                f"Existing pending tasks keep their original rate.",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                "⚠️ Failed to update rate. Try again.", parse_mode="HTML")

        return ConversationHandler.END

    except (ValueError, InvalidOperation):
        await update.message.reply_text(
            "❌ Invalid number. Enter a valid price.\n\n/cancel to abort",
            parse_mode="HTML")
        return ADMIN_SET_PRICE
    except Exception as e:
        logger.error(f"Error in receive_new_price: {e}")
        await update.message.reply_text("⚠️ Error occurred.", parse_mode="HTML")
        return ConversationHandler.END

