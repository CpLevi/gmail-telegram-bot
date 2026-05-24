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
    ADMIN_ID,
    ADMIN_WITHDRAWALS_PER_PAGE, USER_SEARCH, BROADCAST_MSG,
    WALLET_AMOUNT, WALLET_REASON, ADMIN_SET_PRICE, ADMIN_SET_VIDEO,
    ADMIN_SET_MAX_WITHDRAW, WITHDRAW_REJECT_REASON,
    DISABLE_SMTP_CHECK, SMTP_PROXY,
)
from database import get_db
from utils import (
    validate_page, round_decimal, log_audit, notify_user,
    safe_edit_or_reply, mask_email, get_gmail_rate, set_gmail_rate,
    is_task_submission_enabled, set_task_submission,
    is_bulk_submission_enabled, set_bulk_submission,
    get_instruction_video_url, set_instruction_video_url,
    get_max_withdrawal_amount, set_max_withdrawal_amount,
)

logger = logging.getLogger(__name__)

# Verification badge helper
def _vbadge(status):
    """Return emoji badge for verification status."""
    if DISABLE_SMTP_CHECK and not SMTP_PROXY:
        return ""
    return {"verified": "✅", "suspicious": "⚠️", "error": "❓", "unchecked": "⏳"}.get(status or "unchecked", "⏳")


async def process_referral_payout(c, referred_id, count, context=None, gmail_ids=None):
    """Process ongoing referral commissions for approved Gmails."""
    from config import REFERRAL_RATE_MONTH_1, REFERRAL_RATE_MONTH_2_PLUS
    from utils import round_decimal
    
    c.execute("SELECT referrer_id, date FROM referrals WHERE referred_id=%s", (referred_id,))
    ref = c.fetchone()
    if not ref or not ref['referrer_id']:
        return
        
    referrer_id = ref['referrer_id']
    try:
        join_date = datetime.fromisoformat(ref['date'])
    except Exception:
        join_date = datetime.now()
        
    days_since_join = (datetime.now() - join_date).days
    rate = REFERRAL_RATE_MONTH_1 if days_since_join < 30 else REFERRAL_RATE_MONTH_2_PLUS
    
    # Filter out gmail_ids that already have a payout (prevent double-credit)
    gmail_ids = gmail_ids or [None] * count
    unpaid_ids = []
    for gid in gmail_ids:
        if gid is not None:
            c.execute("SELECT 1 FROM referral_payouts WHERE gmail_id=%s", (gid,))
            if c.fetchone():
                continue  # Already paid for this Gmail, skip
        unpaid_ids.append(gid)
    
    if not unpaid_ids:
        return  # All already paid
    
    actual_count = len(unpaid_ids)
    total_payout = round_decimal(rate * actual_count)
    
    if total_payout > 0:
        c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", (total_payout, referrer_id))
        
        for gid in unpaid_ids:
            c.execute("""
                INSERT INTO referral_payouts (referrer_id, referred_id, gmail_id, amount, date)
                VALUES (%s, %s, %s, %s, %s)
            """, (referrer_id, referred_id, gid, rate, datetime.now().isoformat()))
            
        c.execute("UPDATE referrals SET rewarded=1 WHERE referred_id=%s AND rewarded=0", (referred_id,))
        
        if context:
            try:
                c.execute("SELECT first_name FROM users WHERE user_id=%s", (referred_id,))
                rname_row = c.fetchone()
                rname = rname_row['first_name'] if rname_row else "A referral"
                await notify_user(context, referrer_id,
                    f"🎉 <b>Referral Commission!</b>\n\n"
                    f"<b>{rname}</b> had {actual_count} task(s) approved.\n"
                    f"₹{float(total_payout):.2f} credited to your balance!")
            except Exception:
                pass

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
            c.execute("""SELECT COUNT(*) FROM gmail g
                         JOIN users u ON g.user_id = u.user_id
                         WHERE g.status='pending'
                         AND (g.task_status = 'confirmed' OR g.task_id IS NULL)""")
            pg = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='in_review'")
            ir = list(c.fetchone().values())[0]
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
            pw = list(c.fetchone().values())[0]

        text = (
            f"🔐 <b>Admin Panel</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Users: <b>{users}</b>\n"
            f"📬 Pending Gmail: <b>{pg}</b>\n"
            f"🔍 In Review: <b>{ir}</b>\n"
            f"💸 Pending Withdrawals: <b>{pw}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )

        kb = [
            [InlineKeyboardButton(f"📬 Gmail Queue ({pg})", callback_data="gmail_queue")],
            [InlineKeyboardButton(f"🔍 In Review ({ir})", callback_data="in_review_queue")],
            [InlineKeyboardButton("💸 Withdrawals", callback_data="withdrawal_queue")],
            [InlineKeyboardButton("👥 User Management", callback_data="user_mgmt")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── GMAIL QUEUE — PENDING ONLY (10 users per page) ──
    elif d == "gmail_queue" or d.startswith("gmail_queue_"):
        page = validate_page(d.split("_")[-1]) if "_" in d else 0
        per_page = 10
        offset = page * per_page

        with get_db() as conn:
            c = conn.cursor()

            # Count ONLY pending (not in_review) — use JOIN to match user listing
            c.execute("""SELECT COUNT(*) FROM gmail g
                         JOIN users u ON g.user_id = u.user_id
                         WHERE g.status = 'pending'
                         AND (g.task_status = 'confirmed' OR g.task_id IS NULL)""")
            total_all = list(c.fetchone().values())[0]

            # Users with pending submissions
            c.execute("""SELECT DISTINCT u.user_id, u.first_name, u.username, COUNT(g.id) as cnt
                         FROM gmail g JOIN users u ON g.user_id = u.user_id
                         WHERE g.status = 'pending'
                         AND (g.task_status = 'confirmed' OR g.task_id IS NULL)
                         GROUP BY u.user_id, u.first_name, u.username
                         ORDER BY cnt DESC LIMIT %s OFFSET %s""", (per_page, offset))
            users_pending = c.fetchall()

            c.execute("""SELECT COUNT(DISTINCT user_id) FROM gmail
                         WHERE status = 'pending'
                         AND (task_status = 'confirmed' OR task_id IS NULL)""")
            total_users = list(c.fetchone().values())[0]

        if total_all == 0:
            await safe_edit_or_reply(
                q, "✅ <b>No new pending submissions</b>\n\nAll caught up! 🎉",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]])
            )
            return

        total_pages = max(1, (total_users + per_page - 1) // per_page)
        text = (
            f"📬 <b>Gmail Queue — New Pending</b>\n\n"
            f"📬 Total: <b>{total_all}</b> | 👥 Users: <b>{total_users}</b>\n"
            f"📄 Page {page + 1} of {total_pages}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        kb = []
        for i, row in enumerate(users_pending, 1):
            uid, name, username, cnt = row['user_id'], row['first_name'], row['username'], row['cnt']
            text += f"{i}. {name} (@{username or 'N/A'}) — <b>{cnt}</b> new\n"
            kb.append([InlineKeyboardButton(f"📧 {name} ({cnt})", callback_data=f"review_user_{uid}_0")])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"gmail_queue_{page - 1}"))
        if offset + per_page < total_users:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"gmail_queue_{page + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── PENDING USER VIEW (10 gmails per page) ──
    elif d.startswith("review_user_"):
        parts = d.split("_")
        uid = int(parts[2])
        page = validate_page(parts[3]) if len(parts) > 3 else 0
        per_page = 10
        offset = page * per_page

        with get_db() as conn:
            c = conn.cursor()

            # Only PENDING for this user
            c.execute("""SELECT COUNT(*) FROM gmail WHERE user_id=%s
                         AND status = 'pending'
                         AND (task_status = 'confirmed' OR task_id IS NULL)""", (uid,))
            total_count = list(c.fetchone().values())[0]

            if total_count == 0:
                await q.answer("✅ No pending for this user!", show_alert=True)
                q.data = "gmail_queue_0"
                await admin_callback(update, context)
                return

            # Get 10 gmails for this page
            c.execute("""SELECT id, email, password, reward, submit_date, totp_secret, verification_status
                        FROM gmail WHERE user_id=%s
                        AND status = 'pending'
                        AND (task_status = 'confirmed' OR task_id IS NULL)
                        ORDER BY submit_date ASC
                        LIMIT %s OFFSET %s""", (uid, per_page, offset))
            gmails = c.fetchall()

            c.execute("SELECT first_name, username FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()

        if not gmails or not user_info:
            await q.answer("Not found", show_alert=True)
            return

        name = user_info['first_name']
        username = user_info['username'] or 'N/A'
        total_pages = max(1, (total_count + per_page - 1) // per_page)

        text = (
            f"📬 <b>Pending — {name}</b>\n\n"
            f"User: @{username} (ID: {uid})\n"
            f"Pending: {total_count} | Page {page + 1}/{total_pages}\n\n"
        )

        # Compact list with copy-paste format + verification badge
        for i, gmail in enumerate(gmails, 1):
            gid = gmail['id']
            secret = gmail.get('totp_secret') or 'N/A'
            badge = _vbadge(gmail.get('verification_status'))
            text += (
                f"{i}. <b>#{gid}</b> {badge}\n"
                f"<code>{gmail['email']}|{gmail['password']}|{secret}</code>\n"
                f"₹{float(gmail['reward']):.1f}\n\n"
            )

        kb = []

        # Export button
        kb.append([
            InlineKeyboardButton(f"📥 Export All Pending ({total_count})", callback_data=f"export_pending_{uid}"),
        ])

        # Send to Review buttons
        kb.append([
            InlineKeyboardButton(f"📤 Send All to Review ({total_count})", callback_data=f"send_review_all_{uid}"),
        ])

        # Navigation
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"review_user_{uid}_{page - 1}"))
        if offset + per_page < total_count:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"review_user_{uid}_{page + 1}"))
        if nav:
            kb.append(nav)

        # Direct actions (can still approve/reject directly from here)
        if total_count > 1:
            kb.append([
                InlineKeyboardButton(f"✅ Approve All ({total_count})", callback_data=f"approve_all_{uid}"),
                InlineKeyboardButton(f"❌ Reject All ({total_count})", callback_data=f"reject_all_{uid}"),
            ])

        kb.append([InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue_0")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── SEND TO REVIEW (all pending for a user → in_review) ──
    elif d.startswith("send_review_all_"):
        uid = int(d.split("_")[3])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""UPDATE gmail SET status='in_review'
                             WHERE user_id=%s AND status='pending'
                             AND (task_status = 'confirmed' OR task_id IS NULL)
                             RETURNING id""", (uid,))
                moved = c.fetchall()
                conn.commit()

            count = len(moved)
            if count == 0:
                await q.answer("No pending to send", show_alert=True)
                return

            log_audit("send_to_review", ADMIN_ID, uid, f"{count} gmails")
            await q.answer(f"📤 {count} sent to review!", show_alert=True)
            q.data = "gmail_queue_0"
            await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error send_review_all {uid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── EXPORT PENDING (download as .txt) ──
    elif d.startswith("export_pending_"):
        uid = int(d.split("_")[2])
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT email, password, totp_secret FROM gmail
                         WHERE user_id=%s AND status='pending'
                         AND (task_status = 'confirmed' OR task_id IS NULL)
                         ORDER BY submit_date ASC""", (uid,))
            rows = c.fetchall()
            c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()

        if not rows:
            await q.answer("No data to export", show_alert=True)
            return

        name = user_info['first_name'] if user_info else 'unknown'
        lines = []
        for row in rows:
            secret = row.get('totp_secret') or 'N/A'
            lines.append(f"{row['email']}|{row['password']}|{secret}")

        content = "\n".join(lines)
        import io
        file = io.BytesIO(content.encode('utf-8'))
        file.name = f"pending_{name}_{uid}_{len(rows)}.txt"

        await context.bot.send_document(
            chat_id=q.message.chat_id,
            document=file,
            caption=f"📥 <b>Pending Export — {name}</b>\n{len(rows)} accounts\nFormat: Email|Password|2FA_Secret",
            parse_mode="HTML"
        )
        await q.answer(f"📥 {len(rows)} exported!", show_alert=True)

    # ── EXPORT IN REVIEW (download as .txt) ──
    elif d.startswith("export_inreview_"):
        uid = int(d.split("_")[2])
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT email, password, totp_secret FROM gmail
                         WHERE user_id=%s AND status='in_review'
                         ORDER BY submit_date ASC""", (uid,))
            rows = c.fetchall()
            c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()

        if not rows:
            await q.answer("No data to export", show_alert=True)
            return

        name = user_info['first_name'] if user_info else 'unknown'
        lines = []
        for row in rows:
            secret = row.get('totp_secret') or 'N/A'
            lines.append(f"{row['email']}|{row['password']}|{secret}")

        content = "\n".join(lines)
        import io
        file = io.BytesIO(content.encode('utf-8'))
        file.name = f"in_review_{name}_{uid}_{len(rows)}.txt"

        await context.bot.send_document(
            chat_id=q.message.chat_id,
            document=file,
            caption=f"📥 <b>In Review Export — {name}</b>\n{len(rows)} accounts\nFormat: Email|Password|2FA_Secret",
            parse_mode="HTML"
        )
        await q.answer(f"📥 {len(rows)} exported!", show_alert=True)

    # ── IN REVIEW QUEUE (10 users per page) ──
    elif d == "in_review_queue" or d.startswith("in_review_queue_"):
        page = validate_page(d.split("_")[-1]) if d.startswith("in_review_queue_") else 0
        per_page = 10
        offset = page * per_page

        with get_db() as conn:
            c = conn.cursor()

            c.execute("SELECT COUNT(*) FROM gmail WHERE status='in_review'")
            total_all = list(c.fetchone().values())[0]

            c.execute("""SELECT DISTINCT u.user_id, u.first_name, u.username, COUNT(g.id) as cnt
                         FROM gmail g JOIN users u ON g.user_id = u.user_id
                         WHERE g.status = 'in_review'
                         GROUP BY u.user_id, u.first_name, u.username
                         ORDER BY cnt DESC LIMIT %s OFFSET %s""", (per_page, offset))
            users_review = c.fetchall()

            c.execute("SELECT COUNT(DISTINCT user_id) FROM gmail WHERE status='in_review'")
            total_users = list(c.fetchone().values())[0]

        if total_all == 0:
            await safe_edit_or_reply(
                q, "✅ <b>No gmails in review</b>\n\nNothing to verify! 🎉",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin")]])
            )
            return

        total_pages = max(1, (total_users + per_page - 1) // per_page)
        text = (
            f"🔍 <b>In Review</b>\n\n"
            f"🔍 Total: <b>{total_all}</b> | 👥 Users: <b>{total_users}</b>\n"
            f"📄 Page {page + 1} of {total_pages}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        kb = []
        for i, row in enumerate(users_review, 1):
            uid, name, username, cnt = row['user_id'], row['first_name'], row['username'], row['cnt']
            text += f"{i}. {name} (@{username or 'N/A'}) — <b>{cnt}</b> in review\n"
            kb.append([InlineKeyboardButton(f"🔍 {name} ({cnt})", callback_data=f"review_detail_{uid}_0")])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"in_review_queue_{page - 1}"))
        if offset + per_page < total_users:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"in_review_queue_{page + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── IN REVIEW DETAIL — per user (10 gmails per page with approve/reject) ──
    elif d.startswith("review_detail_"):
        parts = d.split("_")
        uid = int(parts[2])
        page = validate_page(parts[3]) if len(parts) > 3 else 0
        per_page = 10
        offset = page * per_page

        with get_db() as conn:
            c = conn.cursor()

            c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='in_review'", (uid,))
            total_count = list(c.fetchone().values())[0]

            if total_count == 0:
                await q.answer("✅ All processed!", show_alert=True)
                q.data = "in_review_queue"
                await admin_callback(update, context)
                return

            c.execute("""SELECT id, email, password, reward, submit_date, totp_secret, verification_status
                        FROM gmail WHERE user_id=%s AND status='in_review'
                        ORDER BY submit_date ASC
                        LIMIT %s OFFSET %s""", (uid, per_page, offset))
            gmails = c.fetchall()

            c.execute("SELECT first_name, username FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()

        if not gmails or not user_info:
            await q.answer("Not found", show_alert=True)
            return

        name = user_info['first_name']
        username = user_info['username'] or 'N/A'
        total_pages = max(1, (total_count + per_page - 1) // per_page)

        text = (
            f"🔍 <b>In Review — {name}</b>\n\n"
            f"User: @{username} (ID: {uid})\n"
            f"In Review: {total_count} | Page {page + 1}/{total_pages}\n\n"
        )

        kb = []
        for i, gmail in enumerate(gmails, 1):
            gid = gmail['id']
            secret = gmail.get('totp_secret') or 'N/A'
            badge = _vbadge(gmail.get('verification_status'))
            text += (
                f"{i}. <b>#{gid}</b> {badge}\n"
                f"<code>{gmail['email']}|{gmail['password']}|{secret}</code>\n"
                f"₹{float(gmail['reward']):.1f}\n\n"
            )
            row = [InlineKeyboardButton(f"✅ #{gid}", callback_data=f"approve_{gid}_{uid}_{page}_ir")]
            if not DISABLE_SMTP_CHECK or SMTP_PROXY:
                row.append(InlineKeyboardButton(f"🔍 Verify", callback_data=f"verify_smtp_{gid}_{uid}_{page}"))
            row.append(InlineKeyboardButton(f"❌ #{gid}", callback_data=f"reject_{gid}_{uid}_{page}_ir"))
            kb.append(row)

        # Export button
        kb.append([
            InlineKeyboardButton(f"📥 Export All In Review ({total_count})", callback_data=f"export_inreview_{uid}"),
        ])

        # Navigation
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"review_detail_{uid}_{page - 1}"))
        if offset + per_page < total_count:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"review_detail_{uid}_{page + 1}"))
        if nav:
            kb.append(nav)

        # Batch actions
        if total_count > 1:
            kb.append([
                InlineKeyboardButton(f"✅ Approve All ({total_count})", callback_data=f"irapprove_all_{uid}"),
                InlineKeyboardButton(f"❌ Reject All ({total_count})", callback_data=f"irreject_all_{uid}"),
            ])

        kb.append([InlineKeyboardButton("🔙 Back to In Review", callback_data="in_review_queue")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── ON-DEMAND VERIFICATION (SMTP) ──
    elif d.startswith("verify_smtp_"):
        parts = d.split("_")
        gid = int(parts[2])
        uid = int(parts[3])
        page = int(parts[4])

        if DISABLE_SMTP_CHECK and not SMTP_PROXY:
            await q.answer("⚠️ SMTP verification is disabled. Set SMTP_PROXY to enable.", show_alert=True)
            return

        await q.answer("🔍 Checking Google servers...", show_alert=False)

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT email FROM gmail WHERE id=%s", (gid,))
                row = c.fetchone()

            if not row:
                await q.answer("❌ Gmail not found", show_alert=True)
                return

            email = row['email']
            from verifier import check_gmail_exists

            status, detail = await check_gmail_exists(email)

            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail
                    SET verification_status = %s, verification_checked_at = %s
                    WHERE id = %s
                """, (status, datetime.now().isoformat(), gid))

            # Display popup alert of the result
            status_emoji = _vbadge(status)
            await q.answer(f"{status_emoji} Result: {status.upper()}\n{detail}", show_alert=True)

            # Refresh page to show updated badge
            q.data = f"review_detail_{uid}_{page}"
            await admin_callback(update, context)
            return

        except Exception as e:
            logger.error(f"Error in manual SMTP check for #{gid}: {e}")
            await q.answer("⚠️ Error occurred during SMTP check", show_alert=True)
            return

    # ── APPROVE SINGLE (from Pending or In Review) ──
    elif d.startswith("approve_") and not d.startswith("approve_all_"):
        parts = d.split("_")
        gid = int(parts[1])
        uid = int(parts[2]) if len(parts) > 2 else None
        page = validate_page(parts[3]) if len(parts) > 3 else 0
        from_ir = len(parts) > 4 and parts[4] == "ir"  # came from In Review

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

                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='approved'", (uid,))
                approval_count = list(c.fetchone().values())[0]
                is_first = (approval_count == 1)

                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+1 WHERE user_id=%s",
                          (reward, uid))

                await process_referral_payout(c, uid, 1, context, [gid])

                conn.commit()
                log_audit("approve_gmail", ADMIN_ID, uid, f"#{gid} — ₹{float(reward):.2f}")

                await notify_user(context, uid,
                    f"✅ <b>Gmail Verified!</b>\n\n"
                    f"📧 {email}\n"
                    f"💰 ₹{float(reward):.2f} credited!")

                await q.answer(f"✅ Approved — ₹{float(reward):.2f}", show_alert=True)
                # Navigate back to correct section
                if from_ir:
                    q.data = f'review_detail_{uid}_{page}'
                else:
                    q.data = f'review_user_{uid}_{page}'
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error approving {gid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── REJECT SINGLE (from Pending or In Review) ──
    elif d.startswith("reject_") and not d.startswith("reject_all_"):
        parts = d.split("_")
        gid = int(parts[1])
        uid = int(parts[2]) if len(parts) > 2 else None
        page = validate_page(parts[3]) if len(parts) > 3 else 0
        from_ir = len(parts) > 4 and parts[4] == "ir"

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
                if from_ir:
                    q.data = f'review_detail_{uid}_{page}'
                else:
                    q.data = f'review_user_{uid}_{page}'
                await admin_callback(update, context)
        except Exception as e:
            logger.error(f"Error rejecting {gid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── APPROVE ALL (pending only) ──
    elif d.startswith("approve_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='approved', review_date=%s
                    WHERE user_id=%s AND status='pending'
                    RETURNING id, reward, email
                """, (datetime.now().isoformat(), uid))
                gmails = c.fetchall()

                if not gmails:
                    await q.answer("No pending or already processed", show_alert=True)
                    return

                total_reward = sum(round_decimal(g['reward']) for g in gmails)
                count = len(gmails)

                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+%s WHERE user_id=%s",
                          (total_reward, count, uid))

                gmail_ids = [g['id'] for g in gmails]
                await process_referral_payout(c, uid, count, context, gmail_ids)

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

    # ── REJECT ALL (pending only) ──
    elif d.startswith("reject_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='rejected', review_date=%s, rejection_reason=%s
                    WHERE user_id=%s AND status='pending'
                    RETURNING id
                """, (datetime.now().isoformat(), "Quality issues", uid))
                rejected_ids = c.fetchall()
                count = len(rejected_ids)

                if count == 0:
                    await q.answer("No pending or already processed", show_alert=True)
                    return
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

    # ── IN-REVIEW APPROVE ALL ──
    elif d.startswith("irapprove_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='approved', review_date=%s
                    WHERE user_id=%s AND status='in_review'
                    RETURNING id, reward, email
                """, (datetime.now().isoformat(), uid))
                gmails = c.fetchall()

                if not gmails:
                    await q.answer("No in-review or already processed", show_alert=True)
                    return

                total_reward = sum(round_decimal(g['reward']) for g in gmails)
                count = len(gmails)

                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+%s WHERE user_id=%s",
                          (total_reward, count, uid))

                gmail_ids = [g['id'] for g in gmails]
                await process_referral_payout(c, uid, count, context, gmail_ids)

                conn.commit()
                log_audit("ir_approve_all", ADMIN_ID, uid, f"{count} — ₹{float(total_reward):.2f}")

                email_list = "\n".join([f"• {mask_email(g['email'])}" for g in gmails[:5]])
                if len(gmails) > 5:
                    email_list += f"\n• ...and {len(gmails) - 5} more"

                await notify_user(context, uid,
                    f"✅ <b>All Gmail Verified!</b>\n\n"
                    f"Total: {count} accounts\n"
                    f"₹{float(total_reward):.2f} credited!\n\n{email_list}")

                await q.answer(f"✅ {count} approved — ₹{float(total_reward):.2f}", show_alert=True)

                await safe_edit_or_reply(q,
                    f"✅ <b>Batch Approved (Review)</b>\n\nUser: {uid}\nCount: {count}\nTotal: ₹{float(total_reward):.2f}",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to In Review", callback_data="in_review_queue")]]))

        except Exception as e:
            logger.error(f"Error ir approve all {uid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── IN-REVIEW REJECT ALL ──
    elif d.startswith("irreject_all_"):
        uid = int(d.split("_")[2])
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE gmail SET status='rejected', review_date=%s, rejection_reason=%s
                    WHERE user_id=%s AND status='in_review'
                    RETURNING id
                """, (datetime.now().isoformat(), "Quality issues", uid))
                rejected_ids = c.fetchall()
                count = len(rejected_ids)

                if count == 0:
                    await q.answer("No in-review or already processed", show_alert=True)
                    return
                conn.commit()

                log_audit("ir_reject_all", ADMIN_ID, uid, f"{count} rejected")

                await notify_user(context, uid,
                    f"❌ <b>Gmail Rejected</b>\n\n"
                    f"Total: {count} accounts\n"
                    f"Reason: Quality issues\n\n"
                    f"<i>Ensure accounts match assigned task details exactly.</i>")

                await q.answer(f"❌ {count} rejected", show_alert=True)

                await safe_edit_or_reply(q,
                    f"❌ <b>Batch Rejected (Review)</b>\n\nUser: {uid}\nCount: {count}",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to In Review", callback_data="in_review_queue")]]))

        except Exception as e:
            logger.error(f"Error ir reject all {uid}: {e}")
            await q.answer("Error", show_alert=True)

    # ── WITHDRAWAL QUEUE ──
    elif d == "withdrawal_queue" or d.startswith("withdrawal_queue_"):
        per_page = ADMIN_WITHDRAWALS_PER_PAGE
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

            max_page = (total_pending - 1) // per_page
            if page < 0:
                page = 0
            elif page > max_page:
                page = max_page

            offset = page * per_page

            c.execute("""SELECT w.id, w.amount, w.fee, w.final_amount, w.method, w.payment_info, w.request_date,
                         u.first_name, u.username, u.user_id
                         FROM withdrawals w JOIN users u ON w.user_id = u.user_id
                         WHERE w.status='pending' ORDER BY w.request_date LIMIT %s OFFSET %s""", (per_page, offset))
            withdrawals = c.fetchall()

        if withdrawals:
            total_pages = max(1, (total_pending + per_page - 1) // per_page)
            text = (
                f"💸 <b>Pending Withdrawals</b>\n"
                f"<i>Total Pending: {total_pending} | Page {page + 1} of {total_pages}</i>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
            )

            kb = []
            for i, w in enumerate(withdrawals, 1):
                text += (
                    f"{i}. <b>💸 Withdrawal #{w['id']}</b>\n"
                    f"👤 User: {w['first_name']} (@{w['username'] or 'N/A'}) [<code>{w['user_id']}</code>]\n"
                    f"💰 Final: <b>₹{float(w['final_amount']):.2f}</b> (Amt: ₹{float(w['amount']):.2f} | Fee: ₹{float(w['fee']):.2f})\n"
                    f"💳 {w['method'].upper()}: <code>{w['payment_info']}</code>\n"
                    f"📅 {w['request_date'][:16]}\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                )
                kb.append([
                    InlineKeyboardButton(f"✅ #{w['id']}", callback_data=f"withdraw_approve_{w['id']}_{page}"),
                    InlineKeyboardButton(f"❌ #{w['id']}", callback_data=f"withdraw_reject_{w['id']}_{page}")
                ])

            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"withdrawal_queue_{page - 1}"))
            if offset + per_page < total_pending:
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

        # Save variables to context.user_data
        context.user_data['reject_wid'] = wid
        context.user_data['reject_page'] = page
        context.user_data['reject_reason_code'] = reason_code
        context.user_data['reject_default_reason'] = rejection_reason

        text = (
            f"📝 <b>Add Rejection Comment</b> (Optional)\n\n"
            f"Withdrawal: <b>#{wid}</b>\n"
            f"Reason category: <b>{rejection_reason}</b>\n\n"
            f"Please send a custom comment/detail to explain this rejection to the user (e.g., <i>\"Your UPI ID is inactive\"</i>), or send <code>/skip</code> to use the default reason.\n\n"
            f"Send <code>/cancel</code> to abort the rejection."
        )

        await safe_edit_or_reply(q, text)
        return WITHDRAW_REJECT_REASON

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
        tasks_enabled = is_task_submission_enabled()
        bulk_enabled = is_bulk_submission_enabled()
        video_url = get_instruction_video_url()
        max_withdraw = float(get_max_withdrawal_amount())
        status_icon = "✅" if tasks_enabled else "🔴"
        status_text = "Active" if tasks_enabled else "Paused"
        toggle_label = "🔴 Disable Task Submission" if tasks_enabled else "🟢 Enable Task Submission"
        bulk_icon = "✅" if bulk_enabled else "🔴"
        bulk_text = "Active" if bulk_enabled else "Disabled"
        bulk_toggle_label = "🔴 Disable Bulk Tasks" if bulk_enabled else "🟢 Enable Bulk Tasks"
        video_status = "✅ Set" if video_url else "❌ Not set"

        text = (
            f"⚙️ <b>Bot Settings</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Gmail Rate:</b> ₹{current_rate:.0f}/account\n"
            f"💸 <b>Max Withdrawal:</b> ₹{max_withdraw:.0f}/request\n"
            f"📋 <b>Task Submission:</b> {status_icon} {status_text}\n"
            f"📦 <b>Bulk Submission:</b> {bulk_icon} {bulk_text}\n"
            f"🎥 <b>Video Instruction:</b> {video_status}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Rate applies to <b>all users</b>.\n"
            f"Toggle tasks to pause/resume submissions."
        )
        kb = [
            [InlineKeyboardButton(f"💰 Change Price (₹{current_rate:.0f})", callback_data="set_price")],
            [InlineKeyboardButton(f"💸 Max Withdrawal (₹{max_withdraw:.0f})", callback_data="set_max_withdraw")],
            [InlineKeyboardButton(toggle_label, callback_data="toggle_tasks")],
            [InlineKeyboardButton(bulk_toggle_label, callback_data="toggle_bulk")],
            [InlineKeyboardButton("🎥 Set Video Instruction", callback_data="set_video")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── TOGGLE TASK SUBMISSION ──
    elif d == "toggle_tasks":
        current = is_task_submission_enabled()
        new_state = not current

        if set_task_submission(new_state):
            state_word = "enabled" if new_state else "disabled"
            log_audit("toggle_task_submission", ADMIN_ID, None, f"Task submission {state_word}")

            alert_msg = "✅ Task submissions enabled" if new_state else "🔴 Task submissions disabled"
            await q.answer(alert_msg, show_alert=True)
            # Refresh settings page
            q.data = "admin_settings"
            await admin_callback(update, context)
        else:
            await q.answer("❌ Failed to update. Try again.", show_alert=True)

    # ── TOGGLE BULK SUBMISSION ──
    elif d == "toggle_bulk":
        current = is_bulk_submission_enabled()
        new_state = not current

        if set_bulk_submission(new_state):
            state_word = "enabled" if new_state else "disabled"
            log_audit("toggle_bulk_submission", ADMIN_ID, None, f"Bulk submission {state_word}")

            alert_msg = "✅ Bulk submissions enabled" if new_state else "🔴 Bulk submissions disabled"
            await q.answer(alert_msg, show_alert=True)
            # Refresh settings page
            q.data = "admin_settings"
            await admin_callback(update, context)
        else:
            await q.answer("❌ Failed to update. Try again.", show_alert=True)

    # ── SET VIDEO INSTRUCTION (entry point) ──
    elif d == "set_video":
        current_url = get_instruction_video_url()
        current_display = f"Current: {current_url[:50]}..." if current_url and len(current_url) > 50 else (f"Current: {current_url}" if current_url else "No video set")
        await safe_edit_or_reply(
            q,
            f"🎥 <b>Set Video Instruction</b>\n\n"
            f"{current_display}\n\n"
            f"Send the video URL or forward a video message.\n"
            f"Supported: Telegram file_id, direct URL, or YouTube link.\n\n"
            f"/cancel to abort",
        )
        return ADMIN_SET_VIDEO

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

    # ── SET MAX WITHDRAWAL (entry point) ──
    elif d == "set_max_withdraw":
        current_max = float(get_max_withdrawal_amount())
        await safe_edit_or_reply(
            q,
            f"💸 <b>Set Max Withdrawal</b>\n\n"
            f"Current limit: <b>₹{current_max:.0f}</b>/request\n\n"
            f"Send the new max amount (in ₹):\n\n"
            f"<i>Examples: 200, 500, 1000</i>\n\n"
            f"/cancel to abort",
        )
        return ADMIN_SET_MAX_WITHDRAW

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
            joined_display = result['joined_date'][:10] if result['joined_date'] else "N/A"
            text = (
                f"👤 <b>User Info</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 ID: <code>{uid}</code>\n"
                f"👤 Name: {result['first_name']}\n"
                f"📱 Username: @{result['username'] or 'N/A'}\n"
                f"📊 Status: {status}\n\n"
                f"💰 Balance: ₹{float(result['balance']):.2f}\n"
                f"📧 Gmail: {result['approved_gmail']}/{result['total_gmail']}\n"
                f"📅 Joined: {joined_display}\n"
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
            # Check for orphaned gmail records (user submitted gmail but not in users table)
            with get_db() as conn2:
                c2 = conn2.cursor()
                c2.execute("""SELECT status, COUNT(*) as cnt FROM gmail
                              WHERE user_id=%s GROUP BY status""", (uid,))
                orphaned = c2.fetchall()

            if orphaned:
                status_summary = ", ".join([f"{r['status']}: {r['cnt']}" for r in orphaned])
                total_orphaned = sum(r['cnt'] for r in orphaned)
                text = (
                    f"⚠️ <b>Orphaned Records Found</b>\n\n"
                    f"🆔 ID: <code>{uid}</code>\n"
                    f"User NOT registered in the bot, but has <b>{total_orphaned}</b> gmail submissions.\n\n"
                    f"📊 Breakdown: {status_summary}\n\n"
                    f"<i>These submissions exist but the user never started the bot or their record is missing.</i>"
                )
                kb = [[InlineKeyboardButton("🔙 Back", callback_data="admin")]]
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


async def receive_video_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sends a video or URL for task instruction."""
    # Check if admin sent a video file
    if update.message.video:
        file_id = update.message.video.file_id
        if set_instruction_video_url(file_id):
            log_audit("set_instruction_video", ADMIN_ID, None, f"Video file_id: {file_id[:20]}...")
            kb = [[InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
                   InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
            await update.message.reply_text(
                "✅ <b>Video Instruction Updated!</b>\n\n"
                "Users will now see this video when they tap \"📹 Video instruction\" on tasks.",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
            )
            return ConversationHandler.END
        else:
            await update.message.reply_text("⚠️ Failed to save video. Try again.", parse_mode="HTML")
            return ADMIN_SET_VIDEO

    # Check if admin sent text (URL)
    text = update.message.text.strip() if update.message.text else ""

    if not text:
        await update.message.reply_text(
            "❌ Please send a video file or a URL.\n\n/cancel to abort",
            parse_mode="HTML"
        )
        return ADMIN_SET_VIDEO

    if set_instruction_video_url(text):
        log_audit("set_instruction_video", ADMIN_ID, None, f"URL: {text[:50]}")
        kb = [[InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
               InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
        await update.message.reply_text(
            f"✅ <b>Video Instruction Updated!</b>\n\n"
            f"URL: {text[:60]}{'...' if len(text) > 60 else ''}\n\n"
            f"Users will now see this when they tap \"📹 Video instruction\".",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
        )
        return ConversationHandler.END
    else:
        await update.message.reply_text("⚠️ Failed to save. Try again.\n\n/cancel to abort", parse_mode="HTML")
        return ADMIN_SET_VIDEO


async def receive_max_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sets a new max withdrawal limit."""
    try:
        new_limit = Decimal(update.message.text.strip())

        if new_limit < 100:
            await update.message.reply_text(
                "❌ Minimum limit is ₹100.\n\n/cancel to abort", parse_mode="HTML")
            return ADMIN_SET_MAX_WITHDRAW

        if new_limit > 50000:
            await update.message.reply_text(
                "❌ Maximum limit is ₹50,000.\n\n/cancel to abort", parse_mode="HTML")
            return ADMIN_SET_MAX_WITHDRAW

        old_limit = float(get_max_withdrawal_amount())

        if set_max_withdrawal_amount(new_limit):
            log_audit("change_max_withdrawal", ADMIN_ID, None,
                      f"₹{old_limit:.0f} → ₹{float(new_limit):.0f}")

            kb = [[InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"),
                   InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
            await update.message.reply_text(
                f"✅ <b>Max Withdrawal Updated!</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Old: ₹{old_limit:.0f}/request\n"
                f"New: <b>₹{float(new_limit):.0f}/request</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━",
                reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                "⚠️ Failed to update. Try again.", parse_mode="HTML")

        return ConversationHandler.END

    except (ValueError, InvalidOperation):
        await update.message.reply_text(
            "❌ Invalid number. Enter a valid amount.\n\n/cancel to abort",
            parse_mode="HTML")
        return ADMIN_SET_MAX_WITHDRAW
    except Exception as e:
        logger.error(f"Error in receive_max_withdraw: {e}")
        await update.message.reply_text("⚠️ Error occurred.", parse_mode="HTML")
        return ConversationHandler.END


async def receive_withdraw_reject_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin enters a custom comment/reason for rejection (or /skip to use default)."""
    text = update.message.text.strip()
    
    # Retrieve details from context.user_data
    wid = context.user_data.get('reject_wid')
    page = context.user_data.get('reject_page')
    reason_code = context.user_data.get('reject_reason_code')
    default_reason = context.user_data.get('reject_default_reason')

    if not all([wid, page is not None, reason_code, default_reason]):
        await update.message.reply_text("⚠️ Rejection session expired. Please start over from the withdrawal queue.")
        context.user_data.clear()
        return ConversationHandler.END

    # If admin sent /skip, use the default reason, otherwise use the typed text
    if text.lower() == '/skip':
        rejection_reason = default_reason
    else:
        rejection_reason = f"{default_reason} - {text}"

    # Clear html tags from custom reason to avoid parse mode crashes
    import html
    rejection_reason = html.escape(rejection_reason)

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
                await update.message.reply_text("⚠️ This withdrawal has already been processed or is not pending.")
                context.user_data.clear()
                return ConversationHandler.END

            uid, amount = result['user_id'], round_decimal(result['amount'])
            c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", (amount, uid))
            conn.commit()

            log_audit("reject_withdrawal", ADMIN_ID, uid, f"#{wid} — ₹{float(amount):.2f} refunded — {rejection_reason}")

            # Notify the user with the custom reason / comments!
            await notify_user(context, uid,
                f"❌ <b>Withdrawal Rejected</b>\n\n"
                f"ID: #{wid}\n"
                f"Amount: ₹{float(amount):.2f}\n"
                f"<b>Reason:</b> {rejection_reason}\n\n"
                f"₹{float(amount):.2f} refunded to your balance.")

            await update.message.reply_text(
                f"❌ <b>Withdrawal #{wid} Rejected & Refunded</b>\n\n"
                f"User ID: {uid}\n"
                f"Refunded: ₹{float(amount):.2f}\n"
                f"Reason: {rejection_reason}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Withdrawals Queue", callback_data=f"withdrawal_queue_{page}")]])
            )
            context.user_data.clear()
            return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error executing custom reject withdrawal: {e}")
        await update.message.reply_text("⚠️ Error occurred during rejection.")
        context.user_data.clear()
        return ConversationHandler.END
