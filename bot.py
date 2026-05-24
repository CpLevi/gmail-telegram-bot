"""
EarnX Gmail Bot — Main Entry Point
Supports both polling (local dev) and webhook (Railway production).
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, ContextTypes, filters,
)

from config import (
    BOT_TOKEN, ADMIN_ID, WEBHOOK_URL, RAILWAY_PUBLIC_DOMAIN, PORT,
    USDT_ADDRESS, UPI_ID, WITHDRAW_AMT, WITHDRAW_CONFIRM,
    BROADCAST_MSG, USER_SEARCH,
    WALLET_AMOUNT, WALLET_REASON, ADMIN_SET_PRICE, ADMIN_SET_MAX_WITHDRAW,
    TASK_CONFIRM, BULK_TASK_QTY, BULK_TASK_CONFIRM,
    SINGLE_TASK_EXPIRY_MINUTES, BULK_TASK_EXPIRY_MINUTES,
    TOTP_SECRET, TOTP_BULK_SECRET, ADMIN_SET_VIDEO, WITHDRAW_REJECT_REASON,
)


# Handlers
from handlers.user import (
    start, user_callback,
    get_main_reply_keyboard, get_task_reply_keyboard,
    get_profile_keyboard, get_payment_keyboard,
    get_settings_keyboard, get_referral_keyboard, get_withdraw_keyboard,
    get_active_task_keyboard,
    build_balance_content, build_profile_content,
    build_help_content, build_referral_content, build_leaderboard_content,
    build_settings_content,
)
from handlers.submission import (
    handle_get_task, handle_task_done, handle_task_skip,
    handle_bulk_task, handle_bulk_qty,
    handle_bulk_done, handle_bulk_cancel,
    handle_get_task_text, handle_bulk_task_text,
    handle_task_done_text, handle_task_skip_text,
    handle_cancel_task_text,
    # 2FA handlers
    receive_totp_secret, handle_totp_refresh, handle_totp_done,
    handle_totp_refresh_text, handle_totp_done_text,
    receive_bulk_totp_secret, handle_bulk_totp_refresh,
    handle_bulk_totp_next, handle_bulk_totp_alldone,
)
from handlers.withdrawal import (
    handle_withdraw, handle_withdraw_method, handle_setup_payment,
    handle_set_upi, handle_set_usdt, receive_upi, receive_usdt,
    receive_withdraw_amt, confirm_withdrawal, cancel_withdrawal_confirm,
    handle_set_upi_text, handle_set_usdt_text,
    handle_withdraw_upi_text, handle_withdraw_usdt_text,
)
from handlers.admin import (
    admin_callback, start_wallet_operation,
    receive_user_search, receive_broadcast,
    receive_wallet_amount, receive_wallet_reason,
    receive_new_price, receive_video_url,
    receive_max_withdraw, receive_withdraw_reject_reason,
)
from database import get_db, init_db, close_pool
from utils import ensure_user_exists, get_instruction_video_url, rate_limiter

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ==================== COMMON HANDLERS ====================

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel any conversation."""
    context.user_data.clear()
    await update.message.reply_text(
        "❌ Cancelled",
        reply_markup=get_main_reply_keyboard(update.effective_user.id), parse_mode="HTML"
    )
    return ConversationHandler.END


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle persistent keyboard buttons — single active message pattern."""
    ensure_user_exists(update.effective_user)
    text = update.message.text.strip()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # Rate limiting
    if not rate_limiter.is_allowed(user_id):
        return  # silently ignore spammed commands

    # Delete user's keyboard text from chat (keeps it clean)
    try:
        await update.message.delete()
    except Exception:
        pass

    # Helper: delete previous bot message + send new one + track it
    async def send_clean(content, reply_markup, parse_mode="HTML"):
        """Delete old bot response, send new one, track the message ID."""
        # Delete previous bot message
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        # Send new message
        msg = await context.bot.send_message(
            chat_id, content, reply_markup=reply_markup, parse_mode=parse_mode
        )
        # Track it
        context.user_data['last_bot_msg'] = msg.message_id
        return msg

    # ── TASKS: Switch to task keyboard ──
    if text == '📋 Tasks':
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id,
            "📋 <b>Task Options</b>\n\nSelect a task type below:",
            reply_markup=get_task_reply_keyboard(),
            parse_mode="HTML"
        )
        return

    # ── Task keyboard actions ──
    if text == '📋 Get Single Task':
        context.user_data.pop('last_bot_msg', None)
        await handle_get_task_text(update, context)
        return

    if text == '📦 Bulk Tasks':
        context.user_data.pop('last_bot_msg', None)
        result = await handle_bulk_task_text(update, context)
        return result  # Returns BULK_TASK_QTY state for ConversationHandler

    if text == '❌ Cancel':
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id, "✅ Returned to main menu.",
            reply_markup=get_main_reply_keyboard(user_id), parse_mode="HTML"
        )
        return

    # ── ADMIN PANEL (keyboard button) ──
    if text == '🔐 Admin Panel':
        if user_id == ADMIN_ID:
            from database import get_db
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM users")
                users = list(c.fetchone().values())[0]
                c.execute("SELECT COUNT(*) FROM gmail WHERE task_status='pending'")
                pg = list(c.fetchone().values())[0]
                c.execute("SELECT COUNT(*) FROM gmail WHERE task_status='in_review'")
                ir = list(c.fetchone().values())[0]
                c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
                pw = list(c.fetchone().values())[0]

            admin_text = (
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
            ]
            await context.bot.send_message(
                chat_id, admin_text,
                reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
            )
        return

    # ── ACTIVE TASK: Account Created ──
    if text == '✅ Account Created':
        from handlers.submission import restore_active_task
        task = restore_active_task(chat_id, context)
        task_id = task['task_id'] if task else None
        if task_id:
            from handlers.submission import handle_task_done_text
            await handle_task_done_text(update, context, task_id)
        else:
            await context.bot.send_message(
                chat_id, "⚠️ No active task found. Get a new task first.",
                reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
            )
        return

    # ── ACTIVE TASK: Video instruction ──
    if text == '🎥 Video instruction':
        from utils import get_instruction_video_url
        video_url = get_instruction_video_url()
        if video_url:
            try:
                await context.bot.send_video(chat_id, video_url)
            except Exception:
                try:
                    await context.bot.send_message(
                        chat_id, f"🎥 <b>Video instruction:</b>\n{video_url}",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
        else:
            await context.bot.send_message(
                chat_id, "📹 No instruction video available yet.",
                parse_mode="HTML"
            )
        return

    # ── ACTIVE TASK: Cancel Task ──
    if text == '❌ Cancel Task':
        from handlers.submission import restore_active_task
        task = restore_active_task(chat_id, context)
        task_id = task['task_id'] if task else None
        if task_id:
            from handlers.submission import handle_task_skip_text
            await handle_task_skip_text(update, context, task_id)
        else:
            await context.bot.send_message(
                chat_id, "✅ No active task.",
                reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
            )
        return

    # ── BALANCE: Show info (no sub-menu needed) ──
    if text == '💰 Balance':
        content, _ = build_balance_content(user_id)
        await send_clean(content, None)
        return

    # ── PROFILE: Show info + profile keyboard ──
    if text == '👤 Profile':
        content, _ = build_profile_content(user_id)
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id, content, reply_markup=get_profile_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── PROFILE SUB: Payment Methods ──
    if text == '💳 Payment Methods':
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id,
            "💳 <b>Setup Payment Method</b>\n\nChoose your preferred method:",
            reply_markup=get_payment_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── PAYMENT SUB: Setup UPI / USDT ──
    # Handled by ConversationHandlers (see main())
    if text in ['📱 Setup UPI', '💎 Setup USDT']:
        return  # ConversationHandler picks these up

    # ── HELP: Show info ──
    if text == '❓ Help':
        content, _ = build_help_content()
        await send_clean(content, None)
        return

    # ── REFERRALS: Show info + referral keyboard ──
    if text == '👥 My Referrals':
        content, _ = build_referral_content(user_id, context.bot.username)
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id, content, reply_markup=get_referral_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── REFERRAL SUB: Leaderboard ──
    if text == '🏆 Leaderboard':
        content, _ = build_leaderboard_content(user_id)
        await send_clean(content, None)
        return

    # ── LEADERBOARD (Top button) ──
    if text == '🏆 Top':
        content, _ = build_leaderboard_content(user_id)
        await send_clean(content, None)
        return

    # ── WITHDRAW: Show info + withdraw keyboard ──
    if text == '💸 Withdraw':
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id,
            "💸 <b>Withdraw</b>\n\nSelect your withdrawal method:",
            reply_markup=get_withdraw_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── WITHDRAW SUB: UPI / USDT ──
    # Handled by ConversationHandlers (see main())
    if text in ['📱 Withdraw UPI', '💎 Withdraw USDT']:
        return  # ConversationHandler picks these up

    # ── SETTINGS: Show info + settings keyboard ──
    if text == '⚙️ Settings':
        content, _ = build_settings_content(user_id)
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id, content, reply_markup=get_settings_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── SETTINGS SUB: Toggle Notifications ──
    if text == '🔔 Toggle Notifications':
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET notifications_enabled = 1 - notifications_enabled WHERE user_id=%s",
                      (user_id,))
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (user_id,))
            new_state = list(c.fetchone().values())[0]
        status = "✅ Enabled" if new_state else "🔕 Disabled"
        # Re-show settings
        content, _ = build_settings_content(user_id)
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id,
            f"🔔 Notifications: <b>{status}</b>\n\n" + content,
            reply_markup=get_settings_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── SETTINGS SUB: Terms & Conditions ──
    if text == '📜 Terms & Conditions':
        from config import WITHDRAWAL_FEE_PERCENT, WITHDRAWAL_FEE_MIN, MAX_WITHDRAWALS_PER_DAY, ALLOWED_DOMAINS, SUPPORT_USERNAME
        terms = (
            f"📜 <b>Terms &amp; Conditions</b>\n\n"
            f"1. Create accounts exactly as shown in tasks\n"
            f"2. No fake or stolen accounts\n"
            f"3. Minimum withdrawal: ₹100\n"
            f"4. Maximum {MAX_WITHDRAWALS_PER_DAY} withdrawals/day\n"
            f"5. Withdrawal fee: {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})\n"
            f"6. Processing time: 24-48 hours\n"
            f"7. Only {', '.join(ALLOWED_DOMAINS)} allowed\n"
            f"8. Referral rewards after first verified task\n"
            f"9. Suspicious activity = account suspension\n\n"
            f"📞 Support: @{SUPPORT_USERNAME}"
        )
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        msg = await context.bot.send_message(
            chat_id, terms, reply_markup=get_settings_keyboard(), parse_mode="HTML"
        )
        context.user_data['last_bot_msg'] = msg.message_id
        return

    # ── BACK: Return to main menu ──
    if text == '🔙 Back':
        prev_msg_id = context.user_data.get('last_bot_msg')
        if prev_msg_id:
            try:
                await context.bot.delete_message(chat_id, prev_msg_id)
            except Exception:
                pass
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id, "✅ Main menu",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )
        return

    # ── Fallback ──
    lower = text.lower()
    if lower in ['start', 'menu', 'hi', 'hello', 'hey']:
        context.user_data.pop('last_bot_msg', None)
        await start(update, context)
    # Silently ignore unrecognized text (no messy replies)

async def handle_video_instruction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the instruction video to the user."""
    q = update.callback_query
    await q.answer()

    video_url = get_instruction_video_url()
    if not video_url:
        await q.answer("📹 No instruction video available yet.", show_alert=True)
        return

    try:
        await context.bot.send_video(
            chat_id=q.from_user.id,
            video=video_url,
            caption="📹 <b>Video Instruction</b>\n\nWatch this video to learn how to complete the task correctly.",
            parse_mode="HTML"
        )
    except Exception:
        # If video URL fails, try sending as a message with link
        await q.message.reply_text(
            f"📹 <b>Video Instruction</b>\n\n"
            f"Watch the tutorial here:\n{video_url}",
            parse_mode="HTML"
        )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and notify admin."""
    logger.error(f"Exception while handling an update: {context.error}")

    try:
        if update and hasattr(update, 'effective_user'):
            user_id = update.effective_user.id if update.effective_user else "Unknown"
            error_msg = (
                f"⚠️ <b>Error Report</b>\n\n"
                f"User ID: {user_id}\n"
                f"Error: {str(context.error)[:200]}"
            )
            await context.bot.send_message(ADMIN_ID, error_msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to send error notification: {e}")


# ==================== AUTO MESSAGE WORKER ====================

async def auto_message_worker(app: Application):
    """Send auto engagement messages every 6 hours."""
    await asyncio.sleep(30)  # wait for bot startup

    while not _shutdown:
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM system_flags WHERE key='auto_messages_enabled'")
                flag = c.fetchone()
                if not flag or flag['value'] != 'true':
                    await asyncio.sleep(3600)
                    continue

                c.execute("""
                    SELECT message FROM auto_messages
                    WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1
                """)
                msg = c.fetchone()
                if not msg:
                    await asyncio.sleep(3600)
                    continue

                message_text = msg['message']

                c.execute("SELECT user_id FROM users WHERE is_blocked = 0")
                users = c.fetchall()

            sent = 0
            for u in users:
                if _shutdown:
                    break
                try:
                    await app.bot.send_message(u['user_id'], message_text, parse_mode="HTML")
                    sent += 1
                    await asyncio.sleep(0.05)  # Throttle: ~20 msg/sec to avoid Telegram flood
                except Exception:
                    pass

            logger.info(f"📢 Auto message sent to {sent} users")
        except asyncio.CancelledError:
            logger.info("📢 Auto message worker stopped")
            return
        except Exception as e:
            logger.error(f"Auto message error: {e}")

        try:
            await asyncio.sleep(6 * 60 * 60)
        except asyncio.CancelledError:
            logger.info("📢 Auto message worker stopped")
            return


async def task_expiry_worker(app: Application):
    """Check for expired tasks every 2 minutes, mark them expired, and notify users."""
    await asyncio.sleep(60)  # wait for bot startup

    while not _shutdown:
        try:
            with get_db() as conn:
                c = conn.cursor()
                now = datetime.now()

                # Find expired SINGLE tasks (batch_id IS NULL)
                single_cutoff = (now - timedelta(minutes=SINGLE_TASK_EXPIRY_MINUTES)).isoformat()
                c.execute("""
                    SELECT id, user_id, task_id, assigned_email
                    FROM gmail
                    WHERE task_status = 'assigned'
                      AND batch_id IS NULL
                      AND task_assigned_at < %s
                """, (single_cutoff,))
                expired_singles = c.fetchall()

                # Find expired BULK tasks (batch_id IS NOT NULL)
                bulk_cutoff = (now - timedelta(minutes=BULK_TASK_EXPIRY_MINUTES)).isoformat()
                c.execute("""
                    SELECT id, user_id, task_id, batch_id, assigned_email
                    FROM gmail
                    WHERE task_status = 'assigned'
                      AND batch_id IS NOT NULL
                      AND task_assigned_at < %s
                """, (bulk_cutoff,))
                expired_bulks = c.fetchall()

                all_expired = list(expired_singles) + list(expired_bulks)

                if not all_expired:
                    await asyncio.sleep(120)
                    continue

                # Mark all expired
                expired_ids = [row['id'] for row in all_expired]
                c.execute(f"""
                    UPDATE gmail SET task_status = 'expired'
                    WHERE id IN ({','.join(['%s'] * len(expired_ids))})
                """, expired_ids)

                # Decrement total_gmail per user
                user_counts = {}
                for row in all_expired:
                    uid = row['user_id']
                    user_counts[uid] = user_counts.get(uid, 0) + 1

                for uid, count in user_counts.items():
                    c.execute("""
                        UPDATE users SET total_gmail = GREATEST(total_gmail - %s, 0)
                        WHERE user_id = %s
                    """, (count, uid))

            # Notify users (outside DB transaction)
            # Group by user to send one message per user
            user_tasks = {}
            for row in all_expired:
                uid = row['user_id']
                if uid not in user_tasks:
                    user_tasks[uid] = []
                user_tasks[uid].append(row)

            for uid, tasks in user_tasks.items():
                try:
                    if len(tasks) == 1:
                        text = (
                            f"⏰ <b>Task Expired</b>\n\n"
                            f"Your task <code>{tasks[0]['task_id']}</code> has expired "
                            f"because it was not completed within <b>{SINGLE_TASK_EXPIRY_MINUTES} minutes</b>.\n\n"
                            f"No penalty — you can get a new task anytime."
                        )
                    else:
                        text = (
                            f"⏰ <b>{len(tasks)} Tasks Expired</b>\n\n"
                            f"Your bulk tasks have expired because they were not "
                            f"completed within the time limit.\n\n"
                            f"No penalty — you can get new tasks anytime."
                        )

                    await app.bot.send_message(
                        uid, text, parse_mode="HTML",
                        reply_markup=get_main_reply_keyboard()
                    )
                except Exception:
                    pass

            logger.info(f"⏰ Expired {len(all_expired)} tasks for {len(user_tasks)} users")

        except Exception as e:
            logger.error(f"Task expiry worker error: {e}")

        await asyncio.sleep(120)  # check every 2 minutes


async def verification_worker(app: Application):
    """Background worker: verify pending Gmail submissions via SMTP.
    
    Safety design:
    - Runs every 5 minutes
    - Only checks accounts confirmed at least 3 minutes ago (propagation buffer)
    - Processes max 10 per cycle to avoid Google rate limiting
    - 2-second delay between checks
    - Never blocks or modifies user-facing flows
    """
    from verifier import check_gmail_exists
    await asyncio.sleep(60)  # wait for bot startup

    while not _shutdown:
        try:
            cutoff = (datetime.now() - timedelta(minutes=3)).isoformat()
            with get_db() as conn:
                c = conn.cursor()
                # Find confirmed submissions that are unchecked and at least 3 minutes old
                c.execute("""
                    SELECT id, email, task_confirmed_at
                    FROM gmail
                    WHERE verification_status = 'unchecked'
                      AND task_status = 'confirmed'
                      AND task_confirmed_at IS NOT NULL
                      AND task_confirmed_at::TIMESTAMP < %s::TIMESTAMP
                    ORDER BY task_confirmed_at ASC
                    LIMIT 10
                """, (cutoff,))
                pending = c.fetchall()

            if not pending:
                await asyncio.sleep(300)  # 5 minutes
                continue

            checked = 0
            for row in pending:
                if _shutdown:
                    break

                gid = row['id']
                email = row['email']

                try:
                    status, detail = await check_gmail_exists(email)

                    with get_db() as conn:
                        c = conn.cursor()
                        c.execute("""
                            UPDATE gmail
                            SET verification_status = %s, verification_checked_at = %s
                            WHERE id = %s
                        """, (status, datetime.now().isoformat(), gid))

                    checked += 1
                    logger.info(f"📧 Verification [{status.upper()}] #{gid}: {email} — {detail}")

                except Exception as e:
                    logger.error(f"Error verifying #{gid} ({email}): {e}")

                # Rate limiting: 2-second gap between checks
                await asyncio.sleep(2)

            if checked > 0:
                logger.info(f"📧 Verification worker: checked {checked} accounts this cycle")

        except asyncio.CancelledError:
            logger.info("📧 Verification worker stopped")
            return
        except Exception as e:
            logger.error(f"Verification worker error: {e}")

        try:
            await asyncio.sleep(300)  # 5 minutes
        except asyncio.CancelledError:
            logger.info("📧 Verification worker stopped")
            return


# Shutdown flag for graceful worker termination
_shutdown = False


async def post_init(application):
    """Runs after bot starts and event loop is ready."""
    application.create_task(auto_message_worker(application))
    application.create_task(task_expiry_worker(application))
    application.create_task(verification_worker(application))


async def post_shutdown(application):
    """Runs after the application shuts down. Cleanup resources."""
    global _shutdown
    _shutdown = True
    close_pool()
    logger.info("🔒 Bot shutdown complete")


# ==================== CALLBACK ROUTER ====================

def route_callback(data: str) -> str:
    """Determine which handler group a callback_data belongs to."""
    admin_prefixes = [
        "admin",
        "gmail_queue", "review_user_",
        "in_review_queue", "review_detail_", "send_review_",
        "approve_", "reject_", "approve_all_", "reject_all_",
        "irapprove_", "irreject_",
        "export_pending_", "export_inreview_",
        "withdrawal_queue", "withdraw_approve", "withdraw_reject",
        "user_mgmt", "broadcast", "stats",
        "block_", "wallet_confirm_", "wallet_cancel",
        "admin_settings", "set_price", "set_max_withdraw", "toggle_tasks", "toggle_bulk", "set_video",
    ]
    submission_prefixes = [
        "get_task", "task_done_", "task_skip_",
        "bulk_task", "bulk_qty_", "bulk_done_", "bulk_cancel_",
        "totp_refresh_", "totp_done_",
        "btotp_refresh_", "btotp_next_", "btotp_alldone_",
        "video_instruction",
    ]
    withdrawal_prefixes = [
        "withdraw", "setup_payment", "set_upi", "set_usdt",
    ]

    for prefix in submission_prefixes:
        if data == prefix or data.startswith(prefix):
            return "submission"
    for prefix in admin_prefixes:
        if data == prefix or data.startswith(prefix):
            return "admin"
    for prefix in withdrawal_prefixes:
        if data == prefix or data.startswith(prefix):
            # Don't route withdraw_approve/reject to withdrawal handler
            if data.startswith("withdraw_approve") or data.startswith("withdraw_reject"):
                return "admin"
            if data.startswith("withdrawal_queue"):
                return "admin"
            return "withdrawal"
    return "user"


async def main_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main callback router — dispatches to the correct handler module."""
    q = update.callback_query
    if not q or not q.data:
        return

    # Auto-register old users who interact without hitting /start
    ensure_user_exists(q.from_user)

    # Rate limiting
    if q.from_user and not rate_limiter.is_allowed(q.from_user.id):
        await q.answer("⚠️ Slow down! Try again in a moment.", show_alert=True)
        return

    route = route_callback(q.data)

    if route == "submission":
        if q.data == "get_task":
            return await handle_get_task(update, context)
        # task_done_ and bulk_done_ are handled by ConversationHandlers
        elif q.data.startswith("task_skip_"):
            return await handle_task_skip(update, context)
        elif q.data == "bulk_task":
            return await handle_bulk_task(update, context)
        elif q.data.startswith("bulk_qty_"):
            return await handle_bulk_qty(update, context)
        elif q.data.startswith("bulk_cancel_"):
            return await handle_bulk_cancel(update, context)
        elif q.data == "video_instruction":
            return await handle_video_instruction(update, context)

    elif route == "admin":
        return await admin_callback(update, context)

    elif route == "withdrawal":
        if q.data == "withdraw":
            return await handle_withdraw(update, context)
        elif q.data in ("withdraw_upi", "withdraw_usdt"):
            return await handle_withdraw_method(update, context)
        elif q.data == "setup_payment":
            return await handle_setup_payment(update, context)
        elif q.data == "set_upi":
            return await handle_set_upi(update, context)
        elif q.data == "set_usdt":
            return await handle_set_usdt(update, context)

    else:
        return await user_callback(update, context)


# ==================== MAIN ====================

def main():
    logger.info("🚀 Starting EarnX Bot...")
    init_db()

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()

    # ── UPI setup conversation ──
    upi_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_set_upi, pattern="^set_upi$"),
            MessageHandler(filters.Regex(r'^📱 Setup UPI$'), handle_set_upi_text),
        ],
        states={UPI_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_upi)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── USDT setup conversation ──
    usdt_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_set_usdt, pattern="^set_usdt$"),
            MessageHandler(filters.Regex(r'^💎 Setup USDT$'), handle_set_usdt_text),
        ],
        states={USDT_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_usdt)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── Withdrawal conversation ──
    withdraw_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_withdraw_method, pattern="^withdraw_(upi|usdt)$"),
            MessageHandler(filters.Regex(r'^📱 Withdraw UPI$'), handle_withdraw_upi_text),
            MessageHandler(filters.Regex(r'^💎 Withdraw USDT$'), handle_withdraw_usdt_text),
        ],
        states={
            WITHDRAW_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_withdraw_amt)],
            WITHDRAW_CONFIRM: [
                CallbackQueryHandler(confirm_withdrawal, pattern=r"^wdraw_yes$"),
                CallbackQueryHandler(cancel_withdrawal_confirm, pattern=r"^wdraw_no$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── User management conversation ──
    user_mgmt_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^user_mgmt$")],
        states={USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_user_search)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── Broadcast conversation ──
    broadcast_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^broadcast$")],
        states={BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_broadcast)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── Wallet operation conversation ──
    wallet_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_wallet_operation, pattern="^wallet_(add|deduct)_")],
        states={
            WALLET_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_wallet_amount)],
            WALLET_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_wallet_reason)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── Register handlers (ORDER MATTERS) ──
    app.add_handler(CommandHandler("start", start))

    # Conversation handlers first (they consume messages)
    app.add_handler(upi_conv)
    app.add_handler(usdt_conv)
    app.add_handler(withdraw_conv)
    app.add_handler(user_mgmt_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(wallet_conv)

    # ── Price setting conversation ──
    price_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^set_price$")],
        states={ADMIN_SET_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_new_price)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(price_conv)

    # ── Video instruction setting conversation ──
    video_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^set_video$")],
        states={ADMIN_SET_VIDEO: [
            MessageHandler(filters.VIDEO & ~filters.COMMAND, receive_video_url),
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_video_url),
        ]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(video_conv)

    # ── Max withdrawal setting conversation ──
    max_withdraw_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^set_max_withdraw$")],
        states={ADMIN_SET_MAX_WITHDRAW: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_max_withdraw)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(max_withdraw_conv)

    # ── Withdrawal rejection comment conversation ──
    reject_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_callback, pattern="^withdraw_reject_confirm_")],
        states={WITHDRAW_REJECT_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_withdraw_reject_reason)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(reject_conv)

    # ── Single task 2FA conversation ──
    async def _account_created_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Entry point for '✅ Account Created' keyboard button."""
        from handlers.submission import restore_active_task, handle_task_done_text
        task = restore_active_task(update.effective_user.id, context)
        task_id = task['task_id'] if task else None
        if not task_id:
            await context.bot.send_message(
                update.effective_chat.id,
                "⚠️ No active task found. Get a new task first.",
                reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
            )
            return ConversationHandler.END
        return await handle_task_done_text(update, context, task_id)

    totp_single_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_task_done, pattern=r"^task_done_"),
            MessageHandler(filters.Regex(r'^✅ Account Created$'), _account_created_entry),
        ],
        states={
            TOTP_SECRET: [
                # Keyboard button handlers FIRST (before generic text)
                MessageHandler(filters.Regex(r'^🔄 Refresh OTP$'), handle_totp_refresh_text),
                MessageHandler(filters.Regex(r'^✅ Submit Task$'), handle_totp_done_text),
                MessageHandler(filters.Regex(r'^❌ Cancel Task$'), handle_cancel_task_text),
                # Generic text handler (for secret key input)
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_totp_secret),
                # Callback handlers (legacy inline buttons)
                CallbackQueryHandler(handle_totp_refresh, pattern=r"^totp_refresh_"),
                CallbackQueryHandler(handle_totp_done, pattern=r"^totp_done_"),
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(r'^❌ Cancel Task$'), handle_cancel_task_text),
            CommandHandler("cancel", cancel),
        ],
        allow_reentry=True,
    )
    app.add_handler(totp_single_conv)

    # ── Bulk task 2FA conversation ──
    totp_bulk_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_bulk_done, pattern=r"^bulk_done_")],
        states={
            TOTP_BULK_SECRET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_bulk_totp_secret),
                CallbackQueryHandler(handle_bulk_totp_refresh, pattern=r"^btotp_refresh_"),
                CallbackQueryHandler(handle_bulk_totp_next, pattern=r"^btotp_next_"),
                CallbackQueryHandler(handle_bulk_totp_alldone, pattern=r"^btotp_alldone_"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )
    app.add_handler(totp_bulk_conv)

    # Generic callback handler (catches everything else)
    app.add_handler(CallbackQueryHandler(main_callback), group=1)

    # Text fallback
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages))

    # Error handler
    app.add_error_handler(error_handler)

    # ── Webhook vs Polling ──
    # Auto-detect Render or Railway
    render_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "")
    webhook_url = WEBHOOK_URL or \
        (f"https://{RAILWAY_PUBLIC_DOMAIN}" if RAILWAY_PUBLIC_DOMAIN else "") or \
        (f"https://{render_host}" if render_host else "")

    if webhook_url:
        full_webhook = f"{webhook_url}/webhook/{BOT_TOKEN}"
        print(f"🌐 Running in WEBHOOK mode on port {PORT}")
        print(f"   Webhook: {full_webhook}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=f"/webhook/{BOT_TOKEN}",
            webhook_url=full_webhook,
        )
    else:
        print("🔄 Running in POLLING mode (local development)")
        app.run_polling()


if __name__ == "__main__":
    main()