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
    USDT_ADDRESS, UPI_ID, WITHDRAW_AMT,
    BROADCAST_MSG, USER_SEARCH,
    WALLET_AMOUNT, WALLET_REASON, ADMIN_SET_PRICE,
    TASK_CONFIRM, BULK_TASK_QTY, BULK_TASK_CONFIRM,
    SINGLE_TASK_EXPIRY_MINUTES, BULK_TASK_EXPIRY_MINUTES,
    TOTP_SECRET, TOTP_BULK_SECRET, ADMIN_SET_VIDEO,
)
from database import get_db, init_db

# Handlers
from handlers.user import (
    start, user_callback,
    get_main_reply_keyboard, get_task_reply_keyboard,
    get_profile_keyboard, get_payment_keyboard,
    get_settings_keyboard, get_referral_keyboard, get_withdraw_keyboard,
    build_balance_content, build_profile_content,
    build_help_content, build_referral_content, build_leaderboard_content,
    build_settings_content,
)
from handlers.submission import (
    handle_get_task, handle_task_done, handle_task_skip,
    handle_bulk_task, handle_bulk_qty,
    handle_bulk_done, handle_bulk_cancel,
    handle_get_task_text, handle_bulk_task_text,
    # 2FA handlers
    receive_totp_secret, handle_totp_refresh, handle_totp_done,
    receive_bulk_totp_secret, handle_bulk_totp_refresh,
    handle_bulk_totp_next, handle_bulk_totp_alldone,
)
from handlers.withdrawal import (
    handle_withdraw, handle_withdraw_method, handle_setup_payment,
    handle_set_upi, handle_set_usdt,
    receive_upi, receive_usdt, receive_withdraw_amt,
)
from handlers.admin import (
    admin_callback, start_wallet_operation,
    receive_user_search, receive_broadcast,
    receive_wallet_amount, receive_wallet_reason,
    receive_new_price, receive_video_url,
)
from utils import ensure_user_exists, get_instruction_video_url

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ==================== COMMON HANDLERS ====================

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel any conversation."""
    context.user_data.clear()
    kb = [[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]
    await update.message.reply_text("❌ Cancelled", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle persistent keyboard buttons — single active message pattern."""
    ensure_user_exists(update.effective_user)
    text = update.message.text.strip()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

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

    # ── PAYMENT SUB: Setup UPI ──
    if text == '📱 Setup UPI':
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id,
            "📱 <b>Setup UPI</b>\n\nTap below to start:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📱 Setup UPI", callback_data="set_upi")],
            ]),
            parse_mode="HTML"
        )
        return

    # ── PAYMENT SUB: Setup USDT ──
    if text == '💎 Setup USDT':
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id,
            "💎 <b>Setup USDT</b>\n\nTap below to start:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Setup USDT", callback_data="set_usdt")],
            ]),
            parse_mode="HTML"
        )
        return

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

    # ── WITHDRAW SUB: UPI ──
    if text == '📱 Withdraw UPI':
        context.user_data.pop('last_bot_msg', None)
        # Trigger withdraw via inline callback
        await context.bot.send_message(
            chat_id, "💸 <b>Withdraw via UPI</b>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📱 Withdraw UPI", callback_data="withdraw_upi")],
            ]),
            parse_mode="HTML"
        )
        return

    # ── WITHDRAW SUB: USDT ──
    if text == '💎 Withdraw USDT':
        context.user_data.pop('last_bot_msg', None)
        await context.bot.send_message(
            chat_id, "💸 <b>Withdraw via USDT</b>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Withdraw USDT", callback_data="withdraw_usdt")],
            ]),
            parse_mode="HTML"
        )
        return

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

    while True:
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
                try:
                    await app.bot.send_message(u['user_id'], message_text, parse_mode="HTML")
                    sent += 1
                except Exception:
                    pass

            logger.info(f"📢 Auto message sent to {sent} users")
        except Exception as e:
            logger.error(f"Auto message error: {e}")

        await asyncio.sleep(6 * 60 * 60)


async def task_expiry_worker(app: Application):
    """Check for expired tasks every 2 minutes, mark them expired, and notify users."""
    await asyncio.sleep(60)  # wait for bot startup

    while True:
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

                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Get New Task", callback_data="get_task")],
                        [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
                    ])
                    await app.bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
                except Exception:
                    pass

            logger.info(f"⏰ Expired {len(all_expired)} tasks for {len(user_tasks)} users")

        except Exception as e:
            logger.error(f"Task expiry worker error: {e}")

        await asyncio.sleep(120)  # check every 2 minutes


async def post_init(application):
    """Runs after bot starts and event loop is ready."""
    application.create_task(auto_message_worker(application))
    application.create_task(task_expiry_worker(application))


# ==================== CALLBACK ROUTER ====================

def route_callback(data: str) -> str:
    """Determine which handler group a callback_data belongs to."""
    admin_prefixes = [
        "admin",
        "gmail_queue", "review_user_",
        "in_review_queue", "review_detail_", "send_review_",
        "approve_", "reject_", "approve_all_", "reject_all_",
        "irapprove_", "irreject_",
        "withdrawal_queue", "withdraw_approve", "withdraw_reject",
        "user_mgmt", "broadcast", "stats",
        "block_", "wallet_confirm_", "wallet_cancel",
        "admin_settings", "set_price", "toggle_tasks", "toggle_bulk", "set_video",
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
    print("🚀 Starting EarnX Bot...")
    init_db()

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # ── UPI setup conversation ──
    upi_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_set_upi, pattern="^set_upi$")],
        states={UPI_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_upi)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── USDT setup conversation ──
    usdt_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_set_usdt, pattern="^set_usdt$")],
        states={USDT_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_usdt)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # ── Withdrawal conversation ──
    withdraw_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_withdraw_method, pattern="^withdraw_(upi|usdt)$"),
        ],
        states={
            WITHDRAW_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_withdraw_amt)],
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

    # ── Single task 2FA conversation ──
    totp_single_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_task_done, pattern=r"^task_done_")],
        states={
            TOTP_SECRET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_totp_secret),
                CallbackQueryHandler(handle_totp_refresh, pattern=r"^totp_refresh_"),
                CallbackQueryHandler(handle_totp_done, pattern=r"^totp_done_"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
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