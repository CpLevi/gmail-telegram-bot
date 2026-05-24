"""
EarnX Gmail Bot — Task-Based Submission Handlers
Single task, bulk task, confirmation, skip, and 2FA flows.
"""

import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    ADMIN_ID, TASK_CONFIRM, BULK_TASK_QTY, BULK_TASK_CONFIRM,
    SINGLE_TASK_EXPIRY_MINUTES, BULK_TASK_EXPIRY_MINUTES,
    TOTP_SECRET, TOTP_BULK_SECRET,
    COOKIE_INPUT, BULK_COOKIE_INPUT,
)
from database import get_db
from utils import (
    can_submit_gmail, update_submit_time, calc_rate, mask_email,
    safe_edit_or_reply, is_blocked, notify_user,
    is_task_submission_enabled, is_bulk_submission_enabled,
    validate_totp_secret, generate_totp, get_totp_remaining_seconds,
)
from generator import generate_single_task, generate_bulk_tasks, save_task_to_db, confirm_task, skip_task

logger = logging.getLogger(__name__)


# ==================== SINGLE TASK FLOW ====================

async def handle_get_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User taps 'Get Task' — generate and display a task card."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    # Check if task submission is enabled
    if not is_task_submission_enabled():
        await safe_edit_or_reply(
            q,
            "🚫 <b>Task Submission Paused</b>\n\n"
            "Task submissions are currently paused by the admin.\n"
            "Please check back later.\n\n"
            "💡 <i>You'll be able to get tasks once submissions resume.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]),
        )
        return

    if is_blocked(uid):
        await q.answer("Your account is blocked", show_alert=True)
        return

    # Check cooldown
    can_submit, wait_time = can_submit_gmail(uid)
    if not can_submit:
        await q.answer(f"⏳ Please wait {wait_time}s", show_alert=True)
        temp_msg = await q.message.reply_text(
            f"⏳ <b>Cooldown Active</b>\n\n"
            f"Please wait <b>{wait_time} seconds</b> before getting a new task.\n"
            f"This helps us process tasks efficiently.",
            parse_mode="HTML"
        )
        await asyncio.sleep(5)
        try:
            await temp_msg.delete()
        except Exception:
            pass
        return

    # Generate task
    task = generate_single_task(uid)
    if not task:
        await q.message.reply_text(
            "⚠️ <b>Task Generation Failed</b>\n\n"
            "Unable to generate a unique task right now. Please try again in a moment.",
            parse_mode="HTML"
        )
        return

    # Calculate reward
    reward = calc_rate(uid)

    # Save to DB
    gid = save_task_to_db(uid, task, reward)
    if not gid:
        await q.message.reply_text(
            "⚠️ <b>Error</b>\n\nCould not save task. Please try again.",
            parse_mode="HTML"
        )
        return

    # Update user's total_gmail count
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET total_gmail=total_gmail+1 WHERE user_id=%s", (uid,))

    update_submit_time(uid)

    # Store task info in context for callback
    context.user_data['current_task_id'] = task['task_id']
    context.user_data['current_task_gid'] = gid
    text = (
        f"⏳ <b>Review time: {SINGLE_TASK_EXPIRY_MINUTES} min</b> ⏳\n\n"
        f"📋 <b>Task:</b>  📧  Create Gmail (2FA)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>First Name:</b>  <code>{task['first_name']}</code>\n"
        f"👤 <b>Last Name:</b>   <code>{task['last_name']}</code>\n"
        f"🎂 <b>DOB:</b>          <code>{task['dob']}</code>\n"
        f"⚧️ <b>Gender:</b>       <code>{'Male' if task['gender'] == 'M' else 'Female'}</code>\n"
        f"📧 <b>Email:</b>        <code>{task['email']}</code>\n"
        f"🔑 <b>Password:</b>     <code>{task['password']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Reward:</b> ₹{float(reward):.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ You <b>MUST</b> use the information above to register.\n"
        f"❌ If you use your own information, your task will be <b>REJECTED</b>.\n\n"
        f"After registration:\n"
        f"👉 Tap <b>\"✅ Account Created\"</b> below\n"
        f"🔐 Then set up 2FA and send the secret key\n\n"
        f"📝 <i>Tap the details to copy them!</i>"
    )

    from handlers.user import get_active_task_keyboard
    await q.message.reply_text(text, reply_markup=get_active_task_keyboard(task['task_id']), parse_mode="HTML")

    # Notify admin
    try:
        admin_text = (
            f"📋 <b>New Task Assigned</b>\n\n"
            f"User: {q.from_user.first_name} (@{q.from_user.username})\n"
            f"ID: {uid}\n\n"
            f"Task: #{gid} ({task['task_id']})\n"
            f"Name: {task['first_name']} {task['last_name']}\n"
            f"Email: {task['email']}\n"
            f"Password: {task['password']}\n"
            f"Reward: ₹{float(reward)}"
        )
        await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin: {e}")


async def handle_get_task_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry point for 'Get Single Task' keyboard button — generates task directly."""
    user = update.effective_user
    uid = user.id
    chat_id = update.effective_chat.id

    if not is_task_submission_enabled():
        await context.bot.send_message(
            chat_id,
            "🚫 <b>Task Submission Paused</b>\n\n"
            "Task submissions are currently paused by the admin.\n"
            "Please check back later.",
            parse_mode="HTML"
        )
        return

    if is_blocked(uid):
        await context.bot.send_message(chat_id, "⛔ Your account has been blocked.", parse_mode="HTML")
        return

    can_submit, wait_time = can_submit_gmail(uid)
    if not can_submit:
        temp_msg = await context.bot.send_message(
            chat_id,
            f"⏳ <b>Cooldown Active</b>\n\n"
            f"Please wait <b>{wait_time} seconds</b> before getting a new task.",
            parse_mode="HTML"
        )
        await asyncio.sleep(5)
        try:
            await temp_msg.delete()
        except Exception:
            pass
        return

    task = generate_single_task(uid)
    if not task:
        await context.bot.send_message(
            chat_id,
            "⚠️ <b>Task Generation Failed</b>\n\n"
            "Unable to generate a unique task right now. Please try again.",
            parse_mode="HTML"
        )
        return

    reward = calc_rate(uid)
    gid = save_task_to_db(uid, task, reward)
    if not gid:
        await context.bot.send_message(
            chat_id, "⚠️ Could not save task. Please try again.", parse_mode="HTML"
        )
        return

    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET total_gmail=total_gmail+1 WHERE user_id=%s", (uid,))

    update_submit_time(uid)

    context.user_data['current_task_id'] = task['task_id']
    context.user_data['current_task_gid'] = gid

    text = (
        f"⏳ <b>Review time: {SINGLE_TASK_EXPIRY_MINUTES} min</b> ⏳\n\n"
        f"📋 <b>Task:</b>  📧  Create Gmail (2FA)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>First Name:</b>  <code>{task['first_name']}</code>\n"
        f"👤 <b>Last Name:</b>   <code>{task['last_name']}</code>\n"
        f"🎂 <b>DOB:</b>          <code>{task['dob']}</code>\n"
        f"⚧️ <b>Gender:</b>       <code>{'Male' if task['gender'] == 'M' else 'Female'}</code>\n"
        f"📧 <b>Email:</b>        <code>{task['email']}</code>\n"
        f"🔑 <b>Password:</b>     <code>{task['password']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Reward:</b> ₹{float(reward):.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ You <b>MUST</b> use the information above to register.\n"
        f"❌ If you use your own information, your task will be <b>REJECTED</b>.\n\n"
        f"After registration:\n"
        f"👉 Tap <b>\"✅ Account Created\"</b> below\n"
        f"🔐 Then set up 2FA and send the secret key\n\n"
        f"📝 <i>Tap the details to copy them!</i>"
    )

    from handlers.user import get_active_task_keyboard
    await context.bot.send_message(
        chat_id, text, reply_markup=get_active_task_keyboard(task['task_id']), parse_mode="HTML"
    )

    # Notify admin
    try:
        admin_text = (
            f"📋 <b>New Task Assigned</b>\n\n"
            f"User: {user.first_name} (@{user.username})\n"
            f"ID: {uid}\n\n"
            f"Task: #{gid} ({task['task_id']})\n"
            f"Name: {task['first_name']} {task['last_name']}\n"
            f"Email: {task['email']}\n"
            f"Password: {task['password']}\n"
            f"Reward: ₹{float(reward)}"
        )
        await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin: {e}")


async def handle_bulk_task_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry point for 'Bulk Tasks' keyboard button."""
    chat_id = update.effective_chat.id

    if not is_bulk_submission_enabled():
        await context.bot.send_message(
            chat_id,
            "🚫 <b>Bulk Submissions Paused</b>\n\nPlease check back later.",
            parse_mode="HTML"
        )
        return

    await context.bot.send_message(
        chat_id,
        "📦 <b>Bulk Tasks</b>\n\nHow many accounts? (2-20):\n\n/cancel to abort",
        parse_mode="HTML"
    )
    return BULK_TASK_QTY


def restore_active_task(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    """Restore active task state from database to context.user_data if memory was cleared."""
    task_id = context.user_data.get('current_task_id') or context.user_data.get('totp_task_id')
    gid = context.user_data.get('current_task_gid')
    totp_secret = context.user_data.get('totp_secret')

    if task_id and gid:
        return {
            'task_id': task_id,
            'id': gid,
            'totp_secret': totp_secret
        }

    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT id, task_id, totp_secret
                FROM gmail
                WHERE user_id = %s AND task_status = 'assigned'
                ORDER BY id DESC LIMIT 1
            """, (user_id,))
            row = c.fetchone()
            if row:
                context.user_data['current_task_id'] = row['task_id']
                context.user_data['current_task_gid'] = row['id']
                context.user_data['totp_task_id'] = row['task_id']
                if row.get('totp_secret'):
                    context.user_data['totp_secret'] = row['totp_secret']
                return row
    except Exception as e:
        logger.error(f"Error restoring active task for {user_id}: {e}")
    return None


# ==================== TASK DONE / 2FA FLOW ====================

async def handle_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User confirms they created the account — start 2FA setup."""
    q = update.callback_query
    await q.answer()
    task_id = q.data.replace("task_done_", "")

    # Check task is still valid
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM gmail WHERE task_id=%s AND task_status='assigned'", (task_id,))
        if not c.fetchone():
            await q.answer("⚠️ Task already processed or expired", show_alert=True)
            return ConversationHandler.END

    context.user_data['totp_task_id'] = task_id

    await q.message.reply_text(
        f"🔐 <b>2FA Setup Required</b>\n\n"
        f"Task <code>{task_id}</code>\n\n"
        f"Now set up 2-Step Verification on the Gmail account:\n\n"
        f"1️⃣ Go to Gmail → Settings → Security\n"
        f"2️⃣ Enable <b>2-Step Verification</b>\n"
        f"3️⃣ Choose <b>Authenticator App</b>\n"
        f"4️⃣ Tap <b>\"Can't scan it?\"</b> to see the secret key\n"
        f"5️⃣ <b>Copy the secret key</b> and send it here\n\n"
        f"⬇️ <b>Send the secret key now:</b>",
        parse_mode="HTML"
    )
    return TOTP_SECRET


async def receive_totp_secret(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive 2FA secret, generate OTP, show to user."""
    text = update.message.text.strip()
    task = restore_active_task(update.effective_user.id, context)
    task_id = task['task_id'] if task else None

    if not task_id:
        await update.message.reply_text("⚠️ Session expired. Please tap Done on your task again.")
        return ConversationHandler.END

    valid, result = validate_totp_secret(text)
    if not valid:
        await update.message.reply_text(
            f"❌ <b>Invalid Secret Key</b>\n\n{result}\n\n"
            f"Please send the correct secret key:",
            parse_mode="HTML"
        )
        return TOTP_SECRET

    # Store cleaned secret
    context.user_data['totp_secret'] = result

    # Generate OTP
    otp = generate_totp(result)
    remaining = get_totp_remaining_seconds()

    from handlers.user import get_2fa_keyboard
    await update.message.reply_text(
        f"🔑 <b>Your 2FA Code</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📟  <code>{otp}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ Valid for <b>{remaining}s</b>\n\n"
        f"📌 <b>Enter this code in Gmail's 2FA page.</b>\n"
        f"🔄 Code expired? Tap <b>Refresh OTP</b> below.\n"
        f"♾ <i>You can refresh unlimited times!</i>\n\n"
        f"✅ Once 2FA is activated, tap <b>Submit Task</b>.",
        reply_markup=get_2fa_keyboard(), parse_mode="HTML"
    )
    return TOTP_SECRET


async def handle_totp_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Refresh the TOTP code (callback version)."""
    q = update.callback_query
    await q.answer()
    secret = context.user_data.get('totp_secret')
    task_id = context.user_data.get('totp_task_id')

    if not secret or not task_id:
        restore_active_task(q.from_user.id, context)
        secret = context.user_data.get('totp_secret')
        task_id = context.user_data.get('totp_task_id')

    if not secret:
        await q.answer("⚠️ No secret stored. Send it again.", show_alert=True)
        return TOTP_SECRET

    otp = generate_totp(secret)
    remaining = get_totp_remaining_seconds()

    from handlers.user import get_2fa_keyboard
    await safe_edit_or_reply(
        q,
        f"🔑 <b>Your 2FA Code (Refreshed)</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📟  <code>{otp}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ Valid for <b>{remaining}s</b>\n\n"
        f"📌 Enter this code in Gmail, then tap ✅ Submit Task.",
    )
    return TOTP_SECRET


async def handle_totp_refresh_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based: user taps '🔄 Refresh OTP' keyboard button."""
    secret = context.user_data.get('totp_secret')
    if not secret:
        restore_active_task(update.effective_user.id, context)
        secret = context.user_data.get('totp_secret')

    if not secret:
        await update.message.reply_text("⚠️ No secret stored. Send the secret key again.")
        return TOTP_SECRET

    otp = generate_totp(secret)
    remaining = get_totp_remaining_seconds()

    from handlers.user import get_2fa_keyboard
    await update.message.reply_text(
        f"🔑 <b>Your 2FA Code (Refreshed)</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📟  <code>{otp}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ Valid for <b>{remaining}s</b>\n\n"
        f"📌 Enter this code in Gmail, then tap <b>✅ Submit Task</b>.",
        reply_markup=get_2fa_keyboard(), parse_mode="HTML"
    )
    return TOTP_SECRET


async def handle_totp_done_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based: user taps '✅ Submit Task' keyboard button."""
    task_id = context.user_data.get('totp_task_id')
    secret = context.user_data.get('totp_secret')
    chat_id = update.effective_chat.id

    if not task_id or not secret:
        restore_active_task(chat_id, context)
        task_id = context.user_data.get('totp_task_id')
        secret = context.user_data.get('totp_secret')

    if not task_id:
        from handlers.user import get_main_reply_keyboard
        await context.bot.send_message(
            chat_id, "⚠️ Session expired. Get a new task.",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )
        return ConversationHandler.END

    # Save TOTP secret to DB
    if secret:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE gmail SET totp_secret=%s WHERE task_id=%s", (secret, task_id))

    from handlers.user import get_main_reply_keyboard
    if confirm_task(task_id):
        await context.bot.send_message(
            chat_id,
            f"✅ <b>Task Submitted!</b>\n\n"
            f"Task <code>{task_id}</code> submitted with 2FA ✔️\n\n"
            f"⏳ Admin will verify within <b>24-48 hours</b>.\n"
            f"You'll be notified when approved or rejected.\n\n"
            f"💡 <i>Tap Tasks to get another one!</i>",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )
    else:
        await context.bot.send_message(
            chat_id, "⚠️ Task already processed or expired.",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )

    # Cleanup
    context.user_data.pop('totp_task_id', None)
    context.user_data.pop('totp_secret', None)
    context.user_data.pop('current_task_id', None)
    context.user_data.pop('current_task_gid', None)
    return ConversationHandler.END


async def handle_cancel_task_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based: user taps '❌ Cancel Task' during any task/2FA flow."""
    task_id = context.user_data.get('current_task_id') or context.user_data.get('totp_task_id')
    if task_id:
        skip_task(task_id)

    context.user_data.pop('current_task_id', None)
    context.user_data.pop('current_task_gid', None)
    context.user_data.pop('totp_task_id', None)
    context.user_data.pop('totp_secret', None)

    from handlers.user import get_main_reply_keyboard
    await update.message.reply_text(
        "⏭️ <b>Task Cancelled</b>\n\n"
        "No penalty. You can get a new task anytime.",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )
    return ConversationHandler.END


async def handle_totp_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User confirms 2FA is activated — save secret and submit task (callback version)."""
    q = update.callback_query
    await q.answer()
    task_id = context.user_data.get('totp_task_id')
    secret = context.user_data.get('totp_secret')

    if not task_id or not secret:
        restore_active_task(q.from_user.id, context)
        task_id = context.user_data.get('totp_task_id')
        secret = context.user_data.get('totp_secret')

    if not task_id:
        await q.answer("⚠️ Session expired", show_alert=True)
        return ConversationHandler.END

    # Save TOTP secret to DB
    if secret:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE gmail SET totp_secret=%s WHERE task_id=%s", (secret, task_id))

    # Confirm the task
    if confirm_task(task_id):
        await safe_edit_or_reply(
            q,
            f"✅ <b>Task Submitted!</b>\n\n"
            f"Task <code>{task_id}</code> submitted with 2FA ✔️\n\n"
            f"⏳ Admin will verify within <b>24-48 hours</b>.\n"
            f"You'll be notified when approved or rejected.\n\n"
            f"💡 <i>Tap Tasks to get another one!</i>",
        )
    else:
        await q.answer("⚠️ Task already processed or expired", show_alert=True)

    # Cleanup
    context.user_data.pop('totp_task_id', None)
    context.user_data.pop('totp_secret', None)
    context.user_data.pop('current_task_id', None)
    context.user_data.pop('current_task_gid', None)
    return ConversationHandler.END


async def handle_task_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User skips a task."""
    q = update.callback_query
    await q.answer()
    d = q.data

    task_id = d.replace("task_skip_", "")

    skip_task(task_id)

    from handlers.user import get_main_reply_keyboard
    await safe_edit_or_reply(
        q,
        "⏭️ <b>Task Skipped</b>\n\n"
        "No penalty. You can get a new task anytime.\n\n"
        "💡 <i>Tip: Complete tasks to earn rewards!</i>",
    )


async def handle_task_done_text(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: str):
    """Text-based: user taps '✅ Account Created' keyboard button."""
    chat_id = update.effective_chat.id

    # Check task is still valid
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT id FROM gmail WHERE task_id=%s AND task_status='assigned'", (task_id,))
        if not c.fetchone():
            from handlers.user import get_main_reply_keyboard
            await context.bot.send_message(
                chat_id, "⚠️ Task already processed or expired. Get a new task.",
                reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
            )
            context.user_data.pop('current_task_id', None)
            return

    context.user_data['totp_task_id'] = task_id

    await context.bot.send_message(
        chat_id,
        f"🔐 <b>2FA Setup Required</b>\n\n"
        f"Task <code>{task_id}</code>\n\n"
        f"Now set up 2-Step Verification on the Gmail account:\n\n"
        f"1️⃣ Go to Gmail → Settings → Security\n"
        f"2️⃣ Enable <b>2-Step Verification</b>\n"
        f"3️⃣ Choose <b>Authenticator App</b>\n"
        f"4️⃣ Tap <b>\"Can't scan it?\"</b> to see the secret key\n"
        f"5️⃣ <b>Copy the secret key</b> and send it here\n\n"
        f"⬇️ <b>Send the secret key now:</b>",
        parse_mode="HTML"
    )
    return TOTP_SECRET


async def handle_task_skip_text(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id: str):
    """Text-based: user taps '❌ Cancel Task' keyboard button."""
    chat_id = update.effective_chat.id

    skip_task(task_id)
    context.user_data.pop('current_task_id', None)
    context.user_data.pop('current_task_gid', None)

    from handlers.user import get_main_reply_keyboard
    await context.bot.send_message(
        chat_id,
        "⏭️ <b>Task Skipped</b>\n\n"
        "No penalty. You can get a new task anytime.\n\n"
        "💡 <i>Tip: Complete tasks to earn rewards!</i>",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )


# ==================== BULK TASK FLOW ====================

async def handle_bulk_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User taps 'Bulk Tasks' — show quantity picker."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    # Check if task submission is enabled
    if not is_task_submission_enabled():
        await safe_edit_or_reply(
            q,
            "🚫 <b>Task Submission Paused</b>\n\n"
            "Task submissions are currently paused by the admin.\n"
            "Please check back later.\n\n"
            "💡 <i>You'll be able to get tasks once submissions resume.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]),
        )
        return

    # Check if bulk submission is enabled
    if not is_bulk_submission_enabled():
        await safe_edit_or_reply(
            q,
            "🚫 <b>Bulk Submission Disabled</b>\n\n"
            "Bulk task submissions are currently disabled.\n"
            "You can still use single tasks.\n\n"
            "💡 <i>Use \"Get Task\" for single submissions.</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Get Task", callback_data="get_task")],
                [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
            ]),
        )
        return

    if is_blocked(uid):
        await q.answer("Your account is blocked", show_alert=True)
        return

    can_submit, wait_time = can_submit_gmail(uid)
    if not can_submit:
        await q.answer(f"⏳ Please wait {wait_time}s", show_alert=True)
        temp_msg = await q.message.reply_text(
            f"⏳ <b>Cooldown Active</b>\n\nPlease wait <b>{wait_time} seconds</b>.",
            parse_mode="HTML"
        )
        await asyncio.sleep(5)
        try:
            await temp_msg.delete()
        except Exception:
            pass
        return

    rate = float(calc_rate(uid))

    text = (
        f"📦 <b>Bulk Tasks</b>\n\n"
        f"Select how many accounts to create:\n\n"
        f"⚡ Current rate: <b>₹{rate:.0f}/account</b>\n"
        f"⏰ Complete within <b>{BULK_TASK_EXPIRY_MINUTES // 60} hours</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"│ Qty │  Reward              │\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"│  2  │  ₹{rate * 2:.0f}                │\n"
        f"│  5  │  ₹{rate * 5:.0f}              │\n"
        f"│ 10  │  ₹{rate * 10:.0f}             │\n"
        f"│ 15  │  ₹{rate * 15:.0f}             │\n"
        f"│ 20  │  ₹{rate * 20:.0f}             │\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    kb = [
        [
            InlineKeyboardButton("2️⃣", callback_data="bulk_qty_2"),
            InlineKeyboardButton("5️⃣", callback_data="bulk_qty_5"),
            InlineKeyboardButton("🔟", callback_data="bulk_qty_10"),
        ],
        [
            InlineKeyboardButton("1️⃣5️⃣", callback_data="bulk_qty_15"),
            InlineKeyboardButton("2️⃣0️⃣", callback_data="bulk_qty_20"),
        ],
        [InlineKeyboardButton("🔙 Back", callback_data="menu")],
    ]

    await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))


async def handle_bulk_qty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User selected a bulk quantity — generate tasks."""
    q = update.callback_query
    await q.answer("⏳ Generating tasks...")
    uid = q.from_user.id
    d = q.data

    qty = int(d.replace("bulk_qty_", ""))
    reward = calc_rate(uid)

    # Generate batch
    batch_id, tasks = generate_bulk_tasks(uid, qty)

    if not tasks:
        await q.message.reply_text(
            "⚠️ <b>Generation Failed</b>\n\nCould not generate tasks. Try again.",
            parse_mode="HTML"
        )
        return

    # Save all tasks to DB
    saved_ids = []
    for task in tasks:
        gid = save_task_to_db(uid, task, reward)
        if gid:
            saved_ids.append((gid, task))

    if not saved_ids:
        await q.message.reply_text(
            "⚠️ <b>Error</b>\n\nCould not save tasks. Please try again.",
            parse_mode="HTML"
        )
        return

    # Update user total
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET total_gmail=total_gmail+%s WHERE user_id=%s",
                  (len(saved_ids), uid))

    update_submit_time(uid)

    # Store batch info
    context.user_data['current_batch_id'] = batch_id
    context.user_data['current_batch_tasks'] = [t['task_id'] for _, t in saved_ids]

    # Build task list message
    total_reward = float(reward) * len(saved_ids)

    text = (
        f"📦 <b>BULK TASKS — Batch {batch_id}</b>\n"
        f"<i>{len(saved_ids)} accounts to create</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    for idx, (gid, task) in enumerate(saved_ids, 1):
        gender_label = "Male" if task['gender'] == 'M' else "Female"
        text += (
            f"\n{idx}️⃣  <b>#{gid}</b>\n"
            f"👤 <code>{task['first_name']} {task['last_name']}</code>\n"
            f"🎂 {task['dob']}  |  ⚧️ {gender_label}\n"
            f"📧 <code>{task['email']}</code>\n"
            f"🔑 <code>{task['password']}</code>\n"
        )

    text += (
        f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Total Reward:</b> ₹{total_reward:.2f} (₹{float(reward):.0f} × {len(saved_ids)})\n"
        f"⏰ Complete within <b>{BULK_TASK_EXPIRY_MINUTES // 60} hours</b>\n\n"
        f"⚠️ <b>Create ALL accounts EXACTLY as shown!</b>\n"
        f"🚫 <b>You MUST remove each account from your device after creating it, otherwise it will be considered invalid.</b>"
    )

    kb = [
        [InlineKeyboardButton("✅ Submit All — Done", callback_data=f"bulk_done_{batch_id}")],
        [InlineKeyboardButton("❌ Cancel All", callback_data=f"bulk_cancel_{batch_id}")],
        [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
    ]

    await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

    # Notify admin
    try:
        admin_text = (
            f"📦 <b>Bulk Tasks Assigned</b>\n\n"
            f"User: {q.from_user.first_name} (@{q.from_user.username})\n"
            f"ID: {uid}\n"
            f"Batch: {batch_id}\n"
            f"Count: {len(saved_ids)} accounts\n"
            f"Reward: ₹{float(reward)} each\n\n"
        )
        for idx, (gid, task) in enumerate(saved_ids[:5], 1):
            admin_text += f"{idx}. #{gid} — {task['email']}\n"
        if len(saved_ids) > 5:
            admin_text += f"\n...and {len(saved_ids) - 5} more"

        await context.bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to notify admin: {e}")


async def handle_bulk_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User confirms all bulk tasks are done — start per-task 2FA flow."""
    q = update.callback_query
    await q.answer()
    batch_id = q.data.replace("bulk_done_", "")

    # Fetch all assigned tasks in this batch
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            SELECT task_id, assigned_email FROM gmail
            WHERE batch_id = %s AND task_status = 'assigned'
            ORDER BY id
        """, (batch_id,))
        tasks = c.fetchall()

    if not tasks:
        await q.answer("⚠️ Tasks already processed or expired", show_alert=True)
        return ConversationHandler.END

    # Store bulk 2FA state
    context.user_data['bulk_2fa_batch_id'] = batch_id
    context.user_data['bulk_2fa_tasks'] = [
        {'task_id': t['task_id'], 'email': t['assigned_email']} for t in tasks
    ]
    context.user_data['bulk_2fa_index'] = 0

    # Ask for first task's secret
    t = tasks[0]
    await q.message.reply_text(
        f"🔐 <b>2FA Setup — Account 1/{len(tasks)}</b>\n\n"
        f"📧 <code>{t['assigned_email']}</code>\n\n"
        f"Set up 2FA on this account and send the secret key here:",
        parse_mode="HTML"
    )
    return TOTP_BULK_SECRET


async def _show_bulk_task_prompt(message, context):
    """Helper: show prompt for next bulk task's 2FA secret."""
    tasks = context.user_data['bulk_2fa_tasks']
    idx = context.user_data['bulk_2fa_index']
    total = len(tasks)
    t = tasks[idx]

    await message.reply_text(
        f"🔐 <b>2FA Setup — Account {idx + 1}/{total}</b>\n\n"
        f"📧 <code>{t['email']}</code>\n\n"
        f"Send the 2FA secret key for this account:",
        parse_mode="HTML"
    )


async def receive_bulk_totp_secret(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive 2FA secret for a bulk task, generate OTP."""
    text = update.message.text.strip()
    tasks = context.user_data.get('bulk_2fa_tasks', [])
    idx = context.user_data.get('bulk_2fa_index', 0)

    if not tasks or idx >= len(tasks):
        await update.message.reply_text("⚠️ Session expired.")
        return ConversationHandler.END

    valid, result = validate_totp_secret(text)
    if not valid:
        await update.message.reply_text(
            f"❌ <b>Invalid Secret Key</b>\n\n{result}\n\nSend the correct key:",
            parse_mode="HTML"
        )
        return TOTP_BULK_SECRET

    t = tasks[idx]
    context.user_data['bulk_2fa_current_secret'] = result
    otp = generate_totp(result)
    remaining = get_totp_remaining_seconds()
    total = len(tasks)
    is_last = (idx == total - 1)

    next_label = "✅ All Done — Submit Batch" if is_last else f"➡️ Next Account ({idx + 2}/{total})"
    next_cb = f"btotp_alldone_{t['task_id']}" if is_last else f"btotp_next_{t['task_id']}"

    kb = [
        [InlineKeyboardButton("🔄 Refresh OTP", callback_data=f"btotp_refresh_{t['task_id']}")],
        [InlineKeyboardButton(next_label, callback_data=next_cb)],
    ]
    await update.message.reply_text(
        f"🔑 <b>2FA Code — Account {idx + 1}/{total}</b>\n\n"
        f"📧 <code>{t['email']}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📟  <code>{otp}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ Expires in <b>{remaining}s</b>\n\n"
        f"Enter this code in Gmail, then tap the button below.",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
    )
    return TOTP_BULK_SECRET


async def handle_bulk_totp_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Refresh OTP for current bulk task."""
    q = update.callback_query
    await q.answer()
    secret = context.user_data.get('bulk_2fa_current_secret')
    tasks = context.user_data.get('bulk_2fa_tasks', [])
    idx = context.user_data.get('bulk_2fa_index', 0)

    if not secret or not tasks:
        await q.answer("⚠️ Send the secret key first.", show_alert=True)
        return TOTP_BULK_SECRET

    t = tasks[idx]
    otp = generate_totp(secret)
    remaining = get_totp_remaining_seconds()
    total = len(tasks)
    is_last = (idx == total - 1)

    next_label = "✅ All Done — Submit Batch" if is_last else f"➡️ Next Account ({idx + 2}/{total})"
    next_cb = f"btotp_alldone_{t['task_id']}" if is_last else f"btotp_next_{t['task_id']}"

    kb = [
        [InlineKeyboardButton("🔄 Refresh OTP", callback_data=f"btotp_refresh_{t['task_id']}")],
        [InlineKeyboardButton(next_label, callback_data=next_cb)],
    ]
    await safe_edit_or_reply(
        q,
        f"🔑 <b>2FA Code (Refreshed) — {idx + 1}/{total}</b>\n\n"
        f"📧 <code>{t['email']}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"  📟  <code>{otp}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏱ Expires in <b>{remaining}s</b>",
        InlineKeyboardMarkup(kb)
    )
    return TOTP_BULK_SECRET


async def handle_bulk_totp_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save current task's secret and move to next bulk task."""
    q = update.callback_query
    await q.answer()
    tasks = context.user_data.get('bulk_2fa_tasks', [])
    idx = context.user_data.get('bulk_2fa_index', 0)
    secret = context.user_data.get('bulk_2fa_current_secret')

    if not tasks or idx >= len(tasks):
        return ConversationHandler.END

    # Save secret for current task
    t = tasks[idx]
    if secret:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE gmail SET totp_secret=%s WHERE task_id=%s", (secret, t['task_id']))

    # Move to next
    context.user_data['bulk_2fa_index'] = idx + 1
    context.user_data.pop('bulk_2fa_current_secret', None)
    await _show_bulk_task_prompt(q.message, context)
    return TOTP_BULK_SECRET


async def handle_bulk_totp_alldone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """All bulk 2FA secrets collected — confirm and submit batch."""
    q = update.callback_query
    await q.answer()
    tasks = context.user_data.get('bulk_2fa_tasks', [])
    idx = context.user_data.get('bulk_2fa_index', 0)
    secret = context.user_data.get('bulk_2fa_current_secret')
    batch_id = context.user_data.get('bulk_2fa_batch_id')

    # Save last task's secret
    if secret and tasks and idx < len(tasks):
        t = tasks[idx]
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE gmail SET totp_secret=%s WHERE task_id=%s", (secret, t['task_id']))

    context.user_data.pop('bulk_2fa_current_secret', None)

    # Confirm entire batch
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""
            UPDATE gmail
            SET task_status = 'confirmed', task_confirmed_at = %s
            WHERE batch_id = %s AND task_status = 'assigned'
            RETURNING id
        """, (datetime.now().isoformat(), batch_id))
        confirmed = c.fetchall()
        count = len(confirmed)

    if count > 0:
        await safe_edit_or_reply(
            q,
            f"✅ <b>Bulk Submission Complete!</b>\n\n"
            f"Batch <b>{batch_id}</b> — <b>{count}</b> accounts submitted with 2FA ✔️\n\n"
            f"⏳ Admin will verify within <b>24-48 hours</b>.\n"
            f"You'll be notified for each approval/rejection.\n\n"
            f"💡 <i>Keep going — get more tasks!</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📦 More Bulk Tasks", callback_data="bulk_task")],
                [InlineKeyboardButton("📋 Single Task", callback_data="get_task")],
                [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
            ])
        )
    else:
        await q.answer("⚠️ Tasks already processed or expired", show_alert=True)

    # Cleanup
    for key in ['bulk_2fa_batch_id', 'bulk_2fa_tasks', 'bulk_2fa_index',
                 'bulk_2fa_current_secret']:
        context.user_data.pop(key, None)
    return ConversationHandler.END


async def handle_bulk_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User cancels all bulk tasks."""
    q = update.callback_query
    await q.answer()
    d = q.data
    batch_id = d.replace("bulk_cancel_", "")

    # Delete unconfirmed tasks
    with get_db() as conn:
        c = conn.cursor()
        # Get user_id and count before deleting
        c.execute("""
            SELECT user_id, COUNT(*) as cnt FROM gmail
            WHERE batch_id = %s AND task_status = 'assigned'
            GROUP BY user_id
        """, (batch_id,))
        result = c.fetchone()

        if result:
            uid = result['user_id']
            count = result['cnt']
            c.execute("DELETE FROM gmail WHERE batch_id = %s AND task_status = 'assigned'", (batch_id,))
            c.execute("UPDATE users SET total_gmail = GREATEST(total_gmail - %s, 0) WHERE user_id = %s",
                      (count, uid))

    await safe_edit_or_reply(
        q,
        "❌ <b>Bulk Tasks Cancelled</b>\n\nAll tasks from this batch have been removed. No penalty.",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Get Task", callback_data="get_task")],
            [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
        ])
    )
