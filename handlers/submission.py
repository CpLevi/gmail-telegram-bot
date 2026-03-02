"""
EarnX Gmail Bot — Task-Based Submission Handlers
Single task, bulk task, confirmation, and skip flows.
"""

import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    ADMIN_ID, TASK_CONFIRM, BULK_TASK_QTY, BULK_TASK_CONFIRM,
    SINGLE_TASK_EXPIRY_MINUTES, BULK_TASK_EXPIRY_MINUTES,
)
from database import get_db
from utils import (
    can_submit_gmail, update_submit_time, calc_rate, mask_email,
    safe_edit_or_reply, is_blocked, notify_user,
)
from generator import generate_single_task, generate_bulk_tasks, save_task_to_db, confirm_task, skip_task

logger = logging.getLogger(__name__)


# ==================== SINGLE TASK FLOW ====================

async def handle_get_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User taps 'Get Task' — generate and display a task card."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

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

    # Display task card
    text = (
        f"📋 <b>GMAIL CREATION TASK</b>  #{gid}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>First Name:</b>  <code>{task['first_name']}</code>\n"
        f"👤 <b>Last Name:</b>   <code>{task['last_name']}</code>\n"
        f"🎂 <b>DOB:</b>        <code>{task['dob']}</code>\n"
        f"⚧️ <b>Gender:</b>     <code>{'Male' if task['gender'] == 'M' else 'Female'}</code>\n"
        f"📧 <b>Email:</b>       <code>{task['email']}</code>\n"
        f"🔑 <b>Password:</b>    <code>{task['password']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Reward:</b> ₹{float(reward):.2f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ <b>Create the Gmail account EXACTLY as shown above.</b>\n"
        f"🚫 <b>You MUST remove the account from your device after creating it, otherwise it will be considered invalid.</b>\n"
        f"⏰ You have <b>{SINGLE_TASK_EXPIRY_MINUTES} minutes</b> to complete.\n\n"
        f"📝 <i>Tap the details to copy them!</i>"
    )

    kb = [
        [InlineKeyboardButton("✅ Done — I Created It", callback_data=f"task_done_{task['task_id']}")],
        [InlineKeyboardButton("❌ Skip This Task", callback_data=f"task_skip_{task['task_id']}")],
        [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
    ]

    await q.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")

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


# ==================== TASK DONE / SKIP CALLBACKS ====================

async def handle_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User confirms they created the account."""
    q = update.callback_query
    await q.answer()
    d = q.data

    task_id = d.replace("task_done_", "")

    if confirm_task(task_id):
        await safe_edit_or_reply(
            q,
            f"✅ <b>Task Submitted!</b>\n\n"
            f"Task <code>{task_id}</code> has been submitted for review.\n\n"
            f"⏳ Admin will verify within <b>24-48 hours</b>.\n"
            f"You'll be notified when it's approved or rejected.\n\n"
            f"💡 <i>Tap \"Get Task\" for another one!</i>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Get Another Task", callback_data="get_task")],
                [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
            ])
        )
    else:
        await q.answer("⚠️ Task already processed or expired", show_alert=True)


async def handle_task_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User skips a task."""
    q = update.callback_query
    await q.answer()
    d = q.data

    task_id = d.replace("task_skip_", "")

    skip_task(task_id)

    await safe_edit_or_reply(
        q,
        "⏭️ <b>Task Skipped</b>\n\n"
        "No penalty. You can get a new task anytime.\n\n"
        "💡 <i>Tip: Complete tasks to earn rewards!</i>",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Get New Task", callback_data="get_task")],
            [InlineKeyboardButton("🔙 Menu", callback_data="menu")],
        ])
    )


# ==================== BULK TASK FLOW ====================

async def handle_bulk_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User taps 'Bulk Tasks' — show quantity picker."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

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
    """User confirms all bulk tasks are done."""
    q = update.callback_query
    await q.answer()
    d = q.data
    batch_id = d.replace("bulk_done_", "")

    # Confirm all tasks in this batch
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
            f"Batch <b>{batch_id}</b> — <b>{count}</b> accounts submitted.\n\n"
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
        await q.answer("⚠️ Tasks already processed", show_alert=True)


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
