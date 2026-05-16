"""
EarnX Gmail Bot — Withdrawal & Payment Handlers
Withdrawal flow, UPI/USDT setup, amount entry.
"""

import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    ADMIN_ID, WITHDRAWAL_FEE_PERCENT, WITHDRAWAL_FEE_MIN,
    MAX_WITHDRAWALS_PER_DAY, MAX_PENDING_WITHDRAWALS,
    WITHDRAW_AMT, UPI_ID, USDT_ADDRESS,
)
from database import get_db
from utils import (
    validate_upi, validate_usdt_address, can_withdraw_today,
    calculate_withdrawal_fee, round_decimal, safe_edit_or_reply,
    is_blocked, notify_user,
)

logger = logging.getLogger(__name__)


# ==================== WITHDRAW MENU ====================

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Withdraw main menu."""
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id

    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT balance, usdt_address, upi_id FROM users WHERE user_id=%s", (uid,))
        result = c.fetchone()

        c.execute("SELECT COUNT(*) FROM withdrawals WHERE user_id=%s AND status='pending'", (uid,))
        pending_count = list(c.fetchone().values())[0]

    can_withdraw, remaining = can_withdraw_today(uid)

    if result:
        bal = float(result['balance'])
        usdt = result['usdt_address']
        upi = result['upi_id']

        if not can_withdraw:
            text = (
                f"💸 <b>Withdraw</b>\n\n"
                f"💰 Balance: <b>₹{bal:.2f}</b>\n\n"
                f"⚠️ Daily withdrawal limit reached.\n"
                f"Max {MAX_WITHDRAWALS_PER_DAY}/day. Try again tomorrow."
            )
            kb = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        elif pending_count >= MAX_PENDING_WITHDRAWALS:
            text = (
                f"💸 <b>Withdraw</b>\n\n"
                f"💰 Balance: <b>₹{bal:.2f}</b>\n\n"
                f"⚠️ {pending_count} pending requests. Please wait for processing."
            )
            kb = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        elif bal < 100:
            text = (
                f"💸 <b>Withdraw</b>\n\n"
                f"💰 Balance: <b>₹{bal:.2f}</b>\n\n"
                f"⚠️ Minimum withdrawal: <b>₹100</b>\n"
                f"Keep submitting tasks to reach the threshold!"
            )
            kb = [[InlineKeyboardButton("🔙 Back", callback_data="menu")]]
        else:
            example_fee, example_final = calculate_withdrawal_fee(Decimal("100"))
            text = (
                f"💸 <b>Withdraw</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 Balance: <b>₹{bal:.2f}</b>\n"
                f"📊 Today: <b>{remaining}/{MAX_WITHDRAWALS_PER_DAY}</b> remaining\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💡 <b>Fee:</b> {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})\n"
                f"<i>Example: ₹100 → Fee ₹{float(example_fee):.0f} → You get ₹{float(example_final):.0f}</i>\n\n"
                f"Choose withdrawal method:"
            )
            kb = [
                [InlineKeyboardButton(f"📱 UPI{'  ✅' if upi else ''}", callback_data="withdraw_upi")],
                [InlineKeyboardButton(f"💎 USDT (BEP20){'  ✅' if usdt else ''}", callback_data="withdraw_usdt")],
                [InlineKeyboardButton("💳 Setup Payment", callback_data="setup_payment")],
                [InlineKeyboardButton("🔙 Back", callback_data="menu")],
            ]

        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))
    else:
        await safe_edit_or_reply(
            q, "⚠️ Error occurred",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu")]])
        )


# ==================== WITHDRAWAL METHOD SELECTION ====================

async def handle_withdraw_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle UPI / USDT withdrawal method selection."""
    q = update.callback_query
    await q.answer()
    d = q.data

    if d == "withdraw_upi":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT upi_id FROM users WHERE user_id=%s", (q.from_user.id,))
            result = c.fetchone()
        if not result or not result['upi_id']:
            await q.answer("📱 Please setup UPI first", show_alert=True)
            return
        context.user_data['withdraw_method'] = 'upi'
        await safe_edit_or_reply(
            q,
            "💸 <b>Withdraw via UPI</b>\n\n"
            "Enter amount (minimum ₹100):\n\n"
            "/cancel to abort",
        )
        return WITHDRAW_AMT

    elif d == "withdraw_usdt":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT usdt_address FROM users WHERE user_id=%s", (q.from_user.id,))
            result = c.fetchone()
        if not result or not result['usdt_address']:
            await q.answer("💎 Please setup USDT address first", show_alert=True)
            return
        context.user_data['withdraw_method'] = 'usdt'
        await safe_edit_or_reply(
            q,
            "💸 <b>Withdraw via USDT (BEP20)</b>\n\n"
            "Enter amount (minimum ₹100):\n\n"
            "/cancel to abort",
        )
        return WITHDRAW_AMT


# ==================== PAYMENT SETUP ====================

async def handle_setup_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Payment method setup menu."""
    q = update.callback_query
    await q.answer()

    kb = [
        [InlineKeyboardButton("📱 Setup UPI", callback_data="set_upi")],
        [InlineKeyboardButton("💎 Setup USDT (BEP20)", callback_data="set_usdt")],
        [InlineKeyboardButton("🔙 Back", callback_data="withdraw")],
    ]
    await safe_edit_or_reply(
        q,
        "💳 <b>Setup Payment Method</b>\n\nChoose your preferred method:",
        InlineKeyboardMarkup(kb)
    )


# ==================== TEXT-BASED ENTRY POINTS (for keyboard buttons) ====================

async def handle_set_upi_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry for '📱 Setup UPI' keyboard button."""
    try:
        await update.message.delete()
    except Exception:
        pass
    # Delete previous bot message
    prev_msg_id = context.user_data.get('last_bot_msg')
    if prev_msg_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, prev_msg_id)
        except Exception:
            pass
    context.user_data.pop('last_bot_msg', None)

    from handlers.user import get_main_reply_keyboard
    await context.bot.send_message(
        update.effective_chat.id,
        "📱 <b>Setup UPI</b>\n\nSend your UPI ID (e.g., <code>name@paytm</code>)\n\n/cancel to abort",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )
    return UPI_ID


async def handle_set_usdt_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry for '💎 Setup USDT' keyboard button."""
    try:
        await update.message.delete()
    except Exception:
        pass
    prev_msg_id = context.user_data.get('last_bot_msg')
    if prev_msg_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, prev_msg_id)
        except Exception:
            pass
    context.user_data.pop('last_bot_msg', None)

    from handlers.user import get_main_reply_keyboard
    await context.bot.send_message(
        update.effective_chat.id,
        "💎 <b>Setup USDT</b>\n\nSend your BEP20 (BSC) address\n"
        "<i>Must be 42 characters, starting with 0x</i>\n\n/cancel to abort",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )
    return USDT_ADDRESS


async def handle_withdraw_upi_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry for '📱 Withdraw UPI' keyboard button."""
    try:
        await update.message.delete()
    except Exception:
        pass
    prev_msg_id = context.user_data.get('last_bot_msg')
    if prev_msg_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, prev_msg_id)
        except Exception:
            pass
    context.user_data.pop('last_bot_msg', None)
    uid = update.effective_user.id

    from handlers.user import get_main_reply_keyboard
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT upi_id FROM users WHERE user_id=%s", (uid,))
        result = c.fetchone()
    if not result or not result['upi_id']:
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ Please setup UPI first via Profile → Payment Methods",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )
        return ConversationHandler.END
    context.user_data['withdraw_method'] = 'upi'
    await context.bot.send_message(
        update.effective_chat.id,
        "💸 <b>Withdraw via UPI</b>\n\nEnter amount (minimum ₹100):\n\n/cancel to abort",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )
    return WITHDRAW_AMT


async def handle_withdraw_usdt_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Text-based entry for '💎 Withdraw USDT' keyboard button."""
    try:
        await update.message.delete()
    except Exception:
        pass
    prev_msg_id = context.user_data.get('last_bot_msg')
    if prev_msg_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, prev_msg_id)
        except Exception:
            pass
    context.user_data.pop('last_bot_msg', None)
    uid = update.effective_user.id

    from handlers.user import get_main_reply_keyboard
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT usdt_address FROM users WHERE user_id=%s", (uid,))
        result = c.fetchone()
    if not result or not result['usdt_address']:
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ Please setup USDT first via Profile → Payment Methods",
            reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
        )
        return ConversationHandler.END
    context.user_data['withdraw_method'] = 'usdt'
    await context.bot.send_message(
        update.effective_chat.id,
        "💸 <b>Withdraw via USDT (BEP20)</b>\n\nEnter amount (minimum ₹100):\n\n/cancel to abort",
        reply_markup=get_main_reply_keyboard(), parse_mode="HTML"
    )
    return WITHDRAW_AMT


async def handle_set_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start UPI setup."""
    q = update.callback_query
    await q.answer()
    await safe_edit_or_reply(
        q,
        "📱 <b>Setup UPI</b>\n\nSend your UPI ID (e.g., <code>name@paytm</code>)\n\n/cancel to abort",
    )
    return UPI_ID


async def handle_set_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start USDT setup."""
    q = update.callback_query
    await q.answer()
    await safe_edit_or_reply(
        q,
        "💎 <b>Setup USDT</b>\n\nSend your BEP20 (BSC) address\n"
        "<i>Must be 42 characters, starting with 0x</i>\n\n/cancel to abort",
    )
    return USDT_ADDRESS


# ==================== MESSAGE HANDLERS ====================

async def receive_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive and save UPI ID."""
    upi_id = update.message.text.strip()

    if not validate_upi(upi_id):
        await update.message.reply_text(
            "❌ <b>Invalid UPI ID</b>\n\nFormat: <code>name@bank</code>\n\n/cancel to abort",
            parse_mode="HTML"
        )
        return UPI_ID

    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET upi_id=%s WHERE user_id=%s", (upi_id, update.effective_user.id))

        kb = [[InlineKeyboardButton("🔙 Profile", callback_data="profile")]]
        await update.message.reply_text(
            f"✅ <b>UPI ID Saved</b>\n\nUPI: <code>{upi_id}</code>",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_upi: {e}")
        await update.message.reply_text("⚠️ Error occurred. Please try again.", parse_mode="HTML")
        return ConversationHandler.END


async def receive_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive and save USDT address."""
    addr = update.message.text.strip()

    if not validate_usdt_address(addr):
        await update.message.reply_text(
            "❌ <b>Invalid USDT Address</b>\n\n"
            "Must be 42 characters, starting with <code>0x</code>\n\n/cancel to abort",
            parse_mode="HTML"
        )
        return USDT_ADDRESS

    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET usdt_address=%s WHERE user_id=%s", (addr, update.effective_user.id))

        kb = [[InlineKeyboardButton("🔙 Profile", callback_data="profile")]]
        await update.message.reply_text(
            f"✅ <b>USDT Address Saved</b>\n\nAddress: <code>{addr[:10]}...{addr[-10:]}</code>",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_usdt: {e}")
        await update.message.reply_text("⚠️ Error occurred. Please try again.", parse_mode="HTML")
        return ConversationHandler.END


async def receive_withdraw_amt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive withdrawal amount and process."""
    try:
        amount = Decimal(update.message.text.strip())

        if amount < 100:
            await update.message.reply_text(
                "⚠️ <b>Minimum withdrawal: ₹100</b>\n\nEnter valid amount or /cancel",
                parse_mode="HTML"
            )
            return WITHDRAW_AMT

        can_withdraw, remaining = can_withdraw_today(update.effective_user.id)
        if not can_withdraw:
            await update.message.reply_text(
                f"⚠️ Daily limit reached ({MAX_WITHDRAWALS_PER_DAY}/day). Try tomorrow.",
                parse_mode="HTML"
            )
            return ConversationHandler.END

        method = context.user_data.get('withdraw_method')
        fee, final_amount = calculate_withdrawal_fee(amount)

        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("SELECT balance, usdt_address, upi_id FROM users WHERE user_id=%s",
                          (update.effective_user.id,))
                result = c.fetchone()

                if not result:
                    await update.message.reply_text("⚠️ Error occurred", parse_mode="HTML")
                    return ConversationHandler.END

                balance = round_decimal(result['balance'])

                if amount > balance:
                    await update.message.reply_text(
                        f"⚠️ <b>Insufficient Balance</b>\n\n"
                        f"Balance: ₹{float(balance):.2f}\nRequested: ₹{float(amount):.2f}",
                        parse_mode="HTML"
                    )
                    return WITHDRAW_AMT

                payment_info = result['upi_id'] if method == 'upi' else result['usdt_address']
                method_name = "UPI" if method == 'upi' else "USDT BEP20"

                # ATOMIC DEDUCTION
                c.execute("""
                    UPDATE users SET balance=balance-%s
                    WHERE user_id=%s AND balance >= %s
                    RETURNING balance
                """, (amount, update.effective_user.id, amount))

                updated = c.fetchone()
                if not updated:
                    await update.message.reply_text(
                        "⚠️ Balance changed. Please try again.", parse_mode="HTML"
                    )
                    return ConversationHandler.END

                c.execute("""INSERT INTO withdrawals (user_id, amount, fee, final_amount, method, payment_info, request_date)
                             VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                          (update.effective_user.id, amount, fee, final_amount, method,
                           payment_info, datetime.now().isoformat()))
                wid = c.fetchone()['id']
                conn.commit()

        except Exception as e:
            logger.error(f"Error in withdrawal transaction: {e}")
            await update.message.reply_text("⚠️ Error. Please try again later.", parse_mode="HTML")
            return ConversationHandler.END

        context.user_data.clear()

        kb = [[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]
        await update.message.reply_text(
            f"✅ <b>Withdrawal Requested</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: <b>#{wid}</b>\n"
            f"💰 Amount: <b>₹{float(amount):.2f}</b>\n"
            f"📊 Fee: ₹{float(fee):.2f}\n"
            f"💵 Final: <b>₹{float(final_amount):.2f}</b>\n"
            f"💳 Method: {method_name}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⏳ Processing within <b>24-48 hours</b>",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML"
        )

        # Notify admin
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"💸 <b>New Withdrawal Request</b>\n\n"
                f"User: {update.effective_user.first_name}\n"
                f"ID: {update.effective_user.id}\n\n"
                f"Amount: ₹{float(amount):.2f}\n"
                f"Fee: ₹{float(fee):.2f}\n"
                f"Final: ₹{float(final_amount):.2f}\n"
                f"Method: {method_name}\n"
                f"Payment: {payment_info}",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")

        return ConversationHandler.END

    except (ValueError, InvalidOperation):
        await update.message.reply_text(
            "❌ <b>Invalid amount</b>\n\nEnter a valid number or /cancel",
            parse_mode="HTML"
        )
        return WITHDRAW_AMT
    except Exception as e:
        logger.error(f"Error in receive_withdraw_amt: {e}")
        await update.message.reply_text("⚠️ Error. Please try again later.", parse_mode="HTML")
        return ConversationHandler.END
