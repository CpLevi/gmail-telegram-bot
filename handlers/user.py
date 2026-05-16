"""
EarnX Gmail Bot — User Handlers
/start, menu, balance, profile, earnings, history, referral, settings, help, and channel claim.
"""

import logging
import psycopg2
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    ADMIN_ID, TELEGRAM_CHANNEL, SUPPORT_USERNAME, ALLOWED_DOMAINS,
    WITHDRAWAL_FEE_PERCENT, WITHDRAWAL_FEE_MIN, MAX_WITHDRAWALS_PER_DAY,
)
from database import get_db
from utils import (
    is_blocked, notify_user, check_channel,
    calc_rate, get_earnings_stats,
    mask_email, validate_page, safe_edit_or_reply,
    is_task_submission_enabled, is_bulk_submission_enabled,
    get_instruction_video_url,
)

logger = logging.getLogger(__name__)


# ==================== PERSISTENT REPLY KEYBOARD ====================

def get_main_reply_keyboard():
    """Build the persistent bottom keyboard."""
    keyboard = [
        [KeyboardButton("💰 Balance"), KeyboardButton("📋 Tasks")],
        [KeyboardButton("💸 Withdraw"), KeyboardButton("👤 Profile")],
        [KeyboardButton("🏆 Top"), KeyboardButton("⚙️ Settings")],
        [KeyboardButton("👥 My Referrals"), KeyboardButton("❓ Help")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, is_persistent=True)


def get_task_reply_keyboard():
    """Dynamic reply keyboard when user is in task selection."""
    keyboard = [
        [KeyboardButton("📋 Get Single Task")],
        [KeyboardButton("📦 Bulk Tasks")],
        [KeyboardButton("❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, is_persistent=True)


# ==================== STANDALONE CONTENT BUILDERS ====================
# These return (text, InlineKeyboardMarkup) and can be called from
# both callback queries and text message handlers.

def build_balance_content(user_id):
    """Build balance info content."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT balance, total_gmail, approved_gmail FROM users WHERE user_id=%s", (user_id,))
        result = c.fetchone()

        c.execute("SELECT COALESCE(SUM(reward), 0) FROM gmail WHERE user_id=%s AND status='pending'", (user_id,))
        pending = float(list(c.fetchone().values())[0])

        c.execute("SELECT COALESCE(SUM(reward), 0) FROM gmail WHERE user_id=%s AND status='in_review'", (user_id,))
        in_review = float(list(c.fetchone().values())[0])

    bal = float(result['balance']) if result else 0
    approved = result['approved_gmail'] if result else 0
    total = result['total_gmail'] if result else 0
    rate = float(calc_rate(user_id))
    total_pending = pending + in_review

    text = (
        f"💰 <b>Balance: ₹{bal:.2f}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Rate:</b> ₹{rate:.0f}/account\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄 <b>Under Verification</b>\n"
        f"├ ⏳ Pending review: ₹{pending:.2f}\n"
        f"├ 🔍 In review: ₹{in_review:.2f}\n"
        f"└ 💰 Total: ₹{total_pending:.2f}\n\n"
        f"ℹ️ <i>\"In review\" = admin forwarded your Gmail to verification team.</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Statistics</b>\n"
        f"├ ✅ Approved (all time): {approved}\n"
        f"└ 📧 Total submitted: {total}\n"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu")]])
    return text, kb


def build_profile_content(user_id):
    """Build profile info content."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT balance, approved_gmail, usdt_address, upi_id, joined_date FROM users WHERE user_id=%s",
                  (user_id,))
        result = c.fetchone()
        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1", (user_id,))
        ref_count = list(c.fetchone().values())[0]

    if not result:
        return "❌ Profile not found.", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="menu")]])

    bal = float(result['balance'])
    approved = result['approved_gmail']
    usdt = result['usdt_address']
    upi = result['upi_id']
    joined = result['joined_date']
    rate = float(calc_rate(user_id))
    joined_display = joined[:10] if joined else "N/A"

    text = (
        f"👤 <b>Profile</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Balance:</b> ₹{bal:.2f}\n"
        f"⚡ <b>Rate:</b> ₹{rate:.0f}/account\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Activity</b>\n"
        f"├ ✅ Approved (all time): <b>{approved}</b>\n"
        f"└ 👥 Referrals: <b>{ref_count}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 <b>Payment Methods</b>\n"
        f"├ 📱 UPI: {'✅ Set' if upi else '❌ Not set'}\n"
        f"└ 💎 USDT: {'✅ Set' if usdt else '❌ Not set'}\n\n"
        f"📅 <b>Joined:</b> {joined_display}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Payment Methods", callback_data="setup_payment")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu")],
    ])
    return text, kb


def build_help_content():
    """Build help page content."""
    rate = float(calc_rate())
    text = (
        f"❓ <b>Help &amp; Support — EarnX Bot</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>How It Works</b>\n"
        f"1️⃣ Tap <b>\"Get Task\"</b> — bot gives you account details\n"
        f"2️⃣ Create the Gmail account <b>exactly</b> as shown\n"
        f"3️⃣ Tap <b>\"Done\"</b> — submission goes under review\n"
        f"4️⃣ Approved = reward credited!\n"
        f"5️⃣ Withdraw once balance hits ₹100\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 <b>Task Types</b>\n"
        f"• <b>Single Task</b> — one account at a time\n"
        f"• <b>Bulk Tasks</b> — up to 20 accounts\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Reward</b>\n"
        f"• Fixed rate: <b>₹{rate:.0f}</b>/account for everyone\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎁 <b>Bonus Earnings</b>\n"
        f"• Channel join: <b>₹1</b>\n"
        f"• Referral: <b>₹5</b>/friend\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💸 <b>Withdrawals</b>\n"
        f"• Minimum: ₹100\n"
        f"• Fee: {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})\n"
        f"• Limit: {MAX_WITHDRAWALS_PER_DAY}/day\n"
        f"• Methods: UPI &amp; USDT (BEP20)\n\n"
        f"📩 Need help? @{SUPPORT_USERNAME}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📞 Contact Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu")],
    ])
    return text, kb


def build_referral_content(user_id, bot_username):
    """Build referral info content."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s", (user_id,))
        ref_count = list(c.fetchone().values())[0]
        c.execute("SELECT COALESCE(SUM(reward), 0) FROM referrals WHERE referrer_id=%s AND rewarded=1", (user_id,))
        total_earned = float(list(c.fetchone().values())[0])
        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=0", (user_id,))
        pending_refs = list(c.fetchone().values())[0]

    ref_link = f"https://t.me/{bot_username}?start={user_id}"
    text = (
        f"👥 <b>Refer &amp; Earn</b>\n\n"
        f"Earn <b>₹5</b> for each friend you refer!\n"
        f"Reward credited after their first verified task.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Your Stats</b>\n"
        f"├ 👥 Total referrals: <b>{ref_count}</b>\n"
        f"├ ⏳ Pending rewards: <b>{pending_refs}</b>\n"
        f"└ 💰 Total earned: <b>₹{total_earned:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"📲 <i>Share this link with friends!</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="referral_leaderboard")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu")],
    ])
    return text, kb


def build_leaderboard_content(user_id):
    """Build referral leaderboard content."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("""SELECT u.first_name, u.username, u.user_id, COUNT(r.id) as ref_count
                    FROM users u
                    JOIN referrals r ON u.user_id = r.referrer_id
                    WHERE r.rewarded = 1
                    GROUP BY u.user_id, u.first_name, u.username
                    ORDER BY ref_count DESC LIMIT 10""")
        top_referrers = c.fetchall()

        c.execute("""SELECT COUNT(DISTINCT referrer_id) + 1 as rank
                    FROM referrals WHERE rewarded = 1 AND referrer_id IN (
                        SELECT referrer_id FROM referrals WHERE rewarded = 1
                        GROUP BY referrer_id HAVING COUNT(*) > (
                            SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1
                    ))""", (user_id,))
        result = c.fetchone()
        user_rank = list(result.values())[0] if result else "N/A"

        c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1", (user_id,))
        user_refs = list(c.fetchone().values())[0]

    text = "🏆 <b>Referral Leaderboard</b>\n\n"
    if top_referrers:
        medals = ["🥇", "🥈", "🥉"]
        for idx, row in enumerate(top_referrers, 1):
            medal = medals[idx - 1] if idx <= 3 else f"<b>{idx}.</b>"
            text += f"{medal} {row['first_name']} — <b>{row['ref_count']}</b> referrals\n"
    else:
        text += "<i>No referrals yet. Be the first!</i>\n"

    text += f"\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📍 Your rank: <b>#{user_rank}</b>\n"
    text += f"👥 Your referrals: <b>{user_refs}</b>"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="referral")]])
    return text, kb


def build_settings_content(user_id):
    """Build settings page content."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (user_id,))
        result = c.fetchone()
        notif = result['notifications_enabled'] if result else 1

    text = (
        f"⚙️ <b>Settings</b>\n\n"
        f"🔔 Notifications: <b>{'✅ Enabled' if notif else '🔕 Disabled'}</b>\n\n"
        f"📞 Support: @{SUPPORT_USERNAME}"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔕 Disable Notifications" if notif else "🔔 Enable Notifications",
                              callback_data="toggle_notif")],
        [InlineKeyboardButton("📜 Terms & Conditions", callback_data="view_terms")],
        [InlineKeyboardButton("📞 Contact Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu")],
    ])
    return text, kb


# ==================== /START COMMAND ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not update.message:
        return

    if is_blocked(user.id):
        await update.message.reply_text("⛔ Your account has been blocked from using this service.")
        return

    # Handle referral with self-referral protection
    ref_id = None
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id == user.id:
                ref_id = None
        except Exception:
            pass

    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE user_id=%s", (user.id,))
        existing = c.fetchone()

        if not existing:
            c.execute("""INSERT INTO users (user_id, username, first_name, referrer_id, joined_date)
                         VALUES (%s, %s, %s, %s, %s)""",
                      (user.id, user.username, user.first_name, ref_id, datetime.now().isoformat()))

            if ref_id and ref_id != user.id:
                c.execute("SELECT user_id FROM users WHERE user_id=%s", (ref_id,))
                if c.fetchone():
                    try:
                        c.execute("""INSERT INTO referrals (referrer_id, referred_id, reward, date, rewarded)
                                     VALUES (%s, %s, %s, %s, %s)""",
                                  (ref_id, user.id, 5, datetime.now().isoformat(), 0))
                        await notify_user(context, ref_id,
                            f"🎉 <b>New Referral!</b>\n\n"
                            f"<b>{user.first_name}</b> joined via your link.\n"
                            f"You'll earn <b>₹5</b> when they complete their first verified task.")
                    except psycopg2.IntegrityError:
                        pass

    # Fetch dashboard data
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT balance, approved_gmail, channel_claimed FROM users WHERE user_id=%s", (user.id,))
        result = c.fetchone()
        bal = float(result['balance']) if result else 0
        approved = result['approved_gmail'] if result else 0
        claimed = result['channel_claimed'] if result else 0

        c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='pending'", (user.id,))
        pending = list(c.fetchone().values())[0]

    rate = float(calc_rate(user.id))

    text = (
        f"🏦 <b>EarnX — Gmail Task Bot</b>\n\n"
        f"Welcome, <b>{user.first_name}</b>! 👋\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Your Dashboard</b>\n"
        f"├ 💰 Balance: <b>₹{bal:.2f}</b>\n"
        f"├ ⚡ Rate: <b>₹{rate:.0f}/account</b>\n"
        f"├ ✅ Approved: <b>{approved}</b>\n"
        f"└ ⏳ Pending: <b>{pending}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡 <i>Use the keyboard buttons below to navigate.</i>\n"
        f"📋 <i>Tap <b>Tasks</b> to start earning!</i>"
    )

    # Only show inline buttons for channel bonus (if unclaimed)
    if not claimed:
        text += f"\n\n⚡ Join <b>{TELEGRAM_CHANNEL}</b> to claim ₹1 bonus!"
        channel_url = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}"
        kb = [
            [InlineKeyboardButton("📢 Join Channel", url=channel_url)],
            [InlineKeyboardButton("🎁 Claim ₹1 Bonus", callback_data="claim_channel")],
        ]
        await update.message.reply_text(text, reply_markup=get_main_reply_keyboard(), parse_mode="HTML")
        await update.message.reply_text(
            "🎁 <b>Claim your bonus:</b>",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(text, reply_markup=get_main_reply_keyboard(), parse_mode="HTML")


# ==================== USER CALLBACK HANDLER ====================

async def user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all user-facing callbacks (non-admin, non-submission)."""
    q = update.callback_query
    await q.answer()
    d = q.data

    if is_blocked(q.from_user.id) and q.from_user.id != ADMIN_ID:
        await q.answer("Your account is blocked", show_alert=True)
        return

    # ── CHANNEL CLAIM ──
    if d == "claim_channel":
        await q.answer("Checking membership...", show_alert=False)
        if await check_channel(q.from_user.id, context):
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE users SET balance=balance+1, channel_claimed=1
                    WHERE user_id=%s AND channel_claimed=0
                    RETURNING user_id
                """, (q.from_user.id,))
                result = c.fetchone()
                if result:
                    conn.commit()
                    await q.answer("✅ ₹1 added to your balance!", show_alert=True)
                    await q.message.reply_text(
                        "🎉 <b>Bonus Credited: ₹1</b>\n\nThank you for joining our channel!",
                        parse_mode="HTML"
                    )
                else:
                    await q.answer("You've already claimed this bonus", show_alert=True)
        else:
            await q.answer(f"Please join {TELEGRAM_CHANNEL} first", show_alert=True)
        return

    # ── MAIN MENU ──
    if d == "menu":
        kb = []
        if is_task_submission_enabled():
            kb.append([InlineKeyboardButton("📋 Get Task", callback_data="get_task")])
            if is_bulk_submission_enabled():
                kb.append([InlineKeyboardButton("📦 Bulk Tasks", callback_data="bulk_task")])
        else:
            kb.append([InlineKeyboardButton("🚫 Tasks Paused", callback_data="tasks_paused")])
        kb += [
            [InlineKeyboardButton("💰 Balance", callback_data="balance"),
             InlineKeyboardButton("📋 History", callback_data="history")],
            [InlineKeyboardButton("💸 Withdraw", callback_data="withdraw"),
             InlineKeyboardButton("👤 Profile", callback_data="profile")],
            [InlineKeyboardButton("👥 Refer & Earn", callback_data="referral")],
            [InlineKeyboardButton("📊 Earnings", callback_data="earnings")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
             InlineKeyboardButton("❓ Help", callback_data="help")],
        ]
        if q.from_user.id == ADMIN_ID:
            kb.append([InlineKeyboardButton("🔐 ADMIN PANEL", callback_data="admin")])
        await safe_edit_or_reply(q, "📋 <b>Main Menu</b>\n\nChoose an option below:", InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    # ── BALANCE ──
    elif d == "balance":
        text, kb = build_balance_content(q.from_user.id)
        await safe_edit_or_reply(q, text, kb)

    # ── EARNINGS ──
    elif d == "earnings" or d.startswith("earnings_"):
        period = d.split("_")[1] if "_" in d else "all"
        stats = get_earnings_stats(q.from_user.id, period)

        period_names = {'today': '📅 Today', 'week': '📅 This Week', 'month': '📅 This Month', 'all': '📅 All Time'}

        text = (
            f"📊 <b>Earnings Dashboard</b>\n\n"
            f"<b>Period:</b> {period_names.get(period, 'All Time')}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 📧 Gmail: <b>₹{stats['gmail']:.2f}</b>\n"
            f"├ 👥 Referrals: <b>₹{stats['referral']:.2f}</b>\n"
            f"├ 📢 Channel bonus: <b>₹{stats['channel']:.2f}</b>\n"
            f"└ 💰 Total: <b>₹{stats['total']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )

        kb = [
            [InlineKeyboardButton("Today", callback_data="earnings_today"),
             InlineKeyboardButton("Week", callback_data="earnings_week")],
            [InlineKeyboardButton("Month", callback_data="earnings_month"),
             InlineKeyboardButton("All Time", callback_data="earnings_all")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu")],
        ]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── REFERRAL ──
    elif d == "referral":
        text, kb = build_referral_content(q.from_user.id, context.bot.username)
        await safe_edit_or_reply(q, text, kb)

    # ── REFERRAL LEADERBOARD ──
    elif d == "referral_leaderboard":
        text, kb = build_leaderboard_content(q.from_user.id)
        await safe_edit_or_reply(q, text, kb)

    # ── HISTORY (GMAIL) ──
    elif d == "history" or d.startswith("history_gmail_"):
        page = validate_page(d.split("_")[-1]) if "_" in d else 0
        offset = page * 5

        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT email, status, reward, submit_date, rejection_reason, task_id,
                        assigned_first_name, assigned_last_name
                        FROM gmail WHERE user_id=%s ORDER BY submit_date DESC
                        LIMIT 5 OFFSET %s""", (q.from_user.id, offset))
            subs = c.fetchall()

            c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s", (q.from_user.id,))
            total = list(c.fetchone().values())[0]

        text = f"📋 <b>Gmail History</b> — Page {page + 1}\n\n"
        if subs:
            for sub in subs:
                status_emojis = {"pending": "⏳", "in_review": "🔍", "approved": "✅", "rejected": "❌"}
                emoji = status_emojis.get(sub['status'], "❓")
                reward_val = float(sub['reward']) if sub['reward'] else 0

                name_tag = ""
                if sub.get('assigned_first_name') and sub.get('assigned_last_name'):
                    name_tag = f" ({sub['assigned_first_name']} {sub['assigned_last_name']})"

                text += f"{emoji} <code>{mask_email(sub['email'])}</code>{name_tag}\n"
                text += f"    {sub['status'].title()} — ₹{reward_val:.2f}"
                if sub['rejection_reason']:
                    text += f"\n    <i>Reason: {sub['rejection_reason']}</i>"
                text += "\n\n"
        else:
            text += "<i>No submissions yet. Tap \"Get Task\" to start!</i>"

        kb = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"history_gmail_{page - 1}"))
        if offset + 5 < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_gmail_{page + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("💸 Withdrawal History", callback_data="history_withdrawal_0")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── WITHDRAWAL HISTORY ──
    elif d.startswith("history_withdrawal_"):
        page = validate_page(d.split("_")[-1])
        offset = page * 5

        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT amount, fee, final_amount, method, status, request_date, processed_date, rejection_reason
                        FROM withdrawals WHERE user_id=%s ORDER BY request_date DESC
                        LIMIT 5 OFFSET %s""", (q.from_user.id, offset))
            withdrawals = c.fetchall()

            c.execute("SELECT COUNT(*) FROM withdrawals WHERE user_id=%s", (q.from_user.id,))
            total = list(c.fetchone().values())[0]

        text = f"💸 <b>Withdrawal History</b> — Page {page + 1}\n\n"
        if withdrawals:
            for w in withdrawals:
                emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}.get(w['status'], "❓")
                method_emoji = "📱" if w['method'] == 'upi' else "💎"
                fee = float(w['fee']) if w['fee'] is not None else 0
                final_amount = float(w['final_amount']) if w['final_amount'] is not None else float(w['amount'])

                text += f"{emoji} {method_emoji} <b>₹{float(w['amount']):.2f}</b>\n"
                text += f"    Fee: ₹{fee:.2f} | Final: ₹{final_amount:.2f}\n"
                text += f"    {w['status'].title()} — {w['request_date'][:10]}\n"
                if w['rejection_reason']:
                    text += f"    <i>Reason: {w['rejection_reason']}</i>\n"
                text += "\n"
        else:
            text += "<i>No withdrawals yet.</i>"

        kb = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"history_withdrawal_{page - 1}"))
        if offset + 5 < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_withdrawal_{page + 1}"))
        if nav:
            kb.append(nav)
        kb.append([InlineKeyboardButton("📧 Gmail History", callback_data="history_gmail_0")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu")])
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── PROFILE ──
    elif d == "profile":
        text, kb = build_profile_content(q.from_user.id)
        await safe_edit_or_reply(q, text, kb)

    # ── TASKS PAUSED NOTICE ──
    elif d == "tasks_paused":
        await safe_edit_or_reply(
            q,
            "🚫 <b>Task Submission Paused</b>\n\n"
            "Task submissions are currently paused by the admin.\n"
            "Please check back later.\n\n"
            "💡 <i>You'll be able to get tasks once submissions resume.</i>",
            InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]),
        )

    # ── SETTINGS ──
    elif d == "settings":
        text, kb = build_settings_content(q.from_user.id)
        await safe_edit_or_reply(q, text, kb)

    # ── TOGGLE NOTIFICATIONS ──
    elif d == "toggle_notif":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET notifications_enabled = 1 - notifications_enabled WHERE user_id=%s",
                      (q.from_user.id,))
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (q.from_user.id,))
            new_state = list(c.fetchone().values())[0]

        await q.answer(f"{'🔔 Notifications enabled' if new_state else '🔕 Notifications disabled'}", show_alert=True)
        q.data = "settings"
        await user_callback(update, context)

    # ── VIEW TERMS ──
    elif d == "view_terms":
        text = (
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
        kb = [[InlineKeyboardButton("🔙 Back", callback_data="settings")]]
        await safe_edit_or_reply(q, text, InlineKeyboardMarkup(kb))

    # ── HELP ──
    elif d == "help":
        text, kb = build_help_content()
        await safe_edit_or_reply(q, text, kb)
