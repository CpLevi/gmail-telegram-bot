# ==================== PART 1: IMPORTS AND DATABASE SETUP ====================
# SECURE TASK EARNING BOT - PRODUCTION READY v5.0 - PostgreSQL Compatible

import telegram
print("PTB VERSION:", telegram.__version__)

import sys
print("PYTHON VERSION:", sys.version)

import os
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import logging
from datetime import datetime, timedelta
from contextlib import contextmanager
from decimal import Decimal, ROUND_HALF_UP
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@EarnXOfficiial")
SUPPORT_USERNAME = "Mr_Carry07"

# ==================== CONSTANTS ====================
ALLOWED_DOMAINS = ["gmail.com"]

WITHDRAWAL_FEE_PERCENT = Decimal("5")
WITHDRAWAL_FEE_MIN = Decimal("5")

MAX_WITHDRAWALS_PER_DAY = 3
MAX_PENDING_WITHDRAWALS = 2

SUBMIT_COOLDOWN = 20  # seconds
MAX_PAGINATION_PAGE = 50

EMAIL, PASSWORD, USDT_ADDRESS, UPI_ID, WITHDRAW_AMT, BROADCAST_MSG, USER_SEARCH = range(7)

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        c = conn.cursor()
        
        # Users table
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            balance DECIMAL(10,2) DEFAULT 0,
            total_gmail INTEGER DEFAULT 0,
            approved_gmail INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            referrer_id BIGINT,
            usdt_address TEXT,
            upi_id TEXT,
            joined_date TEXT,
            channel_claimed INTEGER DEFAULT 0,
            last_submit_time TEXT,
            terms_accepted INTEGER DEFAULT 1,
            notifications_enabled INTEGER DEFAULT 1
        )''')
        
        # Gmail submissions table
        c.execute('''CREATE TABLE IF NOT EXISTS gmail (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            email TEXT,
            password TEXT,
            status TEXT DEFAULT 'pending',
            reward DECIMAL(10,2),
            submit_date TEXT,
            review_date TEXT,
            rejection_reason TEXT,
            UNIQUE(email)
        )''')
        
        # Withdrawals table
        c.execute('''CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount DECIMAL(10,2),
            fee DECIMAL(10,2) DEFAULT 0,
            final_amount DECIMAL(10,2),
            method TEXT,
            payment_info TEXT,
            status TEXT DEFAULT 'pending',
            request_date TEXT,
            processed_date TEXT,
            rejection_reason TEXT
        )''')
        
        # Referrals table - IDEMPOTENT
        c.execute('''CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT,
            referred_id BIGINT,
            reward DECIMAL(10,2) DEFAULT 5,
            date TEXT,
            rewarded INTEGER DEFAULT 0,
            UNIQUE(referred_id)
        )''')
        
        # Audit log table
        c.execute('''CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            action TEXT,
            admin_id BIGINT,
            target_user_id BIGINT,
            details TEXT,
            timestamp TEXT
        )''')
        
        # Add missing columns
        columns_to_add = [
            ("users", "notifications_enabled", "INTEGER DEFAULT 1"),
            ("users", "last_submit_time", "TEXT"),
            ("gmail", "review_date", "TEXT"),
            ("gmail", "rejection_reason", "TEXT"),
            ("withdrawals", "processed_date", "TEXT"),
            ("withdrawals", "rejection_reason", "TEXT"),
            ("withdrawals", "fee", "DECIMAL(10,2) DEFAULT 0"),
            ("withdrawals", "final_amount", "DECIMAL(10,2)"),
            ("referrals", "rewarded", "INTEGER DEFAULT 0")
        ]
        
        for table, column, definition in columns_to_add:
            try:
                c.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except psycopg2.Error:
                logger.info(f"Adding {column} column to {table} table")
                c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                conn.commit()
        
        # Create indexes for performance
        indexes = [
            ("idx_gmail_user_status", "gmail", "user_id, status"),
            ("idx_gmail_status", "gmail", "status"),
            ("idx_gmail_email", "gmail", "email"),
            ("idx_withdrawals_user_status", "withdrawals", "user_id, status"),
            ("idx_withdrawals_status", "withdrawals", "status"),
            ("idx_withdrawals_date", "withdrawals", "request_date"),
            ("idx_referrals_referrer", "referrals", "referrer_id"),
            ("idx_referrals_rewarded", "referrals", "rewarded"),
            ("idx_users_blocked", "users", "is_blocked")
        ]
        
        for idx_name, table, columns in indexes:
            try:
                c.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")
            except Exception as e:
                logger.error(f"Error creating index {idx_name}: {e}")
        
        conn.commit()
        logger.info("✅ Database initialized successfully")
# ==================== PART 2: VALIDATION AND HELPER FUNCTIONS ====================

def round_decimal(value):
    """Round to 2 decimal places properly"""
    return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def normalize_email(email):
    """Normalize email for duplicate detection"""
    if not email:
        return email
    email = email.lower().strip()
    local, domain = email.split('@', 1)
    # Remove dots and plus aliases from Gmail local part
    if domain == 'gmail.com':
        local = local.replace('.', '')
        if '+' in local:
            local = local.split('+')[0]
    return f"{local}@{domain}"

def validate_email(email):
    """Validate email and check domain"""
    if not email or len(email) > 100:
        return False, "Email too long"
    
    email = email.lower().strip()
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "Invalid email format"
    
    domain = email.split('@')[-1].lower()
    if domain not in ALLOWED_DOMAINS:
        return False, f"Only {', '.join(ALLOWED_DOMAINS)} allowed"
    
    return True, email

def validate_password(password):
    return password and 6 <= len(password) <= 100

def validate_upi(upi_id):
    if not upi_id or len(upi_id) > 50:
        return False
    pattern = r'^[\w.-]+@[\w]+$'
    return bool(re.match(pattern, upi_id))

def validate_usdt_address(address):
    if not address or len(address) != 34:
        return False
    return address.startswith('T')

def mask_email(email):
    """Mask email for privacy"""
    if not email or '@' not in email:
        return email
    
    local, domain = email.split('@', 1)
    
    if len(local) <= 2:
        masked_local = local[0] + '****'
    else:
        masked_local = local[:2] + '****'
    
    return f"{masked_local}@{domain}"

def validate_page(page_str):
    """Validate pagination to prevent abuse"""
    try:
        page = int(page_str)
        if 0 <= page <= MAX_PAGINATION_PAGE:
            return page
        return 0
    except:
        return 0

def calculate_withdrawal_fee(amount):
    """Calculate withdrawal fee with proper decimal precision"""
    amount = round_decimal(amount)
    fee_percent = amount * (WITHDRAWAL_FEE_PERCENT / Decimal("100"))
    fee = max(fee_percent, WITHDRAWAL_FEE_MIN)
    fee = round_decimal(fee)
    final_amount = round_decimal(amount - fee)
    return fee, final_amount

def can_submit_gmail(user_id):
    """Check cooldown for Gmail submission"""
    with get_db() as conn:
        c = conn.cursor()
        try:
            c.execute("SELECT last_submit_time FROM users WHERE user_id=%s", (user_id,))
        except psycopg2.Error:
            return True, 0
        
        result = c.fetchone()
        
        if not result or not result['last_submit_time']:
            return True, 0
        
        last_time = datetime.fromisoformat(result['last_submit_time'])
        time_passed = (datetime.now() - last_time).total_seconds()
        
        if time_passed < SUBMIT_COOLDOWN:
            return False, int(SUBMIT_COOLDOWN - time_passed)
        return True, 0

def update_submit_time(user_id):
    """Update last submit time"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET last_submit_time=%s WHERE user_id=%s", 
                 (datetime.now().isoformat(), user_id))

def can_withdraw_today(user_id):
    """Check if user can withdraw today"""
    with get_db() as conn:
        c = conn.cursor()
        today = datetime.now().date().isoformat()
        c.execute("""SELECT COUNT(*) FROM withdrawals 
                    WHERE user_id=%s AND request_date::date=%s AND status IN ('pending', 'approved')""",
                 (user_id, today))
        count = list(c.fetchone().values())[0]
        return count < MAX_WITHDRAWALS_PER_DAY, MAX_WITHDRAWALS_PER_DAY - count

def check_duplicate_email(email):
    """Check if email exists (normalized)"""
    normalized = normalize_email(email)
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, status FROM gmail WHERE LOWER(TRIM(email))=%s LIMIT 1", (normalized,))
        result = c.fetchone()
        return result

def log_audit(action, admin_id, target_user_id=None, details=""):
    """Audit logging function"""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO audit_log (action, admin_id, target_user_id, details, timestamp)
                        VALUES (%s, %s, %s, %s, %s)""",
                     (action, admin_id, target_user_id, details, datetime.now().isoformat()))
    except Exception as e:
        logger.error(f"Audit log error: {e}")

async def check_channel(user_id, context):
    """Check channel membership with error handling"""
    try:
        channel = TELEGRAM_CHANNEL.lstrip('@')
        if not channel.startswith('@'):
            channel = '@' + channel
        
        member = await context.bot.get_chat_member(channel, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Channel check error for {user_id}: {e}")
        return False

def calc_rate(user_id):
    """Calculate reward rate based on approved Gmail"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT approved_gmail FROM users WHERE user_id=%s", (user_id,))
        result = c.fetchone()
        approved = result['approved_gmail'] if result else 0
        
    if approved >= 100:
        return Decimal("30")
    elif approved >= 50:
        return Decimal("25")
    return Decimal("20")

def is_blocked(user_id):
    """Check if user is blocked"""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT is_blocked FROM users WHERE user_id=%s", (user_id,))
        result = c.fetchone()
        return result['is_blocked'] == 1 if result else False

def notifications_enabled(user_id):
    """Check if user has notifications enabled"""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (user_id,))
            result = c.fetchone()
            return result['notifications_enabled'] == 1 if result else True
    except Exception as e:
        logger.error(f"notifications_enabled error: {e}")
        return True

async def notify_user(context, user_id, message):
    """Send notification to user with error handling"""
    try:
        if not notifications_enabled(user_id):
            logger.info(f"Notifications disabled for user {user_id}")
            return False
        
        await context.bot.send_message(user_id, message, parse_mode=None)
        logger.info(f"✅ Notification sent to user {user_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to notify user {user_id}: {e}")
        return False

def get_earnings_stats(user_id, period='all'):
    """Get earnings statistics for different time periods"""
    with get_db() as conn:
        c = conn.cursor()
        
        now = datetime.now()
        
        if period == 'today':
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        elif period == 'week':
            start_date = (now - timedelta(days=7)).isoformat()
        elif period == 'month':
            start_date = (now - timedelta(days=30)).isoformat()
        else:
            start_date = '2000-01-01'
        
        c.execute("""SELECT COALESCE(SUM(reward), 0) FROM gmail 
                    WHERE user_id=%s AND status='approved' AND review_date >= %s""",
                 (user_id, start_date))
        gmail_earnings = float(c.fetchone().values().__iter__().__next__())
        
        c.execute("""SELECT COALESCE(SUM(reward), 0) FROM referrals 
                    WHERE referrer_id=%s AND rewarded=1 AND date >= %s""",
                 (user_id, start_date))
        referral_earnings = float(c.fetchone().values().__iter__().__next__())
        
        if period == 'all':
            c.execute("SELECT channel_claimed FROM users WHERE user_id=%s", (user_id,))
            result = c.fetchone()
            channel_bonus = 1 if result and result['channel_claimed'] else 0
        else:
            channel_bonus = 0
        
        return {
            'gmail': gmail_earnings,
            'referral': referral_earnings,
            'channel': channel_bonus,
            'total': gmail_earnings + referral_earnings + channel_bonus
        }
# ==================== PART 3: START COMMAND AND BASIC CALLBACKS ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if update.message:
        message_to_use = update.message
    else:
        return
    
    if is_blocked(user.id):
        await message_to_use.reply_text("⛔ You are blocked from using this bot.")
        return
    
    # Handle referral with self-referral protection
    ref_id = None
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id == user.id:
                ref_id = None  # PREVENT SELF-REFERRAL
        except:
            pass
    
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE user_id=%s", (user.id,))
        existing = c.fetchone()
        
        if not existing:
            c.execute("""INSERT INTO users (user_id, username, first_name, referrer_id, joined_date)
                         VALUES (%s, %s, %s, %s, %s)""",
                      (user.id, user.username, user.first_name, ref_id, datetime.now().isoformat()))
            
            # Register referral but DON'T reward yet (rewarded after first approval)
            if ref_id and ref_id != user.id:
                c.execute("SELECT user_id FROM users WHERE user_id=%s", (ref_id,))
                if c.fetchone():
                    try:
                        c.execute("INSERT INTO referrals (referrer_id, referred_id, reward, date, rewarded) VALUES (%s,%s,%s,%s,%s)",
                                 (ref_id, user.id, 5, datetime.now().isoformat(), 0))
                        await notify_user(context, ref_id, 
                            f"🎉 {user.first_name} joined via your link!\n\n"
                            f"You'll earn ₹5 when they complete their first approved Gmail submission.")
                    except psycopg2.IntegrityError:
                        pass
    
    kb = [
        [InlineKeyboardButton("📧 Submit Gmail", callback_data="submit")],
        [InlineKeyboardButton("💰 Balance", callback_data="balance"),
         InlineKeyboardButton("📋 History", callback_data="history")],
        [InlineKeyboardButton("💸 Withdraw", callback_data="withdraw"),
         InlineKeyboardButton("👤 Profile", callback_data="profile")],
        [InlineKeyboardButton("👥 Refer Friends", callback_data="referral")],
        [InlineKeyboardButton("📊 Earnings", callback_data="earnings")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
         InlineKeyboardButton("❓ Help", callback_data="help")]
    ]
    
    if user.id == ADMIN_ID:
        kb.append([InlineKeyboardButton("⚙️ ADMIN", callback_data="admin")])
    
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT channel_claimed FROM users WHERE user_id=%s", (user.id,))
        result = c.fetchone()
        claimed = result['channel_claimed'] if result else 0
    
    text = f"""🎉 **Welcome {user.first_name}!**

💼 **Gmail Rates:**
-  0-49: ₹20/account
-  50-99: ₹25/account
-  100+: ₹30/account

🎁 **Bonuses:**
-  Channel: ₹1 (one-time)
-  Referral: ₹5/friend (after 1st approval)

💸 **Withdrawal Fee:** {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})

📢 Join: {TELEGRAM_CHANNEL}"""
    
    if not claimed:
        text += "\n\n⚡ **Join = ₹1 FREE!**"
        channel_url = f"https://t.me/{TELEGRAM_CHANNEL.lstrip('@')}"
        kb.insert(0, [InlineKeyboardButton("📢 Join Channel", url=channel_url)])
        kb.insert(1, [InlineKeyboardButton("🎁 Claim ₹1", callback_data="claim_channel")])
    
    await message_to_use.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    if is_blocked(q.from_user.id) and q.from_user.id != ADMIN_ID:
        await q.answer("⛔ Blocked!", show_alert=True)
        return
    
    d = q.data
    
    # CHANNEL CLAIM - IDEMPOTENT
    if d == "claim_channel":
        await q.answer("Checking membership...", show_alert=False)
        
        if await check_channel(q.from_user.id, context):
            with get_db() as conn:
                c = conn.cursor()
                # ATOMIC UPDATE - Only claim if not already claimed
                c.execute("""
                    UPDATE users 
                    SET balance=balance+1, channel_claimed=1 
                    WHERE user_id=%s AND channel_claimed=0
                    RETURNING user_id
                """, (q.from_user.id,))
                
                result = c.fetchone()
                
                if result:
                    conn.commit()
                    await q.answer("✅ ₹1 added!", show_alert=True)
                    await q.message.reply_text("🎉 **₹1 credited!**\n\nThank you for joining!")
                else:
                    await q.answer("❌ Already claimed!", show_alert=True)
        else:
            await q.answer(f"❌ Join {TELEGRAM_CHANNEL} first!", show_alert=True)
        return
    
    # MENU
    if d == "menu":
        kb = [
            [InlineKeyboardButton("📧 Submit", callback_data="submit")],
            [InlineKeyboardButton("💰 Balance", callback_data="balance"),
             InlineKeyboardButton("📋 History", callback_data="history")],
            [InlineKeyboardButton("💸 Withdraw", callback_data="withdraw"),
             InlineKeyboardButton("👤 Profile", callback_data="profile")],
            [InlineKeyboardButton("👥 Refer Friends", callback_data="referral")],
            [InlineKeyboardButton("📊 Earnings", callback_data="earnings")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
             InlineKeyboardButton("❓ Help", callback_data="help")]
        ]
        if q.from_user.id == ADMIN_ID:
            kb.append([InlineKeyboardButton("⚙️ ADMIN", callback_data="admin")])
        await q.edit_message_text("📱 Main Menu", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END
    
    # SUBMIT GMAIL - WITH COOLDOWN
    elif d == "submit":
        can_submit, wait_time = can_submit_gmail(q.from_user.id)
        
        if not can_submit:
            await q.answer(f"⏳ Wait {wait_time}s before submitting again!", show_alert=True)
            
            temp_msg = await q.message.reply_text(
                f"⏳ **Cooldown Active**\n\n"
                f"Please wait **{wait_time} seconds** before submitting another Gmail.\n\n"
                f"This prevents spam and helps us process your submissions better.",
                parse_mode=None
            )
            
            await asyncio.sleep(5)
            try:
                await temp_msg.delete()
            except:
                pass
            
            return
        
        await q.edit_message_text(
            "📧 **Submit Gmail**\n\n"
            f"Send the email address:\n\n"
            f"✅ Allowed: {', '.join(ALLOWED_DOMAINS)}\n"
            f"⚠️ Only YOUR OWN accounts!\n"
            "/cancel to abort",
            parse_mode=None
        )
        return EMAIL
    
    # BALANCE
    elif d == "balance":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT balance, total_gmail, approved_gmail FROM users WHERE user_id=%s", 
                     (q.from_user.id,))
            result = c.fetchone()
            
            c.execute("SELECT COALESCE(SUM(reward), 0) FROM gmail WHERE user_id=%s AND status='pending'", 
                     (q.from_user.id,))
            pending = float(c.fetchone().values().__iter__().__next__() or 0)
        
        bal, total, approved = (float(result['balance']), result['total_gmail'], result['approved_gmail']) if result else (0,0,0)
        rate = float(calc_rate(q.from_user.id))
        
        text = f"""💰 **Balance: ₹{bal:.2f}**

**Rate:** ₹{rate}/account
⏳ **Pending:** ₹{pending:.2f}

📊 **Stats:**
✅ Approved: {approved}
📧 Total: {total}"""
        
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙", callback_data="menu")]
        ]), parse_mode=None)
# ==================== PART 4: EARNINGS, REFERRAL AND HISTORY CALLBACKS ====================

    # EARNINGS DASHBOARD (continuation of callback function)
    elif d == "earnings" or d.startswith("earnings_"):
        period = d.split("_")[1] if "_" in d else "all"
        
        stats = get_earnings_stats(q.from_user.id, period)
        
        period_names = {
            'today': 'Today',
            'week': 'This Week',
            'month': 'This Month',
            'all': 'All Time'
        }
        
        text = f"""📊 **Earnings Dashboard**

**Period:** {period_names.get(period, 'All Time')}

📧 **Gmail:** ₹{stats['gmail']:.2f}
👥 **Referrals:** ₹{stats['referral']:.2f}
📢 **Channel Bonus:** ₹{stats['channel']:.2f}
━━━━━━━━━━━━━━━━
💰 **Total:** ₹{stats['total']:.2f}"""
        
        kb = [
            [InlineKeyboardButton("📅 Today", callback_data="earnings_today"),
             InlineKeyboardButton("📅 Week", callback_data="earnings_week")],
            [InlineKeyboardButton("📅 Month", callback_data="earnings_month"),
             InlineKeyboardButton("📅 All Time", callback_data="earnings_all")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu")]
        ]
        
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # REFERRAL
    elif d == "referral":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s", (q.from_user.id,))
            ref_count = list(c.fetchone().values())[0]
            
            c.execute("SELECT COALESCE(SUM(reward), 0) FROM referrals WHERE referrer_id=%s AND rewarded=1", (q.from_user.id,))
            total_earned = float(c.fetchone().values().__iter__().__next__() or 0)
            
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=0", (q.from_user.id,))
            pending_refs = c.fetchone().values().__iter__().__next__()
        
        bot_user = context.bot.username
        ref_link = f"https://t.me/{bot_user}?start={q.from_user.id}"
        
        text = f"""👥 **Refer & Earn**

💰 **Earn ₹5 per referral!**
*Reward credited after their 1st approved Gmail*

📊 **Your Stats:**
- Total Referrals: {ref_count}
- Pending Rewards: {pending_refs}
- Total Earned: ₹{total_earned:.2f}

🔗 **Your Referral Link:**
`{ref_link}`

📱 **Share this link with friends!**
When they join and get their first Gmail approved, you get ₹5 instantly.

💡 **Tip:** Share on WhatsApp, Facebook, or other social media to maximize your earnings!"""
        
        kb = [
            [InlineKeyboardButton("🏆 Leaderboard", callback_data="referral_leaderboard")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu")]
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # REFERRAL LEADERBOARD
    elif d == "referral_leaderboard":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT u.first_name, u.username, u.user_id, COUNT(r.id) as ref_count
                        FROM users u
                        JOIN referrals r ON u.user_id = r.referrer_id
                        WHERE r.rewarded = 1
                        GROUP BY u.user_id, u.first_name, u.username
                        ORDER BY ref_count DESC
                        LIMIT 10""")
            top_referrers = c.fetchall()
            
            c.execute("""SELECT COUNT(DISTINCT referrer_id) + 1 as rank
                        FROM referrals
                        WHERE rewarded = 1 AND referrer_id IN (
                            SELECT referrer_id FROM referrals
                            WHERE rewarded = 1
                            GROUP BY referrer_id
                            HAVING COUNT(*) > (
                                SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1
                            )
                        )""", (q.from_user.id,))
            result = c.fetchone()
            user_rank = result[0] if result else "N/A"
            
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1", (q.from_user.id,))
            user_refs = c.fetchone().values().__iter__().__next__()
        
        text = "🏆 **Referral Leaderboard**\n\n"
        
        if top_referrers:
            medals = ["🥇", "🥈", "🥉"]
            for idx, row in enumerate(top_referrers, 1):
                medal = medals[idx-1] if idx <= 3 else f"{idx}."
                name = row['first_name']
                refs = row['ref_count']
                text += f"{medal} **{name}** - {refs} referrals\n"
        else:
            text += "No referrals yet. Be the first!\n"
        
        text += f"\n🔍 **Your Rank:** #{user_rank}\n"
        text += f"👥 **Your Referrals:** {user_refs}"
        
        kb = [
            [InlineKeyboardButton("🔙 Back", callback_data="referral")]
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)

    # HISTORY - Gmail submissions
    elif d == "history" or d.startswith("history_gmail_"):
        page = validate_page(d.split("_")[-1]) if "_" in d else 0
        offset = page * 5
        
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT email, status, reward, submit_date, rejection_reason 
                        FROM gmail WHERE user_id=%s ORDER BY submit_date DESC 
                        LIMIT 5 OFFSET %s""", (q.from_user.id, offset))
            subs = c.fetchall()
            
            c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s", (q.from_user.id,))
            total = c.fetchone().values().__iter__().__next__()
        
        text = f"📋 **Gmail History** (Page {page+1})\n\n"
        if subs:
            for sub in subs:
                emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}[sub['status']]
                reward_val = float(sub['reward']) if sub['reward'] else 0
                text += f"{emoji} {mask_email(sub['email'])}\n   {sub['status'].title()} - ₹{reward_val}"
                if sub['rejection_reason']:
                    text += f"\n   ⚠️ {sub['rejection_reason']}"
                text += "\n\n"
        else:
            text += "No submissions yet."
        
        kb = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"history_gmail_{page-1}"))
        if offset + 5 < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_gmail_{page+1}"))
        if nav:
            kb.append(nav)
        
        kb.append([InlineKeyboardButton("💸 Withdrawal History", callback_data="history_withdrawal_0")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu")])
        
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # WITHDRAWAL HISTORY
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
            total = c.fetchone().values().__iter__().__next__()
        
        text = f"💸 **Withdrawal History** (Page {page+1})\n\n"
        if withdrawals:
            for w in withdrawals:
                emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}[w['status']]
                method_emoji = "📱" if w['method'] == 'upi' else "💎"
                
                fee = float(w['fee']) if w['fee'] is not None else 0
                final_amount = float(w['final_amount']) if w['final_amount'] is not None else float(w['amount'])
                
                text += f"{emoji} {method_emoji} ₹{float(w['amount']):.2f}\n"
                text += f"   Fee: ₹{fee:.2f} | Final: ₹{final_amount:.2f}\n"
                text += f"   {w['status'].title()} - {w['request_date'][:10]}\n"
                if w['rejection_reason']:
                    text += f"   ⚠️ {w['rejection_reason']}\n"
                text += "\n"
        else:
            text += "No withdrawals yet."
        
        kb = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"history_withdrawal_{page-1}"))
        if offset + 5 < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"history_withdrawal_{page+1}"))
        if nav:
            kb.append(nav)
        
        kb.append([InlineKeyboardButton("📧 Gmail History", callback_data="history_gmail_0")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu")])
        
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
# ==================== PART 5: WITHDRAWAL AND PROFILE CALLBACKS ====================

    # WITHDRAW - ATOMIC BALANCE CHECK
    elif d == "withdraw":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT balance, usdt_address, upi_id FROM users WHERE user_id=%s", 
                     (q.from_user.id,))
            result = c.fetchone()
            
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE user_id=%s AND status='pending'", 
                     (q.from_user.id,))
            pending_count = list(c.fetchone().values())[0]
        
        can_withdraw, remaining = can_withdraw_today(q.from_user.id)
        
        if result:
            bal, usdt, upi = float(result['balance']), result['usdt_address'], result['upi_id']
            
            if not can_withdraw:
                text = f"💸 **Withdraw**\n\n**Balance:** ₹{bal:.2f}\n\n❌ Daily limit reached!\nYou can make {MAX_WITHDRAWALS_PER_DAY} withdrawals per day.\n\nTry again tomorrow."
                kb = [[InlineKeyboardButton("🔙", callback_data="menu")]]
            elif pending_count >= MAX_PENDING_WITHDRAWALS:
                text = f"💸 **Withdraw**\n\n**Balance:** ₹{bal:.2f}\n\n❌ You have {pending_count} pending requests.\nWait for processing."
                kb = [[InlineKeyboardButton("🔙", callback_data="menu")]]
            elif bal < 100:
                text = f"💸 **Withdraw**\n\n**Balance:** ₹{bal:.2f}\n\n❌ Minimum: ₹100"
                kb = [[InlineKeyboardButton("🔙", callback_data="menu")]]
            else:
                example_fee, example_final = calculate_withdrawal_fee(Decimal("100"))
                text = f"💸 **Withdraw**\n\n**Balance:** ₹{bal:.2f}\n**Min:** ₹100\n**Today:** {remaining}/{MAX_WITHDRAWALS_PER_DAY} left\n\n**Fee:** {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})\n*Example: ₹100 → Fee ₹{float(example_fee):.2f} → You get ₹{float(example_final):.2f}*\n\nChoose method:"
                kb = [
                    [InlineKeyboardButton("📱 UPI" + (" ✅" if upi else ""), callback_data="withdraw_upi")],
                    [InlineKeyboardButton("💎 USDT" + (" ✅" if usdt else ""), callback_data="withdraw_usdt")],
                    [InlineKeyboardButton("⚙️ Setup", callback_data="setup_payment")],
                    [InlineKeyboardButton("🔙", callback_data="menu")]
                ]
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        else:
            await q.edit_message_text("❌ Error!", 
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="menu")]]))
    
    # WITHDRAW UPI
    elif d == "withdraw_upi":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT upi_id FROM users WHERE user_id=%s", (q.from_user.id,))
            result = c.fetchone()
        
        if not result or not result['upi_id']:
            await q.answer("❌ Setup UPI first!", show_alert=True)
            return
        
        context.user_data['withdraw_method'] = 'upi'
        await q.edit_message_text(
            "💸 **Withdraw via UPI**\n\nEnter amount (Min: ₹100):\n\n/cancel to abort",
            parse_mode=None
        )
        return WITHDRAW_AMT
    
    # WITHDRAW USDT
    elif d == "withdraw_usdt":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT usdt_address FROM users WHERE user_id=%s", (q.from_user.id,))
            result = c.fetchone()
        
        if not result or not result['usdt_address']:
            await q.answer("❌ Setup USDT first!", show_alert=True)
            return
        
        context.user_data['withdraw_method'] = 'usdt'
        await q.edit_message_text(
            "💸 **Withdraw via USDT**\n\nEnter amount (Min: ₹100):\n\n/cancel to abort",
            parse_mode=None
        )
        return WITHDRAW_AMT
    
    # SETUP PAYMENT
    elif d == "setup_payment":
        kb = [
            [InlineKeyboardButton("📱 UPI", callback_data="set_upi")],
            [InlineKeyboardButton("💎 USDT", callback_data="set_usdt")],
            [InlineKeyboardButton("🔙", callback_data="withdraw")]
        ]
        await q.edit_message_text("⚙️ **Setup Payment**\n\nChoose:", 
                                  reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    elif d == "set_upi":
        await q.edit_message_text("📱 **Setup UPI**\n\nSend UPI ID:\n/cancel to abort", 
                                  parse_mode=None)
        return UPI_ID
    
    elif d == "set_usdt":
        await q.edit_message_text("💎 **Setup USDT**\n\nSend TRC20 address:\n/cancel to abort", 
                                  parse_mode=None)
        return USDT_ADDRESS
    
    # PROFILE
    elif d == "profile":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT balance, approved_gmail, usdt_address, upi_id, joined_date FROM users WHERE user_id=%s", 
                     (q.from_user.id,))
            result = c.fetchone()
            
            c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=%s AND rewarded=1", (q.from_user.id,))
            ref_count = list(c.fetchone().values())[0]
        
        if result:
            bal, approved, usdt, upi, joined = float(result['balance']), result['approved_gmail'], result['usdt_address'], result['upi_id'], result['joined_date']
            rate = float(calc_rate(q.from_user.id))
            
            text = f"""👤 **Profile**

**Balance:** ₹{bal:.2f}
**Rate:** ₹{rate}/account
**Approved:** {approved}
**Referrals:** {ref_count}

💳 **Payment:**
-  UPI: {"✅" if upi else "❌"}
-  USDT: {"✅" if usdt else "❌"}

📅 **Joined:** {joined[:10]}"""
            
            kb = [
                [InlineKeyboardButton("⚙️ Payment", callback_data="setup_payment")],
                [InlineKeyboardButton("🔙", callback_data="menu")]
            ]
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # SETTINGS
    elif d == "settings":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (q.from_user.id,))
            result = c.fetchone()
            notif = result['notifications_enabled'] if result else 1
        
        text = f"""⚙️ **Settings**

**Notifications:** {"🔔 ON" if notif else "🔕 OFF"}

📞 **Support:** @{SUPPORT_USERNAME}
📜 **Terms:** Click below"""
        
        kb = [
            [InlineKeyboardButton("🔕 OFF" if notif else "🔔 ON", callback_data="toggle_notif")],
            [InlineKeyboardButton("📜 Terms", callback_data="view_terms")],
            [InlineKeyboardButton("📞 Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
            [InlineKeyboardButton("🔙", callback_data="menu")]
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # TOGGLE NOTIFICATIONS
    elif d == "toggle_notif":
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET notifications_enabled = 1 - notifications_enabled WHERE user_id=%s", 
                     (q.from_user.id,))
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (q.from_user.id,))
            new_state = c.fetchone().values().__iter__().__next__()
        
        await q.answer(f"{'🔔 Enabled' if new_state else '🔕 Disabled'}!", show_alert=True)
        q.data = "settings"
        await callback(update, context)
    
    # VIEW TERMS
    elif d == "view_terms":
        text = f"""📜 **Terms & Conditions**

1️⃣ Submit only YOUR accounts
2️⃣ No fake/stolen accounts
3️⃣ Min withdrawal: ₹100
4️⃣ Max {MAX_WITHDRAWALS_PER_DAY} withdrawals/day
5️⃣ Withdrawal fee: {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})
6️⃣ Processing: 24-48h
7️⃣ Only {', '.join(ALLOWED_DOMAINS)} allowed
8️⃣ Referral rewards after 1st approval
9️⃣ Suspicious activity = Ban

**Support:** @{SUPPORT_USERNAME}"""
        
        kb = [[InlineKeyboardButton("🔙", callback_data="settings")]]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # HELP
    elif d == "help":
        text = f"""❓ **Help & Support**

**How it works:**
1️⃣ Submit your Gmail accounts
2️⃣ Wait for approval (24-48h)
3️⃣ Earn based on your tier
4️⃣ Withdraw when you reach ₹100

**Earning Rates:**
-  0-49 accounts: ₹20 each
-  50-99 accounts: ₹25 each
-  100+ accounts: ₹30 each

**Bonuses:**
-  Channel join: ₹1
-  Referral: ₹5 per friend (after 1st approval)

**Withdrawal:**
-  Minimum: ₹100
-  Fee: {WITHDRAWAL_FEE_PERCENT}% (min ₹{WITHDRAWAL_FEE_MIN})
-  Limit: {MAX_WITHDRAWALS_PER_DAY} per day
-  Methods: UPI & USDT
-  Processing: 24-48 hours

**Allowed Emails:**
-  {', '.join(ALLOWED_DOMAINS)}

**Need Help?**
Contact our support team:
@{SUPPORT_USERNAME}"""
        
        kb = [
            [InlineKeyboardButton("📞 Contact Support", url=f"https://t.me/{SUPPORT_USERNAME}")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu")]
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
# ==================== PART 6: ADMIN PANEL AND GMAIL APPROVAL (IDEMPOTENT) ====================

    # ADMIN PANEL
    elif d == "admin" and q.from_user.id == ADMIN_ID:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            users = c.fetchone().values().__iter__().__next__()
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='pending'")
            pg = c.fetchone().values().__iter__().__next__()
            c.execute("SELECT COUNT(*) FROM withdrawals WHERE status='pending'")
            pw = c.fetchone().values().__iter__().__next__()
        
        text = f"""⚙️ **ADMIN**

👥 Users: {users}
📧 Pending Gmail: {pg}
💸 Pending Withdrawals: {pw}"""
        
        kb = [
            [InlineKeyboardButton("📧 Gmail Queue", callback_data="gmail_queue")],
            [InlineKeyboardButton("💸 Withdrawals", callback_data="withdrawal_queue")],
            [InlineKeyboardButton("👥 User Mgmt", callback_data="user_mgmt")],
            [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
            [InlineKeyboardButton("📊 Stats", callback_data="stats"),
             InlineKeyboardButton("🔙", callback_data="menu")]
        ]
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
    
    # GMAIL QUEUE
    elif d == "gmail_queue" and q.from_user.id == ADMIN_ID:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT DISTINCT u.user_id, u.first_name, u.username, COUNT(g.id) as cnt
                         FROM gmail g JOIN users u ON g.user_id = u.user_id
                         WHERE g.status='pending'
                         GROUP BY u.user_id, u.first_name, u.username ORDER BY cnt DESC LIMIT 10""")
            users_pending = c.fetchall()
        
        if users_pending:
            text = "📧 **Gmail Queue**\n\n"
            kb = []
            for row in users_pending:
                uid, name, username, cnt = row['user_id'], row['first_name'], row['username'], row['cnt']
                text += f"👤 {name} (@{username or 'N/A'}) - {cnt}\n"
                kb.append([InlineKeyboardButton(f"{name} ({cnt})", callback_data=f"user_gmail_{uid}")])
            kb.append([InlineKeyboardButton("🔙", callback_data="admin")])
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        else:
            await q.edit_message_text("❌ No pending Gmail!",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="admin")]]))
    
    # Individual Gmail Review
    elif d.startswith("user_gmail_"):
        uid = int(d.split("_")[2])
        
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT id, email, password, reward, submit_date, status
                        FROM gmail WHERE user_id=%s AND status='pending' 
                        ORDER BY submit_date""", (uid,))
            gmails = c.fetchall()
            
            c.execute("SELECT first_name, username FROM users WHERE user_id=%s", (uid,))
            user_info = c.fetchone()
        
        if gmails and user_info:
            name, username = user_info['first_name'], user_info['username']
            
            text = f"""📧 **Gmail Review - {name}**

👤 @{username or 'N/A'} (ID: `{uid}`)
📊 **Total Pending:** {len(gmails)}

━━━━━━━━━━━━━━━
"""
            
            for idx, gmail in enumerate(gmails, 1):
                gid, email, pwd, reward = gmail['id'], gmail['email'], gmail['password'], float(gmail['reward'])
                text += f"""
**{idx}. Gmail #{gid}**
📧 `{email}`
🔑 `{pwd}`
💰 ₹{reward}
━━━━━━━━━━━━━━━
"""
            
            kb = [
                [InlineKeyboardButton("✅ Approve All", callback_data=f"approve_all_{uid}"),
                 InlineKeyboardButton("❌ Reject All", callback_data=f"reject_all_{uid}")],
                [InlineKeyboardButton("🔙 Back", callback_data="gmail_queue")]
            ]
            
            for gmail in gmails[:5]:
                gid = gmail['id']
                kb.insert(-1, [
                    InlineKeyboardButton(f"✅ Approve #{gid}", callback_data=f"approve_{gid}"),
                    InlineKeyboardButton(f"❌ Reject #{gid}", callback_data=f"reject_{gid}")
                ])
            
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        else:
            await q.answer("❌ No pending Gmail!", show_alert=True)
            q.data = "gmail_queue"
            await callback(update, context)

    # APPROVE SINGLE GMAIL - IDEMPOTENT & ATOMIC
    elif d.startswith("approve_") and not d.startswith("approve_all_"):
        gid = int(d.split("_")[1])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                # ATOMIC UPDATE - Only approve if status is still 'pending'
                c.execute("""
                    UPDATE gmail 
                    SET status='approved', review_date=%s 
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, reward, email
                """, (datetime.now().isoformat(), gid))
                
                result = c.fetchone()
                
                if not result:
                    await q.answer("⚠️ Already processed!", show_alert=True)
                    return
                
                uid, reward, email = result['user_id'], round_decimal(result['reward']), result['email']
                
                # Check if this is first approval
                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='approved'", (uid,))
                approval_count = c.fetchone().values().__iter__().__next__()
                is_first_approval = (approval_count == 1)
                
                # Credit balance
                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+1 WHERE user_id=%s",
                         (reward, uid))
                
                # IDEMPOTENT REFERRAL REWARD - Only if first approval
                if is_first_approval:
                    c.execute("""
                        UPDATE referrals 
                        SET rewarded=1 
                        WHERE referred_id=%s AND rewarded=0
                        RETURNING referrer_id, reward
                    """, (uid,))
                    
                    ref_result = c.fetchone()
                    if ref_result:
                        referrer_id = ref_result['referrer_id']
                        ref_reward = round_decimal(ref_result['reward'])
                        
                        c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", 
                                 (ref_reward, referrer_id))
                        
                        c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
                        referred_name = c.fetchone()['first_name']
                        
                        await notify_user(context, referrer_id,
                            f"🎉 **Referral Reward!**\n\n"
                            f"{referred_name} completed their first approved Gmail!\n\n"
                            f"**You earned:** ₹{float(ref_reward):.2f}\n"
                            f"**Keep referring for more rewards!**")
                
                conn.commit()
                
                log_audit("approve_gmail", ADMIN_ID, uid, f"Gmail #{gid} - {email} - ₹{float(reward):.2f}")
                
                await notify_user(context, uid,
                    f"✅ **Gmail Verified!**\n\n"
                    f"**Gmail:** `{mask_email(email)}`\n"
                    f"**Amount Credited:** ₹{float(reward):.2f}\n\n"
                    f"Thank you for your submission!")
                
                await q.answer(f"✅ Approved! ₹{float(reward):.2f} credited", show_alert=True)
                
                q.data = f'user_gmail_{uid}'
                await callback(update, context)
        except Exception as e:
            logger.error(f"Error approving gmail {gid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
    
    # REJECT SINGLE GMAIL - IDEMPOTENT
    elif d.startswith("reject_") and not d.startswith("reject_all_"):
        gid = int(d.split("_")[1])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                # ATOMIC UPDATE - Only reject if status is still 'pending'
                c.execute("""
                    UPDATE gmail 
                    SET status='rejected', review_date=%s, rejection_reason=%s 
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, email
                """, (datetime.now().isoformat(), "Invalid/duplicate account", gid))
                
                result = c.fetchone()
                
                if not result:
                    await q.answer("⚠️ Already processed!", show_alert=True)
                    return
                
                uid, email = result['user_id'], result['email']
                conn.commit()
                
                log_audit("reject_gmail", ADMIN_ID, uid, f"Gmail #{gid} - {email}")
                
                await notify_user(context, uid,
                    f"❌ **Gmail Rejected**\n\n"
                    f"**Gmail:** `{mask_email(email)}`\n"
                    f"**Reason:** Invalid/duplicate account\n\n"
                    f"**No amount has been credited.**\n"
                    f"Please submit valid Gmail accounts only.")
                
                await q.answer("❌ Rejected", show_alert=True)
                
                q.data = f'user_gmail_{uid}'
                await callback(update, context)
        except Exception as e:
            logger.error(f"Error rejecting gmail {gid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
# ==================== PART 7: APPROVE/REJECT ALL & WITHDRAWAL QUEUE ====================

    # APPROVE ALL - IDEMPOTENT & ATOMIC
    elif d.startswith("approve_all_"):
        uid = int(d.split("_")[2])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                # Get all pending gmails
                c.execute("SELECT id, reward, email FROM gmail WHERE user_id=%s AND status='pending'", (uid,))
                gmails = c.fetchall()
                
                if not gmails:
                    await q.answer("❌ No pending Gmail found!", show_alert=True)
                    q.data = "gmail_queue"
                    await callback(update, context)
                    return
                
                # Check if this is first approval
                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='approved'", (uid,))
                is_first_approval = c.fetchone().values().__iter__().__next__() == 0
                
                total_reward = sum(round_decimal(row['reward']) for row in gmails)
                count = len(gmails)
                
                # ATOMIC BATCH UPDATE
                c.execute("""
                    UPDATE gmail 
                    SET status='approved', review_date=%s 
                    WHERE user_id=%s AND status='pending'
                """, (datetime.now().isoformat(), uid))
                
                c.execute("UPDATE users SET balance=balance+%s, approved_gmail=approved_gmail+%s WHERE user_id=%s",
                         (total_reward, count, uid))
                
                # IDEMPOTENT REFERRAL REWARD
                if is_first_approval:
                    c.execute("""
                        UPDATE referrals 
                        SET rewarded=1 
                        WHERE referred_id=%s AND rewarded=0
                        RETURNING referrer_id, reward
                    """, (uid,))
                    
                    ref_result = c.fetchone()
                    if ref_result:
                        referrer_id = ref_result['referrer_id']
                        ref_reward = round_decimal(ref_result['reward'])
                        
                        c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", 
                                 (ref_reward, referrer_id))
                        
                        c.execute("SELECT first_name FROM users WHERE user_id=%s", (uid,))
                        referred_name = c.fetchone()['first_name']
                        
                        await notify_user(context, referrer_id,
                            f"🎉 **Referral Reward!**\n\n"
                            f"{referred_name} completed their first approved Gmail!\n\n"
                            f"**You earned:** ₹{float(ref_reward):.2f}\n"
                            f"**Keep referring for more rewards!**")
                
                conn.commit()
                
                log_audit("approve_all_gmail", ADMIN_ID, uid, f"{count} gmails - ₹{float(total_reward):.2f}")
                
                email_list = "\n".join([f"• {mask_email(g['email'])}" for g in gmails[:5]])
                if len(gmails) > 5:
                    email_list += f"\n• ...and {len(gmails) - 5} more"
                
                await notify_user(context, uid,
                    f"✅ **All Gmail Verified!**\n\n"
                    f"**Total Verified:** {count} accounts\n"
                    f"**Amount Credited:** ₹{float(total_reward):.2f}\n\n"
                    f"**Verified Accounts:**\n{email_list}\n\n"
                    f"Your balance has been updated. Thank you!")
                
                await q.answer(f"✅ {count} approved! ₹{float(total_reward):.2f} credited", show_alert=True)
                
                await q.edit_message_text(
                    f"✅ **Batch Approved**\n\n"
                    f"**User ID:** `{uid}`\n"
                    f"**Gmail Approved:** {count}\n"
                    f"**Total Amount:** ₹{float(total_reward):.2f}\n\n"
                    f"User has been notified.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue")]]),
                    parse_mode=None
                )
        except Exception as e:
            logger.error(f"Error approving all gmails for user {uid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
    
    # REJECT ALL - ATOMIC
    elif d.startswith("reject_all_"):
        uid = int(d.split("_")[2])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                c.execute("SELECT COUNT(*) FROM gmail WHERE user_id=%s AND status='pending'", (uid,))
                count = list(c.fetchone().values())[0]
                
                if count == 0:
                    await q.answer("❌ No pending Gmail found!", show_alert=True)
                    q.data = "gmail_queue"
                    await callback(update, context)
                    return
                
                # ATOMIC BATCH UPDATE
                c.execute("""
                    UPDATE gmail 
                    SET status='rejected', review_date=%s, rejection_reason=%s 
                    WHERE user_id=%s AND status='pending'
                """, (datetime.now().isoformat(), "Quality issues", uid))
                
                conn.commit()
                
                log_audit("reject_all_gmail", ADMIN_ID, uid, f"{count} gmails rejected")
                
                await notify_user(context, uid,
                    f"❌ **Gmail Submissions Rejected**\n\n"
                    f"**Total Rejected:** {count} accounts\n"
                    f"**Reason:** Quality issues\n\n"
                    f"**No amount has been credited.**\n"
                    f"Please review submission guidelines and submit valid accounts.")
                
                await q.answer(f"❌ {count} rejected", show_alert=True)
                
                await q.edit_message_text(
                    f"❌ **Batch Rejected**\n\n"
                    f"**User ID:** `{uid}`\n"
                    f"**Gmail Rejected:** {count}\n"
                    f"**Reason:** Quality issues\n\n"
                    f"User has been notified.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Queue", callback_data="gmail_queue")]]),
                    parse_mode=None
                )
        except Exception as e:
            logger.error(f"Error rejecting all gmails for user {uid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
    
    # WITHDRAWAL QUEUE
    elif d == "withdrawal_queue" and q.from_user.id == ADMIN_ID:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT w.id, w.amount, w.fee, w.final_amount, w.method, w.payment_info, w.request_date,
                         u.first_name, u.username, u.user_id
                         FROM withdrawals w JOIN users u ON w.user_id = u.user_id
                         WHERE w.status='pending'
                         ORDER BY w.request_date LIMIT 1""")
            sub = c.fetchone()
        
        if sub:
            wid, amount, fee, final_amount, method, info, date = sub['id'], float(sub['amount']), float(sub['fee']), float(sub['final_amount']), sub['method'], sub['payment_info'], sub['request_date']
            name, username, uid = sub['first_name'], sub['username'], sub['user_id']
            
            text = f"""💸 **Withdrawal #{wid}**

👤 {name} (@{username or 'N/A'})
💰 **Amount:** ₹{amount:.2f}
💳 **Fee:** ₹{fee:.2f}
💵 **Final Amount:** ₹{final_amount:.2f}
💳 **Method:** {method.upper()}
📄 **Info:** `{info}`
📅 **Date:** {date[:16]}"""
            
            kb = [
                [InlineKeyboardButton("✅ Approve", callback_data=f"aw_{wid}"),
                 InlineKeyboardButton("❌ Reject", callback_data=f"rw_{wid}")],
                [InlineKeyboardButton("➡️ Next", callback_data="withdrawal_queue"),
                 InlineKeyboardButton("🔙", callback_data="admin")]
            ]
            await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        else:
            await q.edit_message_text("❌ No pending withdrawals!",
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="admin")]]))
    
    # APPROVE WITHDRAWAL - IDEMPOTENT
    elif d.startswith("aw_"):
        wid = int(d.split("_")[1])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                # ATOMIC UPDATE - Only approve if status is still 'pending'
                c.execute("""
                    UPDATE withdrawals 
                    SET status='approved', processed_date=%s 
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, amount, final_amount
                """, (datetime.now().isoformat(), wid))
                
                result = c.fetchone()
                
                if not result:
                    await q.answer("⚠️ Already processed!", show_alert=True)
                    return
                
                uid, amount, final_amount = result['user_id'], float(result['amount']), float(result['final_amount'])
                conn.commit()
                
                log_audit("approve_withdrawal", ADMIN_ID, uid, f"Withdrawal #{wid} - ₹{amount:.2f}")
                
                await notify_user(context, uid,
                    f"✅ **Withdrawal Approved!**\n\n"
                    f"**Withdrawal ID:** #{wid}\n"
                    f"**Amount:** ₹{amount:.2f}\n"
                    f"**Final Amount:** ₹{final_amount:.2f}\n\n"
                    f"Your payment has been processed successfully.\n"
                    f"Please check your payment method.")
                
                await q.answer("✅ Withdrawal approved!", show_alert=True)
                
                q.data = "withdrawal_queue"
                await callback(update, context)
        except Exception as e:
            logger.error(f"Error approving withdrawal {wid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
    
    # REJECT WITHDRAWAL - ATOMIC WITH REFUND
    elif d.startswith("rw_"):
        wid = int(d.split("_")[1])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                
                # ATOMIC UPDATE - Only reject if status is still 'pending'
                c.execute("""
                    UPDATE withdrawals 
                    SET status='rejected', processed_date=%s, rejection_reason=%s 
                    WHERE id=%s AND status='pending'
                    RETURNING user_id, amount
                """, (datetime.now().isoformat(), "Payment info invalid", wid))
                
                result = c.fetchone()
                
                if not result:
                    await q.answer("⚠️ Already processed!", show_alert=True)
                    return
                
                uid, amount = result['user_id'], round_decimal(result['amount'])
                
                # REFUND TO BALANCE
                c.execute("UPDATE users SET balance=balance+%s WHERE user_id=%s", (amount, uid))
                conn.commit()
                
                log_audit("reject_withdrawal", ADMIN_ID, uid, f"Withdrawal #{wid} - ₹{float(amount):.2f} refunded")
                
                await notify_user(context, uid,
                    f"❌ **Withdrawal Rejected**\n\n"
                    f"**Withdrawal ID:** #{wid}\n"
                    f"**Amount:** ₹{float(amount):.2f}\n"
                    f"**Reason:** Invalid payment information\n\n"
                    f"**Amount refunded to your balance.**\n"
                    f"Please update your payment details and try again.")
                
                await q.answer("❌ Rejected & refunded", show_alert=True)
                
                q.data = "withdrawal_queue"
                await callback(update, context)
        except Exception as e:
            logger.error(f"Error rejecting withdrawal {wid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)
# ==================== PART 8: ADMIN STATS AND MESSAGE HANDLERS ====================

    # USER MANAGEMENT
    elif d == "user_mgmt" and q.from_user.id == ADMIN_ID:
        await q.edit_message_text("👥 **User Management**\n\nSend user ID:\n\n/cancel to abort", parse_mode=None)
        return USER_SEARCH
    
    # BROADCAST
    elif d == "broadcast" and q.from_user.id == ADMIN_ID:
        await q.edit_message_text("📢 **Broadcast**\n\nSend message:\n\n/cancel to abort", parse_mode=None)
        return BROADCAST_MSG
    
    # STATS
    elif d == "stats" and q.from_user.id == ADMIN_ID:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            total_users = c.fetchone().values().__iter__().__next__()
            c.execute("SELECT COUNT(*) FROM gmail WHERE status='approved'")
            approved = c.fetchone().values().__iter__().__next__()
            c.execute("SELECT SUM(balance) FROM users")
            total_bal = float(c.fetchone().values().__iter__().__next__() or 0)
            c.execute("SELECT SUM(reward) FROM gmail WHERE status='approved'")
            paid = float(c.fetchone().values().__iter__().__next__() or 0)
            c.execute("SELECT COUNT(*) FROM referrals WHERE rewarded=1")
            refs = c.fetchone().values().__iter__().__next__()
            c.execute("SELECT SUM(reward) FROM referrals WHERE rewarded=1")
            ref_paid = float(c.fetchone().values().__iter__().__next__() or 0)
            c.execute("SELECT SUM(final_amount) FROM withdrawals WHERE status='approved'")
            withdrawn = float(c.fetchone().values().__iter__().__next__() or 0)
            c.execute("SELECT SUM(fee) FROM withdrawals WHERE status='approved'")
            fees_collected = float(c.fetchone().values().__iter__().__next__() or 0)
        
        text = f"""📊 **Statistics**

👥 **Users:** {total_users}
📧 **Approved:** {approved}
🔗 **Referrals (Rewarded):** {refs}

💰 **Balance:** ₹{total_bal:.2f}
💸 **Paid (Gmail):** ₹{paid:.2f}
💸 **Paid (Referral):** ₹{ref_paid:.2f}
💸 **Total Paid:** ₹{paid + ref_paid:.2f}
💵 **Withdrawn:** ₹{withdrawn:.2f}
💳 **Fees Collected:** ₹{fees_collected:.2f}"""
        
        await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙", callback_data="admin")]
        ]), parse_mode=None)
    
    # TOGGLE BLOCK
    elif d.startswith("block_"):
        uid = int(d.split("_")[1])
        
        try:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("UPDATE users SET is_blocked = 1 - is_blocked WHERE user_id=%s", (uid,))
                c.execute("SELECT is_blocked FROM users WHERE user_id=%s", (uid,))
                blocked = c.fetchone().values().__iter__().__next__()
                conn.commit()
            
            log_audit("block_user" if blocked else "unblock_user", ADMIN_ID, uid, "")
            
            await q.answer(f"{'⛔ Blocked' if blocked else '✅ Unblocked'}!", show_alert=True)
            
            try:
                await context.bot.send_message(
                    uid,
                    "⛔ You have been blocked" if blocked else "✅ You have been unblocked"
                )
            except:
                pass
        except Exception as e:
            logger.error(f"Error blocking/unblocking user {uid}: {e}")
            await q.answer("❌ Error occurred!", show_alert=True)

# ==================== MESSAGE HANDLERS ====================

async def receive_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    email = update.message.text.strip()
    
    is_valid, result = validate_email(email)
    if not is_valid:
        await update.message.reply_text(
            f"❌ **{result}**\n\n"
            f"Allowed domains: {', '.join(ALLOWED_DOMAINS)}\n"
            f"Please send a valid email address.\n"
            "/cancel to abort",
            parse_mode=None
        )
        return EMAIL
    
    email = result  # Use normalized email
    
    duplicate = check_duplicate_email(email)
    if duplicate:
        duplicate_status = duplicate['status']
        duplicate_user = duplicate['user_id']
        
        if duplicate_user == update.effective_user.id:
            msg = "You already submitted this email."
        else:
            msg = "This email has already been submitted by another user."
        
        await update.message.reply_text(
            f"❌ **Duplicate Email!**\n\n"
            f"{msg}\n"
            f"Status: {duplicate_status.title()}\n\n"
            f"/cancel to abort or send a different email",
            parse_mode=None
        )
        return EMAIL
    
    context.user_data['email'] = email
    await update.message.reply_text(
        "✅ **Email received!**\n\n"
        "Now send the password:\n"
        "(6-100 characters)",
        parse_mode=None
    )
    return PASSWORD

async def receive_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pwd = update.message.text.strip()
    
    if not validate_password(pwd):
        await update.message.reply_text(
            "❌ **Invalid password!**\n\n"
            "Password must be 6-100 characters.\n"
            "/cancel to abort",
            parse_mode=None
        )
        return PASSWORD
    
    uid = update.effective_user.id
    email = context.user_data['email']
    reward = calc_rate(uid)
    
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO gmail (user_id, email, password, reward, submit_date)
                         VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                      (uid, email, pwd, reward, datetime.now().isoformat()))
            gid = c.fetchone()['id']
            c.execute("UPDATE users SET total_gmail=total_gmail+1 WHERE user_id=%s", (uid,))
        
        update_submit_time(uid)
        
        context.user_data.clear()
        
        kb = [[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]
        await update.message.reply_text(
            f"✅ **Submitted Successfully!**\n\n"
            f"**ID:** #{gid}\n"
            f"**Email:** {mask_email(email)}\n"
            f"**Reward:** ₹{float(reward)}\n\n"
            f"⏳ Under review (24-48h)",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=None
        )
        
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"🆕 **New Gmail**\n\n"
                f"👤 {update.effective_user.first_name} (@{update.effective_user.username})\n"
                f"🆔 `{uid}`\n\n"
                f"📧 `{email}`\n"
                f"🔑 `{pwd}`\n"
                f"💰 ₹{float(reward)}",
                parse_mode=None
            )
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")
        
        return ConversationHandler.END
        
    except psycopg2.IntegrityError:
        await update.message.reply_text(
            "❌ **Duplicate submission!**\n\n"
            "This email was already submitted.",
            parse_mode=None
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_password: {e}")
        await update.message.reply_text(
            "❌ **Error occurred!**\n\n"
            "Please try again later.",
            parse_mode=None
        )
        return ConversationHandler.END

async def receive_upi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upi_id = update.message.text.strip()
    
    if not validate_upi(upi_id):
        await update.message.reply_text(
            "❌ **Invalid UPI ID!**\n\n"
            "Format: name@bank\n"
            "/cancel to abort",
            parse_mode=None
        )
        return UPI_ID
    
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET upi_id=%s WHERE user_id=%s", (upi_id, update.effective_user.id))
        
        kb = [[InlineKeyboardButton("🔙 Profile", callback_data="profile")]]
        await update.message.reply_text(
            f"✅ **UPI ID saved!**\n\n"
            f"**UPI:** `{upi_id}`",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=None
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_upi: {e}")
        await update.message.reply_text(
            "❌ **Error occurred!**\n\n"
            "Please try again later.",
            parse_mode=None
        )
        return ConversationHandler.END

async def receive_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    addr = update.message.text.strip()
    
    if not validate_usdt_address(addr):
        await update.message.reply_text(
            "❌ **Invalid USDT address!**\n\n"
            "Must be 34 characters, starting with 'T'\n"
            "/cancel to abort",
            parse_mode=None
        )
        return USDT_ADDRESS
    
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET usdt_address=%s WHERE user_id=%s", (addr, update.effective_user.id))
        
        kb = [[InlineKeyboardButton("🔙 Profile", callback_data="profile")]]
        await update.message.reply_text(
            f"✅ **USDT address saved!**\n\n"
            f"**Address:** `{addr[:10]}...{addr[-10:]}`",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=None
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_usdt: {e}")
        await update.message.reply_text(
            "❌ **Error occurred!**\n\n"
            "Please try again later.",
            parse_mode=None
        )
        return ConversationHandler.END
# ==================== PART 9: WITHDRAWAL HANDLER, ERROR HANDLER AND MAIN ====================

async def receive_withdraw_amt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = Decimal(update.message.text.strip())
        
        if amount < 100:
            await update.message.reply_text(
                "❌ **Minimum withdrawal: ₹100**\n\n"
                "Enter valid amount or /cancel",
                parse_mode=None
            )
            return WITHDRAW_AMT
        
        can_withdraw, remaining = can_withdraw_today(update.effective_user.id)
        if not can_withdraw:
            await update.message.reply_text(
                f"❌ **Daily limit reached!**\n\n"
                f"You can make {MAX_WITHDRAWALS_PER_DAY} withdrawals per day.\n"
                f"Try again tomorrow.",
                parse_mode=None
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
                    await update.message.reply_text("❌ Error occurred")
                    return ConversationHandler.END
                
                balance = round_decimal(result['balance'])
                
                if amount > balance:
                    await update.message.reply_text(
                        f"❌ **Insufficient balance!**\n\n"
                        f"**Balance:** ₹{float(balance):.2f}\n"
                        f"**Requested:** ₹{float(amount):.2f}",
                        parse_mode=None
                    )
                    return WITHDRAW_AMT
                
                payment_info = result['upi_id'] if method == 'upi' else result['usdt_address']
                method_name = "UPI" if method == 'upi' else "USDT TRC20"
                
                # ATOMIC DEDUCTION - Deduct balance IMMEDIATELY
                c.execute("""
                    UPDATE users 
                    SET balance=balance-%s 
                    WHERE user_id=%s AND balance >= %s
                    RETURNING balance
                """, (amount, update.effective_user.id, amount))
                
                updated = c.fetchone()
                
                if not updated:
                    await update.message.reply_text(
                        "❌ **Insufficient balance!**\n\n"
                        "Your balance may have changed. Please try again.",
                        parse_mode=None
                    )
                    return ConversationHandler.END
                
                c.execute("""INSERT INTO withdrawals (user_id, amount, fee, final_amount, method, payment_info, request_date)
                             VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                         (update.effective_user.id, amount, fee, final_amount, method, payment_info, datetime.now().isoformat()))
                wid = c.fetchone()['id']
                
                conn.commit()
        except Exception as e:
            logger.error(f"Error in withdrawal transaction: {e}")
            await update.message.reply_text(
                "❌ **Error occurred!**\n\n"
                "Please try again later.",
                parse_mode=None
            )
            return ConversationHandler.END
        
        context.user_data.clear()
        
        kb = [[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]
        await update.message.reply_text(
            f"✅ **Withdrawal Requested!**\n\n"
            f"**ID:** #{wid}\n"
            f"**Amount:** ₹{float(amount):.2f}\n"
            f"**Fee:** ₹{float(fee):.2f}\n"
            f"**Final Amount:** ₹{float(final_amount):.2f}\n"
            f"**Method:** {method_name}\n\n"
            f"⏳ Processing within 24-48h",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=None
        )
        
        try:
            await context.bot.send_message(
                ADMIN_ID,
                f"🆕 **Withdrawal Request**\n\n"
                f"👤 {update.effective_user.first_name}\n"
                f"🆔 `{update.effective_user.id}`\n\n"
                f"💰 **Amount:** ₹{float(amount):.2f}\n"
                f"💳 **Fee:** ₹{float(fee):.2f}\n"
                f"💵 **Final:** ₹{float(final_amount):.2f}\n"
                f"💳 **Method:** {method_name}\n"
                f"📄 **Info:** `{payment_info}`",
                parse_mode=None
            )
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid amount!**\n\n"
            "Enter a valid number or /cancel",
            parse_mode=None
        )
        return WITHDRAW_AMT
    except Exception as e:
        logger.error(f"Error in receive_withdraw_amt: {e}")
        await update.message.reply_text(
            "❌ **Error occurred!**\n\n"
            "Please try again later.",
            parse_mode=None
        )
        return ConversationHandler.END

async def receive_user_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    
    if not user_input.isdigit() or len(user_input) > 15:
        await update.message.reply_text(
            "❌ **Invalid user ID format!**\n\n"
            "Please enter a valid numeric user ID.",
            parse_mode=None
        )
        return USER_SEARCH
    
    try:
        uid = int(user_input)
        
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""SELECT username, first_name, balance, total_gmail, approved_gmail, 
                         is_blocked, joined_date FROM users WHERE user_id=%s""", (uid,))
            result = c.fetchone()
        
        if result:
            username, name, bal, total, approved, blocked, joined = (
                result['username'], result['first_name'], float(result['balance']), 
                result['total_gmail'], result['approved_gmail'], result['is_blocked'], result['joined_date']
            )
            status = "🔴 Blocked" if blocked else "🟢 Active"
            
            text = f"""👤 **User Info**

🆔 `{uid}`
👤 {name}
📱 @{username or 'N/A'}
📊 **Status:** {status}

💰 **Balance:** ₹{bal:.2f}
📧 **Gmail:** {approved}/{total}
📅 **Joined:** {joined[:10]}"""
            
            kb = [
                [InlineKeyboardButton("🔴 Block" if not blocked else "🟢 Unblock", 
                                     callback_data=f"block_{uid}")],
                [InlineKeyboardButton("🔙", callback_data="admin")]
            ]
            
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), 
                                           parse_mode=None)
        else:
            await update.message.reply_text("❌ User not found")
        
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("❌ Invalid ID format")
        return USER_SEARCH
    except Exception as e:
        logger.error(f"Error in receive_user_search: {e}")
        await update.message.reply_text("❌ Error occurred")
        return ConversationHandler.END

async def receive_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                await context.bot.send_message(row['user_id'], f"📢 **Announcement**\n\n{msg}", parse_mode=None)
                sent += 1
            except Exception as e:
                failed += 1
                logger.error(f"Failed to send broadcast to {row['user_id']}: {e}")
        
        log_audit("broadcast", ADMIN_ID, None, f"Sent: {sent}, Failed: {failed}")
        
        kb = [[InlineKeyboardButton("🔙 Admin", callback_data="admin")]]
        await update.message.reply_text(
            f"📢 **Broadcast Complete!**\n\n"
            f"✅ Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"📊 Total: {len(users)} users",
            reply_markup=InlineKeyboardMarkup(kb),
            parse_mode=None
        )
        
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_broadcast: {e}")
        await update.message.reply_text("❌ Error occurred")
        return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    kb = [[InlineKeyboardButton("🔙 Menu", callback_data="menu")]]
    await update.message.reply_text("❌ Cancelled", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages (for Start button and other interactions)"""
    text = update.message.text.lower().strip()
    
    if text in ['start', 'menu', 'hi', 'hello', 'hey']:
        await start(update, context)
    else:
        kb = [[InlineKeyboardButton("📱 Main Menu", callback_data="menu")]]
        await update.message.reply_text(
            "Use the buttons below to navigate:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and notify admin"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    try:
        if update and hasattr(update, 'effective_user'):
            user_id = update.effective_user.id if update.effective_user else "Unknown"
            error_msg = f"⚠️ **Error Report**\n\n" \
                       f"**User ID:** `{user_id}`\n" \
                       f"**Error:** `{str(context.error)[:200]}`"
            
            await context.bot.send_message(ADMIN_ID, error_msg, parse_mode=None)
    except Exception as e:
        logger.error(f"Failed to send error notification: {e}")

def main():
    print("🚀 Starting bot...")
    print("=" * 50)
    print("⚠️ Ensure only ONE Railway instance is running")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    # Conversation handlers
    gmail_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^submit$")],
        states={
            EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_email)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_password)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    withdraw_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^withdraw_(upi|usdt)$")],
        states={
            WITHDRAW_AMT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_withdraw_amt)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    usdt_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^set_usdt$")],
        states={USDT_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_usdt)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    upi_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^set_upi$")],
        states={UPI_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_upi)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    user_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^user_mgmt$")],
        states={USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_user_search)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    broadcast_conv = ConversationHandler(
        per_message=False,
        entry_points=[CallbackQueryHandler(callback, pattern="^broadcast$")],
        states={BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_broadcast)]},
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Register handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(gmail_conv)
    app.add_handler(withdraw_conv)
    app.add_handler(usdt_conv)
    app.add_handler(upi_conv)
    app.add_handler(user_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(CallbackQueryHandler(callback), group=1)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages))
    app.add_error_handler(error_handler)

    print("✅ Bot is running (polling)...")

    app.run_polling()


if __name__ == "__main__":
    main()