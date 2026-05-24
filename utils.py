"""
EarnX Gmail Bot — Utility Functions
Validation, calculations, cooldowns, and helper functions.
"""

import re
import hmac
import hashlib
import base64
import struct
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from config import (
    ALLOWED_DOMAINS, WITHDRAWAL_FEE_PERCENT, WITHDRAWAL_FEE_MIN,
    SUBMIT_COOLDOWN, MAX_PAGINATION_PAGE, MAX_WITHDRAWALS_PER_DAY,
    ADMIN_ID
)
from database import get_db

logger = logging.getLogger(__name__)


# ==================== RATE LIMITER ====================

class RateLimiter:
    """Simple in-memory rate limiter for bot commands."""

    def __init__(self, max_requests=15, window_seconds=60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests = {}  # user_id -> list of timestamps

    def is_allowed(self, user_id):
        """Check if user is within rate limit. Returns True if allowed."""
        import time as _time
        now = _time.time()

        if user_id not in self._requests:
            self._requests[user_id] = []

        # Clean old entries
        self._requests[user_id] = [
            t for t in self._requests[user_id]
            if now - t < self.window_seconds
        ]

        if len(self._requests[user_id]) >= self.max_requests:
            return False

        self._requests[user_id].append(now)
        return True

    def cleanup(self):
        """Remove stale entries to prevent memory leaks."""
        import time as _time
        now = _time.time()
        stale_users = [
            uid for uid, times in self._requests.items()
            if not times or now - max(times) > self.window_seconds * 2
        ]
        for uid in stale_users:
            del self._requests[uid]


# Global rate limiter instance (15 actions per 60 seconds per user)
rate_limiter = RateLimiter(max_requests=15, window_seconds=60)


# ==================== DECIMAL / MATH ====================

def round_decimal(value):
    """Round to 2 decimal places properly."""
    return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def calculate_withdrawal_fee(amount):
    """Calculate withdrawal fee with proper decimal precision."""
    amount = round_decimal(amount)
    fee_percent = amount * (WITHDRAWAL_FEE_PERCENT / Decimal("100"))
    fee = max(fee_percent, WITHDRAWAL_FEE_MIN)
    fee = round_decimal(fee)
    final_amount = round_decimal(amount - fee)
    return fee, final_amount


# ==================== EMAIL VALIDATION ====================

def normalize_email(email):
    """Normalize email for duplicate detection."""
    if not email:
        return email
    email = email.lower().strip()
    local, domain = email.split('@', 1)
    if domain == 'gmail.com':
        local = local.replace('.', '')
        if '+' in local:
            local = local.split('+')[0]
    return f"{local}@{domain}"


def validate_email(email):
    """Validate email and check domain."""
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


def mask_email(email):
    """Mask email for privacy."""
    if not email or '@' not in email:
        return email
    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = local[0] + '****'
    else:
        masked_local = local[:2] + '****'
    return f"{masked_local}@{domain}"


def check_duplicate_email(email):
    """Check if email exists (normalized)."""
    normalized = normalize_email(email)
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, status FROM gmail WHERE LOWER(TRIM(email))=%s LIMIT 1", (normalized,))
        return c.fetchone()


# ==================== PASSWORD / PAYMENT VALIDATION ====================

def validate_password(password):
    return password and 6 <= len(password) <= 100


def validate_upi(upi_id):
    if not upi_id or len(upi_id) > 50:
        return False
    pattern = r'^[\w.-]+@[\w]+$'
    return bool(re.match(pattern, upi_id))


def validate_usdt_address(address):
    if not address or len(address) != 42:
        return False
    if not address.startswith('0x'):
        return False
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


# ==================== PAGINATION ====================

def validate_page(page_str):
    """Validate pagination to prevent abuse."""
    try:
        page = int(page_str)
        if 0 <= page <= MAX_PAGINATION_PAGE:
            return page
        return 0
    except Exception:
        return 0


# ==================== COOLDOWN / LIMITS ====================

def can_submit_gmail(user_id):
    """Check cooldown for Gmail submission."""
    with get_db() as conn:
        c = conn.cursor()
        try:
            c.execute("SELECT last_submit_time FROM users WHERE user_id=%s", (user_id,))
        except Exception:
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
    """Update last submit time."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET last_submit_time=%s WHERE user_id=%s",
                  (datetime.now().isoformat(), user_id))


def update_last_activity(user_id):
    """Update user's last activity time for tracking active referrals."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            # Update only if it's been more than an hour since last update to avoid excessive DB writes
            c.execute("""
                UPDATE users 
                SET last_activity_time = %s 
                WHERE user_id = %s 
                AND (last_activity_time IS NULL OR last_activity_time < %s)
            """, (
                datetime.now().isoformat(), 
                user_id, 
                (datetime.now() - timedelta(hours=1)).isoformat()
            ))
    except Exception as e:
        logger.error(f"Error updating last activity: {e}")


def can_withdraw_today(user_id):
    """Check if user can withdraw today."""
    with get_db() as conn:
        c = conn.cursor()
        today = datetime.now().date().isoformat()
        c.execute("""SELECT COUNT(*) FROM withdrawals 
                    WHERE user_id=%s AND request_date::date=%s AND status IN ('pending', 'approved')""",
                  (user_id, today))
        count = list(c.fetchone().values())[0]
        return count < MAX_WITHDRAWALS_PER_DAY, MAX_WITHDRAWALS_PER_DAY - count


# ==================== USER CHECKS ====================

def is_blocked(user_id):
    """Check if user is blocked."""
    with get_db() as conn:
        c = conn.cursor()
        c.execute("SELECT is_blocked FROM users WHERE user_id=%s", (user_id,))
        result = c.fetchone()
        return result['is_blocked'] == 1 if result else False


def notifications_enabled(user_id):
    """Check if user has notifications enabled."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT notifications_enabled FROM users WHERE user_id=%s", (user_id,))
            result = c.fetchone()
            return result['notifications_enabled'] == 1 if result else True
    except Exception as e:
        logger.error(f"notifications_enabled error: {e}")
        return True


def ensure_user_exists(user):
    """Auto-register user if they don't exist in the database.

    Catches old users who interact with the bot without hitting /start
    on the new database. Uses INSERT ... ON CONFLICT DO NOTHING to be
    safe for concurrent calls.
    """
    if not user:
        return
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO users (user_id, username, first_name, joined_date)
                         VALUES (%s, %s, %s, %s)
                         ON CONFLICT (user_id) DO UPDATE SET
                         username = EXCLUDED.username,
                         first_name = EXCLUDED.first_name""",
                      (user.id, user.username, user.first_name,
                       datetime.now().isoformat()))
    except Exception as e:
        logger.error(f"ensure_user_exists error for {user.id}: {e}")


# ==================== NOTIFICATIONS ====================

async def notify_user(context, user_id, message):
    """Send notification to user with error handling."""
    try:
        if not notifications_enabled(user_id):
            logger.info(f"Notifications disabled for user {user_id}")
            return False

        await context.bot.send_message(user_id, message, parse_mode="HTML")
        logger.info(f"✅ Notification sent to user {user_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to notify user {user_id}: {e}")
        return False


# ==================== CHANNEL CHECK ====================

async def check_channel(user_id, context):
    """Check channel membership with error handling."""
    try:
        from config import TELEGRAM_CHANNEL
        channel = TELEGRAM_CHANNEL.lstrip('@')
        if not channel.startswith('@'):
            channel = '@' + channel

        member = await context.bot.get_chat_member(channel, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Channel check error for {user_id}: {e}")
        return False


# ==================== RATE (FIXED, ADMIN-CONTROLLED) ====================

def get_gmail_rate():
    """Get the current fixed Gmail rate from database."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_flags WHERE key='gmail_rate'")
            result = c.fetchone()
            if result:
                return Decimal(result['value'])
    except Exception as e:
        logger.error(f"Error fetching gmail rate: {e}")
    from config import DEFAULT_GMAIL_RATE
    return DEFAULT_GMAIL_RATE


def set_gmail_rate(new_rate):
    """Update the fixed Gmail rate in database. Returns True on success."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO system_flags (key, value) VALUES ('gmail_rate', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s
            """, (str(new_rate), str(new_rate)))
        return True
    except Exception as e:
        logger.error(f"Error setting gmail rate: {e}")
        return False


def is_task_submission_enabled():
    """Check if task submission is currently enabled."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_flags WHERE key='task_submission_enabled'")
            result = c.fetchone()
            if result:
                return result['value'] == 'true'
    except Exception as e:
        logger.error(f"Error checking task_submission_enabled: {e}")
    return True  # default to enabled if flag missing


def set_task_submission(enabled: bool):
    """Enable or disable task submissions. Returns True on success."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO system_flags (key, value) VALUES ('task_submission_enabled', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s
            """, ('true' if enabled else 'false', 'true' if enabled else 'false'))
        return True
    except Exception as e:
        logger.error(f"Error setting task_submission: {e}")
        return False


def is_bulk_submission_enabled():
    """Check if bulk task submission is currently enabled."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_flags WHERE key='bulk_submission_enabled'")
            result = c.fetchone()
            if result:
                return result['value'] == 'true'
    except Exception as e:
        logger.error(f"Error checking bulk_submission_enabled: {e}")
    return False  # default to DISABLED if flag missing


def set_bulk_submission(enabled: bool):
    """Enable or disable bulk task submissions. Returns True on success."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO system_flags (key, value) VALUES ('bulk_submission_enabled', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s
            """, ('true' if enabled else 'false', 'true' if enabled else 'false'))
        return True
    except Exception as e:
        logger.error(f"Error setting bulk_submission: {e}")
        return False


def calc_rate(user_id=None):
    """
    Fixed rate — same for all users.
    Admin can change it anytime via admin panel.
    user_id parameter kept for backward compatibility but ignored.
    """
    return get_gmail_rate()


def get_instruction_video_url():
    """Get the instruction video URL from database."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_flags WHERE key='instruction_video_url'")
            result = c.fetchone()
            if result and result['value']:
                return result['value']
    except Exception as e:
        logger.error(f"Error fetching instruction_video_url: {e}")
    return ""


def set_instruction_video_url(url: str):
    """Set the instruction video URL. Returns True on success."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO system_flags (key, value) VALUES ('instruction_video_url', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s
            """, (url, url))
        return True
    except Exception as e:
        logger.error(f"Error setting instruction_video_url: {e}")
        return False


def get_max_withdrawal_amount():
    """Get the maximum withdrawal amount from database."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_flags WHERE key='max_withdrawal_amount'")
            result = c.fetchone()
            if result:
                return Decimal(result['value'])
    except Exception as e:
        logger.error(f"Error fetching max_withdrawal_amount: {e}")
    from config import DEFAULT_MAX_WITHDRAWAL
    return DEFAULT_MAX_WITHDRAWAL


def set_max_withdrawal_amount(amount):
    """Set the maximum withdrawal amount. Returns True on success."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO system_flags (key, value) VALUES ('max_withdrawal_amount', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s
            """, (str(amount), str(amount)))
        return True
    except Exception as e:
        logger.error(f"Error setting max_withdrawal_amount: {e}")
        return False


def get_earnings_stats(user_id, period='all'):
    """Get earnings statistics for different time periods."""
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
        gmail_earnings = float(list(c.fetchone().values())[0])

        c.execute("""SELECT COALESCE(SUM(amount), 0) FROM referral_payouts 
                    WHERE referrer_id=%s AND date >= %s""",
                  (user_id, start_date))
        referral_earnings = float(list(c.fetchone().values())[0])

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


# ==================== AUDIT LOGGING ====================

def log_audit(action, admin_id, target_user_id=None, details=""):
    """Audit logging function."""
    try:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""INSERT INTO audit_log (action, admin_id, target_user_id, details, timestamp)
                        VALUES (%s, %s, %s, %s, %s)""",
                      (action, admin_id, target_user_id, details, datetime.now().isoformat()))
    except Exception as e:
        logger.error(f"Audit log error: {e}")


# ==================== MESSAGE HELPERS ====================

async def safe_edit_or_reply(q, text, reply_markup=None):
    """Safely edits a message if possible, otherwise sends a new reply."""
    try:
        if q.message and q.message.text != text:
            await q.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await q.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except Exception:
        try:
            await q.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
        except Exception:
            # Fallback to plain text if HTML parsing fails
            await q.message.reply_text(text, reply_markup=reply_markup, parse_mode=None)


# ==================== TOTP / 2FA UTILITIES ====================

def validate_totp_secret(secret: str) -> tuple[bool, str]:
    """Validate a TOTP secret key (base32 encoded).
    Returns (is_valid, cleaned_secret_or_error_message).
    """
    if not secret:
        return False, "Secret key cannot be empty."

    # Clean: remove spaces, dashes, convert to uppercase
    cleaned = secret.strip().replace(" ", "").replace("-", "").upper()

    if len(cleaned) < 16:
        return False, "Secret key is too short. It should be at least 16 characters."

    if len(cleaned) > 64:
        return False, "Secret key is too long."

    # Check valid base32 characters (A-Z, 2-7, =)
    import re as _re
    if not _re.match(r'^[A-Z2-7=]+$', cleaned):
        return False, "Invalid characters in secret key. It should only contain letters A-Z and digits 2-7."

    # Try to decode
    try:
        base64.b32decode(cleaned, casefold=True)
    except Exception:
        return False, "Invalid secret key format. Please copy the exact key from Gmail."

    return True, cleaned


def generate_totp(secret_base32: str, digits: int = 6, interval: int = 30) -> str:
    """Generate a TOTP code from a base32-encoded secret (RFC 6238).
    Uses only Python stdlib — no external dependencies needed.
    """
    # Decode the base32 secret
    key = base64.b32decode(secret_base32.upper(), casefold=True)

    # Current time step
    time_step = int(time.time()) // interval

    # Pack as big-endian unsigned 64-bit int
    msg = struct.pack('>Q', time_step)

    # HMAC-SHA1
    h = hmac.new(key, msg, hashlib.sha1).digest()

    # Dynamic truncation (RFC 4226 §5.4)
    offset = h[-1] & 0x0F
    code_int = struct.unpack('>I', h[offset:offset + 4])[0]
    code_int = code_int & 0x7FFFFFFF
    code_int = code_int % (10 ** digits)

    return str(code_int).zfill(digits)


def get_totp_remaining_seconds(interval: int = 30) -> int:
    """Get remaining seconds before the current TOTP code expires."""
    return interval - (int(time.time()) % interval)

