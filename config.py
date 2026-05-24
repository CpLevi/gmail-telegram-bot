"""
EarnX Gmail Bot — Configuration
All constants, environment variables, and settings.
"""

import os
from decimal import Decimal

# ==================== BOT CREDENTIALS ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL")

TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "@EarnXOfficiial")
SUPPORT_USERNAME = "Mr_Carry07"

# ==================== WEBHOOK / DEPLOYMENT ====================
# If set, bot runs in webhook mode (Railway production)
# Otherwise, falls back to polling (local dev)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
PORT = int(os.getenv("PORT", 8443))

# ==================== DOMAIN RULES ====================
ALLOWED_DOMAINS = ["gmail.com"]

# ==================== FINANCIALS ====================
DEFAULT_GMAIL_RATE = Decimal("20")  # fallback if DB has no rate set
WITHDRAWAL_FEE_PERCENT = Decimal("5")
WITHDRAWAL_FEE_MIN = Decimal("5")
MAX_WITHDRAWALS_PER_DAY = 3
MAX_PENDING_WITHDRAWALS = 2
DEFAULT_MAX_WITHDRAWAL = Decimal("500")

# ==================== RATE LIMITS ====================
SUBMIT_COOLDOWN = 20  # seconds
MAX_PAGINATION_PAGE = 50

# Task expiry
SINGLE_TASK_EXPIRY_MINUTES = 30
BULK_TASK_EXPIRY_MINUTES = 120

# ==================== ADMIN PAGINATION ====================
ADMIN_USERS_PER_PAGE = 10
ADMIN_GMAIL_PER_PAGE = 10
ADMIN_WITHDRAWALS_PER_PAGE = 5

# ==================== GMAIL STATUS FLOW ====================
GMAIL_STATUS_PENDING = "pending"
GMAIL_STATUS_IN_REVIEW = "in_review"
GMAIL_STATUS_APPROVED = "approved"
GMAIL_STATUS_REJECTED = "rejected"

# Task-based statuses
TASK_STATUS_ASSIGNED = "assigned"
TASK_STATUS_CONFIRMED = "confirmed"

# ==================== CONVERSATION STATES ====================
(
    EMAIL,
    PASSWORD,
    USDT_ADDRESS,
    UPI_ID,
    WITHDRAW_AMT,
    BROADCAST_MSG,
    USER_SEARCH,
    BULK_GMAIL,
    WALLET_AMOUNT,
    WALLET_REASON,
    TASK_CONFIRM,
    BULK_TASK_QTY,
    BULK_TASK_CONFIRM,
    ADMIN_SET_PRICE,
    TOTP_SECRET,          # 2FA secret input (single task)
    TOTP_BULK_SECRET,     # 2FA secret input (bulk task, per-account)
    ADMIN_SET_VIDEO,      # Admin sets instruction video URL
    COOKIE_INPUT,         # Cookie input (single task, after 2FA)
    BULK_COOKIE_INPUT,    # Cookie input (bulk task, per-account, after 2FA)
    WITHDRAW_CONFIRM,     # User confirming withdrawal details
    ADMIN_SET_MAX_WITHDRAW,  # Admin setting max withdrawal limit
    WITHDRAW_REJECT_REASON,  # Admin inputting custom rejection reason
) = range(22)

# ==================== STARTUP VALIDATION ====================
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN environment variable is not set!")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL environment variable is not set!")
if ADMIN_ID == 0:
    raise RuntimeError("❌ ADMIN_ID environment variable is not set or is 0!")
