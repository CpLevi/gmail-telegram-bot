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
) = range(14)
