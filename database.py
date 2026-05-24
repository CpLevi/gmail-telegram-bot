"""
EarnX Gmail Bot — Database Layer
Connection management with pooling, schema initialization, and migrations.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from config import DATABASE_URL

logger = logging.getLogger(__name__)

# ==================== CONNECTION POOL ====================

_pool = None


def _get_pool():
    """Get or create the connection pool (lazy initialization)."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL,
            cursor_factory=RealDictCursor,
        )
        logger.info("✅ Database connection pool initialized (2-10 connections)")
    return _pool


@contextmanager
def get_db():
    """Get a database connection from the pool with automatic commit/rollback."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        pool.putconn(conn)


def close_pool():
    """Close the connection pool. Call on shutdown."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("🔒 Database connection pool closed")
    _pool = None


def init_db():
    """Initialize database schema and run migrations."""
    with get_db() as conn:
        c = conn.cursor()

        # ==================== CORE TABLES ====================

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

        # Gmail submissions table (with task-based columns)
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
            worker_sent_date TEXT,
            task_id TEXT UNIQUE,
            assigned_first_name TEXT,
            assigned_last_name TEXT,
            assigned_dob TEXT,
            assigned_gender TEXT,
            assigned_email TEXT,
            assigned_password TEXT,
            task_status TEXT DEFAULT 'assigned',
            task_assigned_at TEXT,
            task_confirmed_at TEXT,
            batch_id TEXT,
            totp_secret TEXT,
            cookie TEXT,
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

        # Referrals table
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

        # Admin wallet logs table
        c.execute('''CREATE TABLE IF NOT EXISTS admin_wallet_logs (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            action TEXT NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            reason TEXT NOT NULL,
            balance_before DECIMAL(10,2) NOT NULL,
            balance_after DECIMAL(10,2) NOT NULL,
            timestamp TEXT NOT NULL
        )''')

        # ==================== SYSTEM CONTROL TABLES ====================

        # Rate offers (time-limited promos)
        c.execute('''CREATE TABLE IF NOT EXISTS rate_rules (
            id SERIAL PRIMARY KEY,
            rate DECIMAL(10,2) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        # Auto engagement messages
        c.execute('''CREATE TABLE IF NOT EXISTS auto_messages (
            id SERIAL PRIMARY KEY,
            message TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        # System flags
        c.execute('''CREATE TABLE IF NOT EXISTS system_flags (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )''')

        # Default flags
        c.execute('''
            INSERT INTO system_flags (key, value)
            VALUES 
            ('auto_messages_enabled', 'true'),
            ('offers_enabled', 'true'),
            ('gmail_rate', '20'),
            ('task_submission_enabled', 'true'),
            ('bulk_submission_enabled', 'false'),
            ('instruction_video_url', ''),
            ('max_withdrawal_amount', '500')
            ON CONFLICT (key) DO NOTHING
        ''')

        # ==================== MIGRATIONS ====================
        columns_to_add = [
            ("users", "notifications_enabled", "INTEGER DEFAULT 1"),
            ("users", "last_submit_time", "TEXT"),
            ("gmail", "review_date", "TEXT"),
            ("gmail", "rejection_reason", "TEXT"),
            ("gmail", "worker_sent_date", "TEXT"),
            ("gmail", "task_id", "TEXT"),
            ("gmail", "assigned_first_name", "TEXT"),
            ("gmail", "assigned_last_name", "TEXT"),
            ("gmail", "assigned_dob", "TEXT"),
            ("gmail", "assigned_gender", "TEXT"),
            ("gmail", "assigned_email", "TEXT"),
            ("gmail", "assigned_password", "TEXT"),
            ("gmail", "task_status", "TEXT DEFAULT 'assigned'"),
            ("gmail", "task_assigned_at", "TEXT"),
            ("gmail", "task_confirmed_at", "TEXT"),
            ("gmail", "batch_id", "TEXT"),
            ("gmail", "totp_secret", "TEXT"),
            ("gmail", "cookie", "TEXT"),
            ("withdrawals", "processed_date", "TEXT"),
            ("withdrawals", "rejection_reason", "TEXT"),
            ("withdrawals", "fee", "DECIMAL(10,2) DEFAULT 0"),
            ("withdrawals", "final_amount", "DECIMAL(10,2)"),
            ("referrals", "rewarded", "INTEGER DEFAULT 0"),
        ]

        for table, column, definition in columns_to_add:
            try:
                c.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except psycopg2.Error:
                conn.rollback()
                logger.info(f"Adding {column} column to {table} table")
                c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                conn.commit()

        # ==================== INDEXES ====================
        indexes = [
            ("idx_gmail_user_status", "gmail", "user_id, status"),
            ("idx_gmail_status", "gmail", "status"),
            ("idx_gmail_email", "gmail", "email"),
            ("idx_gmail_status_worker", "gmail", "status, worker_sent_date"),
            ("idx_gmail_task_id", "gmail", "task_id"),
            ("idx_gmail_batch_id", "gmail", "batch_id"),
            ("idx_withdrawals_user_status", "withdrawals", "user_id, status"),
            ("idx_withdrawals_status", "withdrawals", "status"),
            ("idx_withdrawals_date", "withdrawals", "request_date"),
            ("idx_referrals_referrer", "referrals", "referrer_id"),
            ("idx_referrals_rewarded", "referrals", "rewarded"),
            ("idx_users_blocked", "users", "is_blocked"),
        ]

        for idx_name, table, columns in indexes:
            try:
                c.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")
            except Exception as e:
                logger.error(f"Error creating index {idx_name}: {e}")

        # ==================== FOREIGN KEY CONSTRAINTS ====================
        # Added safely — skips if constraint already exists or if orphaned data prevents it
        fk_constraints = [
            ("gmail", "fk_gmail_user", "user_id", "users", "user_id"),
            ("withdrawals", "fk_withdrawals_user", "user_id", "users", "user_id"),
            ("referrals", "fk_referrals_referrer", "referrer_id", "users", "user_id"),
            ("referrals", "fk_referrals_referred", "referred_id", "users", "user_id"),
        ]

        for table, constraint_name, column, ref_table, ref_column in fk_constraints:
            try:
                # Check if constraint already exists
                c.execute("""SELECT 1 FROM information_schema.table_constraints
                             WHERE constraint_name = %s AND table_name = %s""",
                          (constraint_name, table))
                if c.fetchone():
                    continue
                c.execute(f"""ALTER TABLE {table} ADD CONSTRAINT {constraint_name}
                              FOREIGN KEY ({column}) REFERENCES {ref_table}({ref_column})""")
                conn.commit()
                logger.info(f"✅ Added FK constraint {constraint_name}")
            except Exception as e:
                conn.rollback()
                logger.warning(f"⚠️ Could not add FK {constraint_name}: {e}")

        conn.commit()
        logger.info("✅ Database initialized successfully")
