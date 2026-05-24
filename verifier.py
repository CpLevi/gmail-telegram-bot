"""
EarnX Gmail Bot — Email Verification Module
Background SMTP handshake checker to verify if Gmail accounts actually exist.
Uses Google's public MX servers — no login, no password, no 2FA needed.

Safety design:
- Runs in asyncio thread pool (never blocks the bot)
- 10-second hard timeout (never hangs)
- Returns "error" on ANY network issue (never falsely flags)
- Only marks "suspicious" on definitive 550 response
- Double-checks with retry before marking suspicious
"""

import asyncio
import logging
import smtplib
import socket
from urllib.parse import urlparse
import socks
from config import SMTP_PROXY

logger = logging.getLogger(__name__)

# Google's primary MX server for gmail.com
GMAIL_MX_SERVER = "gmail-smtp-in.l.google.com"
SMTP_PORT = 25
SMTP_TIMEOUT = 10  # seconds

# Verification status constants
STATUS_UNCHECKED = "unchecked"
STATUS_VERIFIED = "verified"
STATUS_SUSPICIOUS = "suspicious"
STATUS_ERROR = "error"


class SocksSMTP(smtplib.SMTP):
    """Subclass of smtplib.SMTP that routes connections through a SOCKS proxy."""
    def __init__(self, proxy_url, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        self.proxy_url = proxy_url
        super().__init__(timeout=timeout)

    def _get_socket(self, host, port, timeout):
        parsed = urlparse(self.proxy_url)
        scheme = parsed.scheme or "socks5"
        proxy_type = socks.SOCKS5 if "socks5" in scheme else socks.SOCKS4
        return socks.create_connection(
            (host, port),
            timeout=timeout,
            proxy_type=proxy_type,
            proxy_addr=parsed.hostname,
            proxy_port=parsed.port or 1080,
            proxy_username=parsed.username,
            proxy_password=parsed.password
        )


def _smtp_check_email(email: str) -> tuple[str, str]:
    """
    Perform a single SMTP handshake check against Google's MX server.
    This is a SYNCHRONOUS function — must be called via asyncio.to_thread().

    Returns:
        (status, detail_message)
        status: "verified" | "suspicious" | "error"
    """
    server = None
    try:
        if SMTP_PROXY:
            server = SocksSMTP(SMTP_PROXY, timeout=SMTP_TIMEOUT)
        else:
            server = smtplib.SMTP(timeout=SMTP_TIMEOUT)

        server.connect(GMAIL_MX_SERVER, SMTP_PORT)
        server.helo("gmail.com")
        server.mail("check@gmail.com")
        code, message = server.rcpt(email)

        msg_text = message.decode("utf-8", errors="ignore")

        if code == 250:
            return STATUS_VERIFIED, f"250 OK — Account exists"
        elif code == 550:
            return STATUS_SUSPICIOUS, f"550 — {msg_text[:100]}"
        else:
            # Unexpected code — treat as error (do not falsely flag)
            return STATUS_ERROR, f"Unexpected SMTP code {code}: {msg_text[:100]}"

    except (smtplib.SMTPException, socket.timeout, socket.error, OSError) as e:
        return STATUS_ERROR, f"Network error: {str(e)[:100]}"
    except Exception as e:
        logger.error(f"Unexpected error in SMTP check for {email}: {e}")
        return STATUS_ERROR, f"Unexpected error: {str(e)[:100]}"
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


async def check_gmail_exists(email: str) -> tuple[str, str]:
    """
    Async-safe Gmail existence check with retry logic.

    Performs TWO checks with a 2-second gap.
    Only marks as "suspicious" if BOTH checks return 550.
    Any network error or inconsistency → returns "error" (safe fallback).

    Returns:
        (status, detail_message)
    """
    if not email or not email.endswith("@gmail.com"):
        return STATUS_ERROR, "Not a gmail.com address"

    # First check
    status1, msg1 = await asyncio.to_thread(_smtp_check_email, email)

    if status1 == STATUS_VERIFIED:
        logger.info(f"✅ VERIFIED: {email} — {msg1}")
        return STATUS_VERIFIED, msg1

    if status1 == STATUS_ERROR:
        logger.warning(f"❓ ERROR checking {email}: {msg1}")
        return STATUS_ERROR, msg1

    # status1 == "suspicious" — do a SECOND check to confirm (avoid false positives)
    logger.info(f"⚠️ First check suspicious for {email}, retrying in 2s...")
    await asyncio.sleep(2)

    status2, msg2 = await asyncio.to_thread(_smtp_check_email, email)

    if status2 == STATUS_VERIFIED:
        # Second check says it exists — trust the positive result
        logger.info(f"✅ VERIFIED on retry: {email} — {msg2}")
        return STATUS_VERIFIED, msg2

    if status2 == STATUS_SUSPICIOUS:
        # Both checks say 550 — this is a definitive "does not exist"
        logger.warning(f"⚠️ SUSPICIOUS (confirmed): {email} — {msg2}")
        return STATUS_SUSPICIOUS, msg2

    # Second check had an error — be safe, return error
    logger.warning(f"❓ Retry error for {email}: {msg2}")
    return STATUS_ERROR, msg2
