import os
from dotenv import load_dotenv
load_dotenv()

from handlers.user import build_referral_content
from config import ADMIN_ID

try:
    text, kb = build_referral_content(ADMIN_ID, "testbot")
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
