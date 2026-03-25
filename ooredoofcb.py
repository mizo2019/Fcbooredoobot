import os
import logging
import requests
import json
import sys
import time
import hashlib
import hmac
import sqlite3
import datetime
import uuid
import base64
import threading
from threading import Thread
from datetime import datetime as dt_class
from flask import Flask, request as flask_request, jsonify

app = Flask(__name__)

# Deduplication
processed_mids  = set()
processed_lock  = threading.Lock()

# Global lock — only one OTP request to Ooredoo at a time
_otp_lock = threading.Lock()
# Minimum seconds between any two OTP requests
OTP_COOLDOWN = 5
_last_otp_time = 0

# ============================================================
# --- CONFIGURATION ---
# ============================================================
PAGE_ACCESS_TOKEN = "EAAnOVdTFkTsBQ8iRqIatXrslyV4MShPZABDO2yDEgCG69TDpoPOQ7HFznqNp76Uyk5RoIflZCVfJqzCk5Y40h1OSHr4dK8P7eN0L8q0S0mqckPbis3DS62RDFoztXLDyWpTsKPDs3W92F26AxrTa3zkK6YcWtw7Nk5n4L6XkAUarSYwTYAh0KAGNKwXSnT2nudVZBD5FwZDZD"
VERIFY_TOKEN      = "djezzybot"
DB_NAME = '/app/data/botusers.db'

# Facebook PSIDs (strings) of admins
ADMIN_IDS = {"30671135982485176"}

FB_SEND_URL = "https://graph.facebook.com/v18.0/me/messages"

# ============================================================
# --- TIMEZONE & LOGGING ---
# ============================================================
def get_algeria_now():
    tz_dz = datetime.timezone(datetime.timedelta(hours=1))
    return dt_class.now(tz_dz).replace(tzinfo=None)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_h = logging.StreamHandler(sys.stdout)
_h.setLevel(logging.INFO)
_h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_h)

# ============================================================
# --- FACEBOOK SEND API ---
# ============================================================
def send_message(recipient_id, text):
    """Send plain text to a Facebook user. Splits if > 1900 chars."""
    chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
    for chunk in chunks:
        payload = {
            "recipient": {"id": recipient_id},
            "message":   {"text": chunk}
        }
        r = requests.post(
            FB_SEND_URL,
            params={"access_token": PAGE_ACCESS_TOKEN},
            json=payload
        )
        if r.status_code != 200:
            logger.error("FB send error: %s %s", r.status_code, r.text)

def fetch_fb_profile(sender_id):
    """Profile fetch via Graph API requires advanced permissions not available.
    Name is stored when user registers via phone — no-op here."""
    pass

# ============================================================
# --- DATABASE ---
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # chat_id is TEXT because Facebook PSIDs are large numeric strings
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        chat_id TEXT PRIMARY KEY,
        phone_number TEXT,
        access_token TEXT,
        refresh_token TEXT,
        token_expires_in INTEGER,
        last_updated TEXT,
        device_uuid TEXT,
        instant_id TEXT,
        plan_type TEXT,
        last_played_time TEXT,
        full_name TEXT,
        username TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS admin_numbers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT UNIQUE,
        access_token TEXT,
        refresh_token TEXT,
        device_uuid TEXT,
        instant_id TEXT,
        plan_type TEXT,
        last_played_time TEXT,
        last_updated TEXT,
        label TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS bundle_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        full_name TEXT,
        username TEXT,
        offer_title TEXT,
        offer_price TEXT,
        activated_at TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS gift_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        full_name TEXT,
        gift_name TEXT,
        validity TEXT,
        claimed_at TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS snapchat_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        full_name TEXT,
        activated_at TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS blocked_users (
        chat_id TEXT PRIMARY KEY,
        blocked_at TEXT,
        reason TEXT
    )''')

    conn.commit()
    conn.close()

# ============================================================
# --- DB HELPERS (Users) ---
# ============================================================
def generate_synced_instant_id():
    u = uuid.uuid1()
    ts_100ns = u.time
    ts_ms = int((ts_100ns - 0x01b21dd213814000) / 10000)
    return f"{u}{str(ts_ms).ljust(13, '0')}"

def log_bundle_activation(phone, full_name, username, offer_title, offer_price):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO bundle_stats (phone, full_name, username, offer_title, offer_price, activated_at) VALUES (?,?,?,?,?,?)",
        (phone, full_name, username, offer_title, offer_price, get_algeria_now().strftime("%Y-%m-%d %H:%M"))
    )
    conn.commit()
    conn.close()

def get_bundle_stats():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT phone, full_name, username, offer_title, offer_price, activated_at FROM bundle_stats ORDER BY id DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    return rows

def log_gift_claim(phone, full_name, gift_name, validity):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO gift_stats (phone, full_name, gift_name, validity, claimed_at) VALUES (?,?,?,?,?)",
        (phone, full_name, gift_name, str(validity), get_algeria_now().strftime("%Y-%m-%d %H:%M"))
    )
    conn.commit()
    conn.close()

def log_snapchat_activation(phone, full_name):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO snapchat_stats (phone, full_name, activated_at) VALUES (?,?,?)",
        (phone, full_name, get_algeria_now().strftime("%Y-%m-%d %H:%M"))
    )
    conn.commit()
    conn.close()

def get_stats_counts():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM bundle_stats")
    bundles = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM gift_stats")
    gifts = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM snapchat_stats")
    snaps = c.fetchone()[0]
    conn.close()
    return bundles, gifts, snaps

# ============================================================
# --- BLACKLIST ---
# ============================================================
BANNED_WORDS = [
    "تعطي", "نكمك", "سوة", "فرخ", "تعطييي", "تعطيي", "تعطيييي",
    "nikmkn", "nkmk", "تفو", "تفوو", "تفووه", "تفوه",
    "قحبة", "عطاي", "كلب"
]
BLOCK_MSG = "يمنع استخدام هذه الألفاظ ، لقد تم حظرك ، لن تستطيع استخدام البوت بعد الآن ⛔️"

def is_blocked(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT chat_id FROM blocked_users WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    conn.close()
    return row is not None

def block_user(chat_id, reason="banned word"):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO blocked_users (chat_id, blocked_at, reason) VALUES (?,?,?)",
              (chat_id, get_algeria_now().strftime("%Y-%m-%d %H:%M"), reason))
    conn.commit()
    conn.close()

def unblock_user(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM blocked_users WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_all_blocked():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT chat_id, blocked_at, reason FROM blocked_users ORDER BY blocked_at DESC")
    rows = c.fetchall()
    conn.close()
    return rows

def contains_banned_word(text):
    lower = text.lower()
    for word in BANNED_WORDS:
        if word.lower() in lower:
            return True
    return False

def get_or_create_device_info(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT device_uuid, instant_id FROM users WHERE chat_id=?', (chat_id,))
    row = c.fetchone()

    instant_id = row[1] if row else None
    if not instant_id or len(instant_id) != 49:
        instant_id  = generate_synced_instant_id()
        device_uuid = instant_id[:36]
        if row:
            c.execute('UPDATE users SET device_uuid=?, instant_id=? WHERE chat_id=?',
                      (device_uuid, instant_id, chat_id))
        else:
            c.execute('INSERT INTO users (chat_id, device_uuid, instant_id, last_updated) VALUES (?,?,?,?)',
                      (chat_id, device_uuid, instant_id, get_algeria_now().isoformat()))
        conn.commit()
    else:
        device_uuid = instant_id[:36]

    conn.close()
    return device_uuid, instant_id

def save_user_data(chat_id, phone, access, refresh, expires):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    now = get_algeria_now().isoformat()
    c.execute('UPDATE users SET phone_number=?, access_token=?, refresh_token=?, token_expires_in=?, last_updated=? WHERE chat_id=?',
              (phone, access, refresh, expires, now, chat_id))
    if c.rowcount == 0:
        c.execute('INSERT INTO users (chat_id, phone_number, access_token, refresh_token, token_expires_in, last_updated) VALUES (?,?,?,?,?,?)',
                  (chat_id, phone, access, refresh, expires, now))
    conn.commit()
    conn.close()

def save_user_profile(chat_id, full_name, username):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT chat_id FROM users WHERE chat_id=?', (chat_id,))
    if c.fetchone():
        c.execute('UPDATE users SET full_name=?, username=? WHERE chat_id=?', (full_name, username, chat_id))
    else:
        c.execute('INSERT INTO users (chat_id, full_name, username, last_updated) VALUES (?,?,?,?)',
                  (chat_id, full_name, username, get_algeria_now().isoformat()))
    conn.commit()
    conn.close()

def logout_user(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE users SET access_token=NULL, refresh_token=NULL, phone_number=NULL, plan_type=NULL, last_played_time=NULL WHERE chat_id=?', (chat_id,))
    conn.commit()
    conn.close()

def update_user_plan(chat_id, plan):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE users SET plan_type=? WHERE chat_id=?', (plan, chat_id))
    conn.commit()
    conn.close()

def update_last_played(chat_id, played_time_str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE users SET last_played_time=? WHERE chat_id=?', (played_time_str, chat_id))
    conn.commit()
    conn.close()

def get_user_data(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT phone_number, access_token, refresh_token, token_expires_in, last_updated, device_uuid, instant_id, plan_type, last_played_time, full_name, username FROM users WHERE chat_id=?', (chat_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            'phone_number': row[0], 'access_token': row[1], 'refresh_token': row[2],
            'token_expires_in': row[3], 'last_updated': row[4],
            'device_uuid': row[5], 'instant_id': row[6],
            'plan_type': row[7], 'last_played_time': row[8],
            'full_name': row[9], 'username': row[10]
        }
    return None

def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT chat_id, phone_number, full_name, username, plan_type, last_updated, access_token FROM users WHERE phone_number IS NOT NULL ORDER BY last_updated DESC')
    rows = c.fetchall()
    conn.close()
    return rows

def delete_user(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('DELETE FROM users WHERE chat_id=?', (chat_id,))
    conn.commit()
    conn.close()

# ============================================================
# --- DB HELPERS (Admin Numbers) ---
# ============================================================
def save_admin_number(phone, access, refresh, device_uuid, instant_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    now = get_algeria_now().isoformat()
    c.execute('SELECT id FROM admin_numbers WHERE phone=?', (phone,))
    if c.fetchone():
        c.execute('UPDATE admin_numbers SET access_token=?, refresh_token=?, device_uuid=?, instant_id=?, last_updated=? WHERE phone=?',
                  (access, refresh, device_uuid, instant_id, now, phone))
    else:
        c.execute('INSERT INTO admin_numbers (phone, access_token, refresh_token, device_uuid, instant_id, last_updated) VALUES (?,?,?,?,?,?)',
                  (phone, access, refresh, device_uuid, instant_id, now))
    conn.commit()
    conn.close()

def get_admin_number(num_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT id, phone, access_token, refresh_token, device_uuid, instant_id, plan_type, last_played_time, last_updated, label FROM admin_numbers WHERE id=?', (num_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            'id': row[0], 'phone': row[1], 'access_token': row[2], 'refresh_token': row[3],
            'device_uuid': row[4], 'instant_id': row[5], 'plan_type': row[6],
            'last_played_time': row[7], 'last_updated': row[8], 'label': row[9]
        }
    return None

def get_all_admin_numbers():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('SELECT id, phone, label, plan_type, last_updated, access_token FROM admin_numbers ORDER BY last_updated DESC')
    rows = c.fetchall()
    conn.close()
    return rows

def update_admin_number_plan(num_id, plan):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE admin_numbers SET plan_type=? WHERE id=?', (plan, num_id))
    conn.commit()
    conn.close()

def update_admin_number_last_played(num_id, played_time_str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('UPDATE admin_numbers SET last_played_time=? WHERE id=?', (played_time_str, num_id))
    conn.commit()
    conn.close()

def delete_admin_number(num_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('DELETE FROM admin_numbers WHERE id=?', (num_id,))
    conn.commit()
    conn.close()

# ============================================================
# --- OOREDOO API ---
# ============================================================
URL_OTP              = "https://apis.ooredoo.dz/api/auth/realms/myooredoo/protocol/openid-connect/token"
URL_CHECKPOINT       = "https://apis.ooredoo.dz/api/ooredoo-bff/checkpoint/token"
URL_PACKAGES         = "https://apis.ooredoo.dz/api/ooredoo-bff/bundle/getActivePackages"
URL_GIFT_STATUS      = "https://apis.ooredoo.dz/api/ooredoo-bff/gamification/status"
URL_GIFT_PLAY        = "https://apis.ooredoo.dz/api/ooredoo-bff/gamification/play"
URL_SNAP_ELIGIBILITY = "https://apis.ooredoo.dz/api/ooredoo-bff/snap-chat/eligibility"
URL_SNAP_APPLY       = "https://apis.ooredoo.dz/api/ooredoo-bff/snap-chat/apply"
URL_BUNDLE_PURCHASE  = "https://apis.ooredoo.dz/api/ooredoo-bff/bundle/purchase/byop"

def generate_device_fingerprint(instance_id, phone, ts_str):
    key = ts_str.encode('utf-8')
    msg = (ts_str + instance_id + phone).encode('utf-8')
    return hmac.new(key, msg, hashlib.sha256).hexdigest()

def get_headers_verified(access_token, phone, instant_id):
    clean_phone = phone if not phone.startswith("05") else "213" + phone[1:]
    time.sleep(0.1)
    ts_now = str(int(time.time() * 1000))
    fp = generate_device_fingerprint(instant_id, clean_phone, ts_now)
    return {
        "X-Device-Fingerprint": fp,
        "X-Platform-Origin":    "mobile-android",
        "Authorization":        f"Bearer {access_token}",
        "X-Timestamp":          ts_now,
        "X-Instance-Id":        instant_id,
        "X-Msisdn":             clean_phone,
        "User-Agent":           "Dart/3.4 (dart:io)"
    }

def is_waf_block(r):
    """Detect Ooredoo WAF HTML rejection page."""
    return "Request Rejected" in r.text or "requested URL was rejected" in r.text

def request_checkpoint(phone, device_uuid):
    headers = {
        "X-msisdn":          phone,
        "X-platform-origin": "mobile-android",
        "X-path":            "/api/auth/realms/myooredoo/protocol/openid-connect/token",
        "X-method":          "POST",
        "X-Device-ID":       device_uuid,
        "User-Agent":        "Dart/3.4 (dart:io)",
        "Content-Type":      "application/x-www-form-urlencoded; charset=utf-8"
    }
    for attempt in range(4):
        try:
            logger.info("CHECKPOINT >> phone=%s device=%s attempt=%d", phone, device_uuid, attempt + 1)
            r = requests.post(URL_CHECKPOINT, headers=headers, timeout=15)
            logger.info("CHECKPOINT << status=%s nonce=%s chronos=%s body=%s",
                        r.status_code, r.headers.get("X-Nonce-Id"), r.headers.get("X-Chronos-Id"), r.text[:200])
            if is_waf_block(r):
                logger.warning("CHECKPOINT WAF block, retrying in 3s...")
                time.sleep(3)
                continue
            if r.status_code == 202:
                return {"ok": True, "nonce": r.headers.get("X-Nonce-Id"), "chronos": r.headers.get("X-Chronos-Id")}
            return {"ok": False, "err": f"Checkpoint Failed: {r.status_code}"}
        except Exception as e:
            logger.error("CHECKPOINT ERROR: %s", e)
            time.sleep(2)
    return {"ok": False, "err": "تعذر الاتصال بالخادم، حاول مجدداً ❌️"}

def send_otp_request(phone, nonce, chronos, device_uuid):
    global _last_otp_time
    headers = {
        "X-Nonce-Id":        nonce,
        "X-Chronos-Id":      chronos,
        "X-platform-origin": "mobile-android",
        "X-Device-ID":       device_uuid,
        "User-Agent":        "Dart/3.4 (dart:io)",
        "Content-Type":      "application/x-www-form-urlencoded; charset=utf-8"
    }
    data = {"client_id": "myooredoo-app", "grant_type": "password", "username": phone}
    with _otp_lock:
        elapsed = time.time() - _last_otp_time
        if elapsed < OTP_COOLDOWN:
            wait = OTP_COOLDOWN - elapsed
            logger.info("OTP cooldown: waiting %.1fs", wait)
            time.sleep(wait)
        for attempt in range(4):
            try:
                logger.info("SEND_OTP >> phone=%s nonce=%s chronos=%s attempt=%d", phone, nonce, chronos, attempt + 1)
                r = requests.post(URL_OTP, headers=headers, data=data, timeout=15)
                logger.info("SEND_OTP << status=%s body=%s", r.status_code, r.text[:300])
                if is_waf_block(r):
                    logger.warning("SEND_OTP WAF block, retrying in 3s...")
                    time.sleep(3)
                    continue
                _last_otp_time = time.time()
                if r.status_code == 403: return {"ok": True}
                return {"ok": False, "err": f"Send OTP Failed: {r.status_code}\n{r.text}"}
            except Exception as e:
                logger.error("SEND_OTP ERROR: %s", e)
                time.sleep(2)
        return {"ok": False, "err": "تعذر الاتصال بالخادم، حاول مجدداً ❌️"}

def verify_otp_request(phone, otp, nonce, chronos, device_uuid):
    headers = {
        "X-Nonce-Id":        nonce,
        "X-Chronos-Id":      chronos,
        "X-platform-origin": "mobile-android",
        "X-Device-ID":       device_uuid,
        "User-Agent":        "Dart/3.4 (dart:io)",
        "Content-Type":      "application/x-www-form-urlencoded; charset=utf-8"
    }
    data = {"client_id": "myooredoo-app", "grant_type": "password", "username": phone, "otp": otp}
    for attempt in range(4):
        try:
            logger.info("VERIFY_OTP >> phone=%s otp=%s attempt=%d", phone, otp, attempt + 1)
            r = requests.post(URL_OTP, headers=headers, data=data, timeout=15)
            logger.info("VERIFY_OTP << status=%s body=%s", r.status_code, r.text[:300])
            if is_waf_block(r):
                logger.warning("VERIFY_OTP WAF block, retrying in 3s...")
                time.sleep(3)
                continue
            if r.status_code == 200:
                j = r.json()
                return {"ok": True, "access": j.get("access_token"), "refresh": j.get("refresh_token")}
            return {"ok": False, "err": f"Verify Failed: {r.status_code}\n{r.text}"}
        except Exception as e:
            logger.error("VERIFY_OTP ERROR: %s", e)
            time.sleep(2)
    return {"ok": False, "err": "تعذر الاتصال بالخادم، حاول مجدداً ❌️"}

# ============================================================
# --- DATA FETCHING ---
# ============================================================
def fetch_balance_bundles(access_token, phone, instant_id):
    clean_phone = phone if not phone.startswith("05") else "213" + phone[1:]
    last_error  = "فشل جلب الرصيد"

    for attempt in range(3):
        try:
            headers = get_headers_verified(access_token, phone, instant_id)
            r = requests.get(f"{URL_PACKAGES}?msisdn={clean_phone}", headers=headers, timeout=15)
            if r.status_code != 200:
                last_error = f"فشل جلب الرصيد ({r.status_code})"
                time.sleep(1)
                continue

            data = r.json()

            raw_plan = data.get("planName", "Unknown")
            if "ibiza" in raw_plan.lower():    plan_name = "YOOZ"
            elif "alpha" in raw_plan.lower():  plan_name = "DIMA"
            elif raw_plan == "DimaX":          plan_name = "Dima Plus"
            else:                              plan_name = raw_plan

            balance = data.get("accountBalance", "0")
            msg = f"الرصيد: {balance} DA\n" + "-" * 20 + "\n"

            icons = {
                "DATA":          "🌐",
                "YOUTUBE":       "📺",
                "VOICE":         "📞",
                "VOICE_ON_NET":  "📞",
                "VOICE_OFF_NET": "📞",
                "SMS":           "✉️",
                "SMSALLNET":     "✉️",
                "FACEBOOK":      "📘",
                "INSTAGRAM":     "📸",
                "WHATSAPP":      "💬"
            }
            labels = {
                "VOICE_ON_NET":  "Appel Ooredoo",
                "VOICE_OFF_NET": "Appel Tout",
                "DATA":          "Internet",
                "SMSALLNET":     "SMS",
            }

            def fmt_expire(exp_str):
                try:
                    clean = exp_str.split(".")[0]
                    exp   = dt_class.strptime(clean, "%Y-%m-%dT%H:%M:%S") + datetime.timedelta(hours=1)
                    diff  = exp - get_algeria_now()
                    if diff.total_seconds() > 86400: return f"({diff.days} يوم)"
                    elif diff.total_seconds() > 0:   return "(ينتهي اليوم)"
                    else:                            return "(منتهي)"
                except: return ""

            def render_bundles(bundles, name_key="allocationName", val_key="remainingBalance"):
                lines = ""
                for b in bundles or []:
                    if not b: continue
                    name    = b.get(name_key) or b.get("allocationName", "Unknown")
                    rem     = b.get(val_key) or b.get("allocationValue", "0")
                    unit    = b.get("unit") or ""
                    icon    = icons.get(name, "📦")
                    display = labels.get(name, name)
                    days    = fmt_expire(b["expireDate"]) if b.get("expireDate") else ""
                    if str(rem).lower() == "unlimited":
                        lines += f"{icon} {display}: لا محدود {days}\n"
                    else:
                        lines += f"{icon} {display}: {rem} {unit} {days}\n"
                return lines

            found_any = False

            if data.get("activeBundles"):
                found_any = True
                msg += render_bundles(data["activeBundles"])

            mp = data.get("monthlyDataSmartBundlePurchases") or {}
            monthly = (mp.get("dataBundles") or []) + (mp.get("smartBundles") or [])
            if monthly:
                found_any = True
                msg += render_bundles(monthly)

            wp = data.get("weeklyBundlePurchases") or {}
            weekly = (wp.get("dataBundles") or []) + (wp.get("smartBundles") or []) + (wp.get("weeklyExclusiveBundles") or [])
            if weekly:
                found_any = True
                msg += "اسبوعي:\n"
                msg += render_bundles(weekly)

            dp = data.get("dailyBundlePurchases") or {}
            daily = (dp.get("dailyBundles") or []) + (dp.get("dataBundles") or []) + (dp.get("smartBundles") or [])
            if daily:
                found_any = True
                msg += "يومي:\n"
                msg += render_bundles(daily)

            byop = (data.get("byobBundlesPackage") or {}).get("dynamicBundle")
            dyn  = (data.get("dynamicBundlesPackage") or {}).get("dynamicBundle")
            for bundle_pkg in [byop, dyn]:
                if not bundle_pkg: continue
                pkgs = bundle_pkg if isinstance(bundle_pkg, list) else [bundle_pkg]
                for pkg in pkgs:
                    if not isinstance(pkg, dict): continue
                    allocs = pkg.get("bundleAllocations") or []
                    if allocs:
                        found_any = True
                        msg += render_bundles(allocs, val_key="allocationValue")

            pp = data.get("postPaidAllocations") or {}
            pp_bundles = pp.get("postPaidBundleAllocations") or []
            for pkg in pp_bundles:
                allocs = pkg.get("bundleAllocations") or []
                if allocs:
                    found_any = True
                    msg += render_bundles(allocs, val_key="allocationValue")

            if not found_any:
                msg += "لا توجد اشتراكات اساسية.\n"

            gamification = data.get("gamificationAllocations") or []
            if gamification:
                msg += "\nهدايا اللعبة النشطة:\n"
                g_icons = {"DATA": "🎁🌐", "VOICE": "🎁📞", "SMS": "🎁✉️"}
                for g in gamification:
                    name = g.get("allocationName", "Unknown")
                    acc  = g.get("account") or {}
                    rem  = acc.get("balance", "0")
                    unit = acc.get("unit", "")
                    icon = g_icons.get(name, "🎁")
                    days = ""
                    if acc.get("expiryDate"):
                        try:
                            exp  = dt_class.strptime(acc["expiryDate"], "%Y-%m-%d %H:%M:%S")
                            diff = exp - get_algeria_now()
                            if diff.total_seconds() > 0:
                                hrs, sec2 = divmod(diff.seconds, 3600)
                                mins, _  = divmod(sec2, 60)
                                days = f"(باقي {diff.days} يوم)" if diff.days > 0 else f"(باقي {hrs}س {mins}د)"
                            else:
                                days = "(منتهي)"
                        except: pass
                    msg += f"{icon} {name}: {rem} {unit} {days}\n"

            return msg, plan_name

        except Exception as e:
            last_error = str(e)
            logger.warning("fetch_balance_bundles attempt %d failed: %s", attempt + 1, e)
            time.sleep(1)

    logger.error("fetch_balance_bundles all retries failed: %s", last_error)
    return "تعذر جلب الرصيد، حاول مجدداً ⚠️", "Unknown"

def fetch_gift_info(entity_id, access_token, phone, instant_id, cached_last_played, is_admin_num=False):
    if cached_last_played:
        try:
            clean_ts = cached_last_played.split(".")[0]
            last_dt  = dt_class.strptime(clean_ts, "%Y-%m-%dT%H:%M:%S")
            rem = (last_dt + datetime.timedelta(hours=24)) - get_algeria_now()
            if rem.total_seconds() > 0:
                hrs, sec2 = divmod(rem.seconds, 3600)
                mins, _  = divmod(sec2, 60)
                return f"الهدية: باقي {hrs} ساعة و {mins} دقيقة", False
        except: pass

    headers = get_headers_verified(access_token, phone, instant_id)
    try:
        r = requests.get(URL_GIFT_STATUS, headers=headers, timeout=15)
        if r.status_code != 200:
            return f"خطأ هدية ({r.status_code})", False
        data = r.json()
        played          = data.get("played", False)
        last_played_str = data.get("lastPlayedTime")

        if played and last_played_str:
            if is_admin_num: update_admin_number_last_played(entity_id, last_played_str)
            else:            update_last_played(entity_id, last_played_str)
            try:
                clean_ts = last_played_str.split(".")[0]
                last_dt  = dt_class.strptime(clean_ts, "%Y-%m-%dT%H:%M:%S")
                rem = (last_dt + datetime.timedelta(hours=24)) - get_algeria_now()
                if rem.total_seconds() > 0:
                    hrs, sec2 = divmod(rem.seconds, 3600)
                    mins, _  = divmod(sec2, 60)
                    return f"الهدية: باقي {hrs} ساعة و {mins} دقيقة", False
                else:
                    return "الهدية متوفرة!", True
            except:
                return "خطأ في وقت الهدية", False
        else:
            return "الهدية متوفرة!", True
    except:
        return "خطأ شبكة", False

def fetch_snapchat_status(access_token, phone, instant_id):
    headers = get_headers_verified(access_token, phone, instant_id)
    device_id_short = instant_id[:36]
    sig = base64.b64encode(json.dumps({"platform": "android", "is-physical-device": True, "device-id": device_id_short}).encode()).decode()
    headers.update({"x-device-id": device_id_short, "x-platform-data-signature": sig})
    try:
        r = requests.get(URL_SNAP_ELIGIBILITY, headers=headers, timeout=15)
        if r.status_code == 200:
            eligible = r.json().get("eligible", False)
            return ("متوفر" if eligible else "غير متوفر"), eligible
        return "غير معروف", False
    except:
        return "خطأ", False

# ============================================================
# --- STATE + ACTIVE USER RESOLUTION ---
# ============================================================
user_states = {}  # sender_id (PSID string) -> state dict or string

def get_active_user_data(sender_id):
    """Resolve active user, respecting admin impersonation."""
    state = user_states.get(sender_id)
    if isinstance(state, dict) and state.get("st") == "impersonate":
        src = state["src"]
        eid = state["eid"]
        if src == "user":
            return get_user_data(eid), eid, False
        else:
            n = get_admin_number(eid)
            if n:
                return {
                    'phone_number':    n['phone'],
                    'access_token':    n['access_token'],
                    'refresh_token':   n['refresh_token'],
                    'instant_id':      n['instant_id'],
                    'device_uuid':     n['device_uuid'],
                    'plan_type':       n['plan_type'],
                    'last_played_time':n['last_played_time'],
                    'full_name': None, 'username': None,
                    'last_updated':    n['last_updated'],
                    'token_expires_in': None
                }, eid, True
    return get_user_data(sender_id), sender_id, False

# ============================================================
# --- DASHBOARD ---
# ============================================================
def show_dashboard(sender_id):
    u, eid, is_admin_num = get_active_user_data(sender_id)
    if not u or not u['access_token']:
        send_message(sender_id, "يجب تسجيل الدخول اولا.\nارسل 'سجلني' للبدء.")
        return

    send_message(sender_id, "جاري تحميل بياناتك...")

    bal_msg, plan = fetch_balance_bundles(u['access_token'], u['phone_number'], u['instant_id'])
    if is_admin_num: update_admin_number_plan(eid, plan)
    else:            update_user_plan(eid, plan)

    gift_msg, can_claim = fetch_gift_info(
        eid, u['access_token'], u['phone_number'],
        u['instant_id'], u['last_played_time'],
        is_admin_num=is_admin_num
    )

    snap_msg       = ""
    can_apply_snap = False
    if plan and plan.upper() == "YOOZ":
        snap_status, can_apply_snap = fetch_snapchat_status(u['access_token'], u['phone_number'], u['instant_id'])
        snap_msg = f"سناب شات: {snap_status}\n"

    # Build dynamic numbered action list
    action_labels = {
        "claim_gift":  "🎁 احصل على الهدية الان",
        "apply_snap":  "👻 تفعيل سناب شات",
        "offers_menu": "📢 قائمة العروض",
    }
    actions = []
    if can_claim:      actions.append("claim_gift")
    if can_apply_snap: actions.append("apply_snap")
    actions.append("offers_menu")

    text  = "📱 ـ لوحة التحكم ـ 📱\n"
    text += f"🔖 الخطة: {plan}\n"
    if snap_msg: text += f"👻 {snap_msg}"
    text += "\n" + bal_msg + "\n"
    text += "─" * 20 + "\n"
    text += "🎁 " + gift_msg + "\n\n"
    text += "⬇️ اختر رقماً:\n"
    for i, act in enumerate(actions, 1):
        text += f"{action_labels[act]} .{i}\n"

    user_states[sender_id] = {"st": "dashboard", "actions": actions}
    send_message(sender_id, text)

# ============================================================
# --- OFFERS ---
# ============================================================
OFFERS = {
    "offer_100go_100min": {
        "title":    "100Go + 100min 📞",
        "price":    "2000 DA",
        "category": "monthly",
        "description": "استفد من 100 جيغا و 100 دقيقة مكالمات صالحة لكل الشبكات مدة شهر كامل !",
        "body": {
            "validity": "Monthly",
            "limitedBundleDetails": [
                {"account": "data",   "allocation": 100},
                {"account": "offnet", "allocation": 100}
            ],
            "unlimitedBundleDetails": []
        }
    },
    "offer_50min": {
        "title":    "50 دقيقة نحو كل الشبكات",
        "price":    "90 دج",
        "category": "monthly",
        "description": "50 دقيقة نحو كل الشبكات ب 90 دينار فقط صالحة لمدة شهر",
        "body": {"validity": "Monthly", "limitedBundleDetails": [{"account": "offnet", "allocation": 50}], "unlimitedBundleDetails": []}
    },
    "offer_fb": {
        "title":    "فايسبوك 30 يوم",
        "price":    "200 دج",
        "category": "monthly",
        "description": "عرض فايسبوك لمدة شهر 200 دينار",
        "body": {"validity": "Monthly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["FB"]}
    },
    "offer_yt": {
        "title":    "يوتوب 30 يوم",
        "price":    "200 دج",
        "category": "monthly",
        "description": "عرض يوتوب لمدة شهر 200 دينار",
        "body": {"validity": "Monthly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["YT"]}
    },
    "offer_ig_monthly": {
        "title":    "انستغرام 30 يوم",
        "price":    "200 دج",
        "category": "monthly",
        "description": "استفد من انستغرام مجاني لمدة 30 يوم بسعر 200 دج فقط",
        "body": {"validity": "Monthly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["IG"]}
    },
    "offer_fb_weekly": {
        "title":    "فايسبوك 15 يوم",
        "price":    "100 دج",
        "category": "weekly",
        "description": "استفد من فايسبوك مجاني صالح مدة 15 يوم بسعر 100 دج فقط",
        "body": {"validity": "Biweekly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["FB"]}
    },
    "offer_yt_weekly": {
        "title":    "يوتوب 15 يوم",
        "price":    "100 دج",
        "category": "weekly",
        "description": "استفد من يوتوب مجاني صالح 15 يوم بسعر 100 دج فقط",
        "body": {"validity": "Biweekly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["YT"]}
    },
    "offer_ig_weekly": {
        "title":    "انستغرام 15 يوم",
        "price":    "100 دج",
        "category": "weekly",
        "description": "استفد من انستغرام مجاني صالح 15 يوم بسعر 100 دج فقط",
        "body": {"validity": "Biweekly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["IG"]}
    },
    "offer_wa_weekly": {
        "title":    "واتساب 15 يوم",
        "price":    "50 دج",
        "category": "weekly",
        "description": "استفد من واتساب مجاني صالح 15 يوم بسعر 50 دج فقط",
        "body": {"validity": "Biweekly", "limitedBundleDetails": [], "unlimitedBundleDetails": ["WA"]}
    }
}

def show_offers(sender_id, category=None):
    offer_keys = []
    text = "========== قائمة العروض ==========\n"
    i = 1

    monthly = [(k, v) for k, v in OFFERS.items() if v.get("category") == "monthly"]
    weekly  = [(k, v) for k, v in OFFERS.items() if v.get("category") == "weekly"]

    if monthly:
        text += "\n-- عروض شهرية --\n"
        for key, offer in monthly:
            text += f"{i}. {offer['title']} - {offer['price']}\n"
            offer_keys.append(key)
            i += 1

    if weekly:
        text += "\n-- عروض اسبوعية --\n"
        for key, offer in weekly:
            text += f"{i}. {offer['title']} - {offer['price']}\n"
            offer_keys.append(key)
            i += 1

    back_idx = i
    text += f"\n{i}. رجوع\n"

    user_states[sender_id] = {
        "st":         "offers_list",
        "offer_keys": offer_keys,
        "back_idx":   back_idx
    }
    send_message(sender_id, text)

def show_offer_confirm(sender_id, offer_key):
    offer = OFFERS.get(offer_key)
    if not offer:
        send_message(sender_id, "العرض غير موجود.")
        show_dashboard(sender_id)
        return
    text = (
        f"========== تفاصيل العرض ==========\n\n"
        f"{offer['description']}\n\n"
        f"1. نعم، فعّل العرض\n"
        f"2. الغاء\n"
    )
    user_states[sender_id] = {"st": "offer_confirm", "offer_key": offer_key}
    send_message(sender_id, text)

def do_purchase(sender_id, offer_key):
    offer = OFFERS.get(offer_key)
    if not offer:
        send_message(sender_id, "العرض غير موجود.")
        show_dashboard(sender_id)
        return

    u, eid, is_admin_num = get_active_user_data(sender_id)
    if not u or not u['access_token']:
        send_message(sender_id, "يجب تسجيل الدخول اولا.")
        return

    send_message(sender_id, "جاري تفعيل العرض...")

    phone = u['phone_number']
    if phone.startswith("05"): phone = "213" + phone[1:]

    headers = get_headers_verified(u['access_token'], phone, u['instant_id'])
    headers["Content-Type"] = "application/json"

    try:
        r = requests.post(URL_BUNDLE_PURCHASE, headers=headers, json=offer["body"], timeout=15)
        if r.status_code == 200:
            full_name = u.get('full_name') or ''
            username  = u.get('username') or ''
            log_bundle_activation(u['phone_number'], full_name, username, offer['title'], offer['price'])
            send_message(sender_id, f"تم تفعيل العرض بنجاح!\n\n{offer['description']}")
        else:
            try:
                resp_msg = r.json().get("message", "")
            except:
                resp_msg = ""
            if resp_msg == "DYNAMIC_CREDIT_LIMIT_NOT_ENOUGH":
                send_message(sender_id, "رصيدك غير كافي لتفعيل العرض.")
            else:
                send_message(sender_id, f"فشل التفعيل ({r.status_code})\n{r.text[:200]}")
    except Exception as e:
        send_message(sender_id, f"خطأ: {e}")

# ============================================================
# --- CLAIM GIFT ---
# ============================================================
def do_claim_gift(sender_id):
    u, eid, is_admin_num = get_active_user_data(sender_id)
    if not u: return

    phone = u['phone_number']
    if phone.startswith("05"): phone = "213" + phone[1:]

    send_message(sender_id, "جاري تحضير الهدية (الخطوة 1/2)...")

    headers_cp = {
        "X-msisdn":          phone,
        "X-platform-origin": "mobile-android",
        "X-path":            "/api/ooredoo-bff/gamification/play",
        "X-method":          "GET",
        "User-Agent":        "Dart/3.4 (dart:io)",
        "Content-Type":      "application/x-www-form-urlencoded; charset=utf-8"
    }
    try:
        r1 = requests.post(URL_CHECKPOINT, headers=headers_cp, timeout=15)
        if r1.status_code != 202:
            send_message(sender_id, f"فشل التحضير ({r1.status_code})")
            show_dashboard(sender_id)
            return
        nonce   = r1.headers.get("X-Nonce-Id")
        chronos = r1.headers.get("X-Chronos-Id")
    except Exception as e:
        send_message(sender_id, f"خطأ اتصال: {e}")
        show_dashboard(sender_id)
        return

    send_message(sender_id, "جاري فتح الهدية (الخطوة 2/2)...")
    headers_play = get_headers_verified(u['access_token'], phone, u['instant_id'])
    headers_play.update({"X-Nonce-Id": nonce, "X-Chronos-Id": chronos, "X-platform-origin": "mobile-android"})

    try:
        r2 = requests.get(URL_GIFT_PLAY, headers=headers_play, timeout=15)
        if r2.status_code == 200:
            data      = r2.json()
            gift_name = data.get("giftName", "هدية")
            validity  = data.get("validityHour", "?")
            if data.get("playedTime"):
                if is_admin_num: update_admin_number_last_played(eid, data["playedTime"])
                else:            update_last_played(eid, data["playedTime"])

            clean = str(gift_name).strip().lower()
            msg = "آسف، لم تحصل على شيء، جرب حظك غدا" \
                  if clean in ["0mo", "0 mo", "0mb", "0 mb"] \
                  else f"🎉🎁 مبروك! حصلت على:\n\nالهدية: {gift_name}\nالصلاحية: {validity} ساعة 🎉"
            send_message(sender_id, msg)
            log_gift_claim(u['phone_number'], u.get('full_name') or '', gift_name, validity)
        else:
            send_message(sender_id, f"خطأ اثناء الفتح ({r2.status_code})")
    except Exception as e:
        send_message(sender_id, f"خطأ: {e}")

# ============================================================
# --- SNAPCHAT ---
# ============================================================
def do_apply_snapchat(sender_id):
    u, eid, _ = get_active_user_data(sender_id)
    if not u: return

    phone = u['phone_number']
    if phone.startswith("05"): phone = "213" + phone[1:]

    send_message(sender_id, "جاري تفعيل عرض سناب شات...")

    headers = get_headers_verified(u['access_token'], phone, u['instant_id'])
    device_id_short = u['instant_id'][:36]
    sig = base64.b64encode(json.dumps({"platform": "android", "is-physical-device": True, "device-id": device_id_short}).encode()).decode()
    headers.update({"x-device-id": device_id_short, "x-platform-data-signature": sig})

    try:
        r = requests.post(URL_SNAP_APPLY, headers=headers, timeout=15)
        if r.status_code == 200:
            send_message(sender_id, "تم تفعيل عرض سنابشات بنجاح!")
            log_snapchat_activation(u['phone_number'], u.get('full_name') or '')
        else:
            send_message(sender_id, f"فشل تفعيل العرض ({r.status_code})")
    except Exception as e:
        send_message(sender_id, f"خطأ: {e}")

# ============================================================
# --- ADMIN PANEL ---
# ============================================================
def is_admin(sender_id):
    return sender_id in ADMIN_IDS

def show_admin_panel(sender_id):
    managed = get_all_admin_numbers()
    actions = []

    text = "========== لوحة الادارة ==========\n\n"
    i = 1

    text += f"{i}. المستخدمون\n"
    actions.append({"type": "users_list"})
    i += 1

    if managed:
        text += f"\n= ارقامي المضافة =\n"
        for num_id, phone, label, plan, last_updated, access_token in managed:
            display = label or phone
            dot     = "[مسجل]" if access_token else "[غير مسجل]"
            text += f"{i}. {dot} {display}\n"
            actions.append({"type": "admin_num", "num_id": num_id})
            i += 1

    text += f"\n{i}. اضافة رقم جديد\n"
    actions.append({"type": "add"})
    i += 1
    text += f"{i}. احصائيات\n"
    actions.append({"type": "stats"})
    i += 1
    text += f"{i}. المحظورون\n"
    actions.append({"type": "blocked"})
    i += 1
    text += f"{i}. تحديث\n"
    actions.append({"type": "refresh"})

    user_states[sender_id] = {"st": "admin_panel", "actions": actions}
    send_message(sender_id, text)

def show_admin_users_list(sender_id):
    users   = get_all_users()
    actions = []

    text = "========== المستخدمون ==========\n\n"
    i = 1

    if users:
        for chat_id, phone, full_name, username, plan, last_updated, access_token in users:
            name = full_name or phone or "بدون اسم"
            dot  = "[مسجل]" if access_token else "[غير مسجل]"
            text += f"{i}. {dot} {name}\n"
            actions.append({"type": "user", "target_id": chat_id})
            i += 1
    else:
        text += "لا يوجد مستخدمون مسجلون.\n"

    text += f"\n{i}. رجوع\n"
    actions.append({"type": "back_admin"})

    user_states[sender_id] = {"st": "admin_users_list", "actions": actions}
    send_message(sender_id, text)

def show_admin_user_detail(sender_id, target_id):
    u = get_user_data(target_id)
    if not u:
        send_message(sender_id, "المستخدم غير موجود.")
        show_admin_panel(sender_id)
        return

    has_token = "نعم" if u['access_token'] else "لا"
    name_display = u['full_name'] or u['phone_number'] or "بدون اسم"
    text = (
        f"========== تفاصيل المستخدم ==========\n\n"
        f"الاسم:       {name_display}\n"
        f"الرقم:       {u['phone_number'] or '---'}\n"
        f"الخطة:       {u['plan_type'] or '---'}\n"
        f"التوكن:      {has_token}\n"
        f"آخر تحديث:  {u['last_updated'] or '---'}\n\n"
    )
    actions = []
    i = 1
    if u['access_token']:
        text += f"{i}. دخول بالتوكن المحفوظ\n"
        actions.append({"type": "login_user", "target_id": target_id})
        i += 1
    text += f"{i}. حذف المستخدم\n"
    actions.append({"type": "delete_user", "target_id": target_id})
    i += 1
    text += f"{i}. رجوع\n"
    actions.append({"type": "back_admin"})

    user_states[sender_id] = {"st": "admin_user_detail", "actions": actions}
    send_message(sender_id, text)

def show_admin_num_detail(sender_id, num_id):
    n = get_admin_number(num_id)
    if not n:
        send_message(sender_id, "الرقم غير موجود.")
        show_admin_panel(sender_id)
        return

    has_token = "نعم" if n['access_token'] else "لا"
    text = (
        f"========== رقم مضاف ==========\n\n"
        f"الرقم:      {n['phone']}\n"
        f"التسمية:    {n['label'] or '---'}\n"
        f"الخطة:      {n['plan_type'] or '---'}\n"
        f"التوكن:     {has_token}\n"
        f"آخر تحديث: {n['last_updated'] or '---'}\n\n"
    )
    actions = []
    i = 1
    if n['access_token']:
        text += f"{i}. دخول بالتوكن المحفوظ\n"
        actions.append({"type": "login_num", "num_id": num_id})
        i += 1
    text += f"{i}. حذف\n"
    actions.append({"type": "delete_num", "num_id": num_id})
    i += 1
    text += f"{i}. رجوع\n"
    actions.append({"type": "back_admin"})

    user_states[sender_id] = {"st": "admin_num_detail", "actions": actions}
    send_message(sender_id, text)

def show_admin_stats(sender_id):
    bundles, gifts, snaps = get_stats_counts()
    text = (
        "========== الاحصائيات ==========\n\n"
        f"عمليات تفعيل العروض:  {bundles}\n"
        f"مطالبات الهدية:       {gifts}\n"
        f"تفعيلات سناب شات:    {snaps}\n"
    )
    send_message(sender_id, text)

    user_states[sender_id] = {"st": "admin_stats"}
    send_message(sender_id, "ارسل اي شيء للرجوع الى لوحة الادارة.")

def show_blocked_list(sender_id):
    rows    = get_all_blocked()
    actions = []
    text    = "========== المحظورون ==========\n\n"
    i = 1
    if rows:
        for chat_id, blocked_at, reason in rows:
            text += f"{i}. {chat_id}\n   {blocked_at} | {reason[:30]}\n"
            actions.append({"type": "unblock", "target_id": chat_id})
            i += 1
    else:
        text += "لا يوجد مستخدمون محظورون.\n"
    text += f"\n{i}. رجوع\n"
    actions.append({"type": "back_admin"})
    user_states[sender_id] = {"st": "admin_blocked_list", "actions": actions}
    send_message(sender_id, text)

# ============================================================
# --- LOGIN FLOW ---
# ============================================================
def start_login(sender_id):
    u = get_user_data(sender_id)
    fetch_fb_profile(sender_id)
    if u and u['access_token']:
        send_message(sender_id, "مرحبًا بك مجدداً!")
        show_dashboard(sender_id)
    else:
        get_or_create_device_info(sender_id)
        user_states[sender_id] = "phone"
        send_message(sender_id,
            "مرحبا بك في بوت اوريدو!\n\n"
            "ارسل رقم هاتفك للبدء (مثال: 0551234567):"
        )

# ============================================================
# --- MAIN MESSAGE HANDLER ---
# ============================================================
def handle_message(sender_id, text):
    txt   = text.strip()
    state = user_states.get(sender_id)

    # ── Block check ───────────────────────────────────────────
    if is_blocked(sender_id):
        return  # silently ignore blocked users

    # ── Banned word check ─────────────────────────────────────
    if contains_banned_word(txt):
        block_user(sender_id, reason=txt[:100])
        logger.info("Blocked user %s for banned word in: %s", sender_id, txt)
        send_message(sender_id, BLOCK_MSG)
        return

    # ── Global commands ──────────────────────────────────────
    if txt.lower() in [
        "بداية", "start", "مرحبا", "hello", "hi", "بدء",
        "تشغيل", "ابدا", "ابدء", "سجلني", "تسجيل",
        "sejalni", "sejelni", "sajalni", "sajelni",
        "sejjelni", "sejjalni", "sajjelni", "sajjalni",
        "مرحبًا 👋", "مرحبًا، sejjalni!"
    ]:
        start_login(sender_id)
        return

    if txt.lower() in ["خروج", "logout"]:
        logout_user(sender_id)
        user_states.pop(sender_id, None)
        send_message(sender_id, "تم تسجيل الخروج.\nارسل 'سجلني' لتسجيل الدخول مجدداً.")
        return

    if txt.lower() in ["الغاء", "cancel"]:
        user_states.pop(sender_id, None)
        send_message(sender_id, "تم الالغاء.\nارسل 'سجلني' للقائمة الرئيسية.")
        return

    if txt.lower() in ["admin", "ادمن", "ادارة"] and is_admin(sender_id):
        user_states.pop(sender_id, None)
        show_admin_panel(sender_id)
        return

    # ── Admin stats: any input → back to panel ────────────────
    if isinstance(state, dict) and state.get("st") == "admin_stats":
        show_admin_panel(sender_id)
        return

    # ── Admin: waiting for phone to add ──────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_add_phone":
        if not is_admin(sender_id):
            user_states.pop(sender_id, None)
            return
        phone = txt.replace(" ", "").replace("-", "")
        if phone.startswith("07"):
            send_message(sender_id, "آسف البوت لا يدعم ارقام جيزي 😢")
            return
        if phone.startswith("05"):    phone = "213" + phone[1:]
        elif phone.startswith("213"): pass
        else:
            send_message(sender_id, "تنسيق الرقم خطأ. ادخل رقماً يبدأ بـ 05...")
            return

        instant_id  = generate_synced_instant_id()
        device_uuid = instant_id[:36]

        send_message(sender_id, "جاري الاتصال...")
        sec = request_checkpoint(phone, device_uuid)
        if not sec["ok"]:
            send_message(sender_id, f"فشل الاتصال: {sec.get('err')}")
            return

        res = send_otp_request(phone, sec["nonce"], sec["chronos"], device_uuid)
        if res["ok"]:
            user_states[sender_id] = {
                "st": "admin_add_otp", "ph": phone,
                "device_uuid": device_uuid, "instant_id": instant_id
            }
            send_message(sender_id, "تم ارسال الرمز!\nادخل OTP:")
        else:
            send_message(sender_id, "فشل في ارسال الرمز اعد المحاولة ❌️")
        return

    # ── Admin: waiting for OTP to complete add-phone ──────────
    if isinstance(state, dict) and state.get("st") == "admin_add_otp":
        if not is_admin(sender_id):
            user_states.pop(sender_id, None)
            return
        ph          = state["ph"]
        device_uuid = state["device_uuid"]
        instant_id  = state["instant_id"]

        sec = request_checkpoint(ph, device_uuid)
        if not sec["ok"]:
            send_message(sender_id, f"فشل تحديث الجلسة: {sec.get('err')}")
            return

        res = verify_otp_request(ph, txt, sec["nonce"], sec["chronos"], device_uuid)
        if res["ok"]:
            save_admin_number(ph, res["access"], res["refresh"], device_uuid, instant_id)
            user_states.pop(sender_id, None)
            send_message(sender_id, f"تم اضافة الرقم {ph} بنجاح!")
            show_admin_panel(sender_id)
        else:
            send_message(sender_id, "الرمز غير صحيح اعد المحاولة ❌️")
        return

    # ── Login: phone input ────────────────────────────────────
    if state == "phone":
        phone = txt.replace(" ", "").replace("-", "")
        if phone.startswith("07"):
            send_message(sender_id, "آسف البوت لا يدعم ارقام جيزي 😢")
            return
        if phone.startswith("05"):    phone = "213" + phone[1:]
        elif phone.startswith("213"): pass
        else:
            send_message(sender_id, "تنسيق الرقم خطأ. ادخل رقماً يبدأ بـ 05...")
            return

        device_uuid, instant_id = get_or_create_device_info(sender_id)
        sec = request_checkpoint(phone, device_uuid)
        if not sec["ok"]:
            send_message(sender_id, f"فشل الاتصال: {sec.get('err')}")
            return

        res = send_otp_request(phone, sec["nonce"], sec["chronos"], device_uuid)
        if res["ok"]:
            user_states[sender_id] = {"st": "otp", "ph": phone}
            send_message(sender_id, "تم ارسال الرمز!\nادخل OTP:")
        else:
            send_message(sender_id, "فشل في ارسال الرمز اعد المحاولة ❌️")
        return

    # ── Login: OTP verification ───────────────────────────────
    if isinstance(state, dict) and state.get("st") == "otp":
        ph = state["ph"]
        device_uuid, instant_id = get_or_create_device_info(sender_id)

        sec = request_checkpoint(ph, device_uuid)
        if not sec["ok"]:
            send_message(sender_id, f"فشل تحديث الجلسة: {sec.get('err')}")
            return

        res = verify_otp_request(ph, txt, sec["nonce"], sec["chronos"], device_uuid)
        if res["ok"]:
            save_user_data(sender_id, ph, res["access"], res["refresh"], 3600)
            user_states.pop(sender_id, None)
            send_message(sender_id, "تم تسجيل الدخول بنجاح!")
            show_dashboard(sender_id)
        else:
            send_message(sender_id, "الرمز غير صحيح اعد المحاولة ❌️")
        return

    # ── Dashboard ─────────────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "dashboard":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act == "claim_gift":   do_claim_gift(sender_id)
                elif act == "apply_snap": do_apply_snapchat(sender_id)
                elif act == "offers_menu":show_offers(sender_id, "monthly")
            else:
                send_message(sender_id, "رقم غير صحيح، اختر من القائمة.")
                show_dashboard(sender_id)
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار من القائمة.")
            show_dashboard(sender_id)
        return

    # ── Offers list ───────────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "offers_list":
        offer_keys = state.get("offer_keys", [])
        back_idx   = state.get("back_idx")
        try:
            choice = int(txt)
            if 1 <= choice <= len(offer_keys):
                show_offer_confirm(sender_id, offer_keys[choice - 1])
            elif choice == back_idx:
                show_dashboard(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Offer confirm ─────────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "offer_confirm":
        offer_key = state.get("offer_key")
        try:
            choice = int(txt)
            if choice == 1:
                do_purchase(sender_id, offer_key)
            elif choice == 2:
                show_offers(sender_id)
            else:
                send_message(sender_id, "ارسل 1 للتاكيد او 2 للالغاء.")
        except ValueError:
            send_message(sender_id, "ارسل 1 للتاكيد او 2 للالغاء.")
        return

    # ── Admin panel ───────────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_panel":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act["type"] == "users_list":
                    show_admin_users_list(sender_id)
                elif act["type"] == "admin_num":
                    show_admin_num_detail(sender_id, act["num_id"])
                elif act["type"] == "add":
                    user_states[sender_id] = {"st": "admin_add_phone"}
                    send_message(sender_id, "ارسل رقم الهاتف المراد اضافته (مثال: 0555123456)\nارسل 'الغاء' للإلغاء.")
                elif act["type"] == "stats":
                    show_admin_stats(sender_id)
                elif act["type"] == "blocked":
                    show_blocked_list(sender_id)
                elif act["type"] == "refresh":
                    show_admin_panel(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Admin: blocked list ───────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_blocked_list":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act["type"] == "unblock":
                    unblock_user(act["target_id"])
                    send_message(sender_id, f"✅ تم رفع الحظر عن {act['target_id']}")
                    show_blocked_list(sender_id)
                elif act["type"] == "back_admin":
                    show_admin_panel(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Admin: users list ─────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_users_list":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act["type"] == "user":
                    show_admin_user_detail(sender_id, act["target_id"])
                elif act["type"] == "back_admin":
                    show_admin_panel(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Admin: user detail ────────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_user_detail":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act["type"] == "login_user":
                    user_states[sender_id] = {"st": "impersonate", "src": "user", "eid": act["target_id"]}
                    send_message(sender_id, "جاري التحميل...")
                    show_dashboard(sender_id)
                elif act["type"] == "delete_user":
                    delete_user(act["target_id"])
                    send_message(sender_id, "تم الحذف بنجاح.")
                    show_admin_panel(sender_id)
                elif act["type"] == "back_admin":
                    show_admin_panel(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Admin: number detail ──────────────────────────────────
    if isinstance(state, dict) and state.get("st") == "admin_num_detail":
        actions = state.get("actions", [])
        try:
            idx = int(txt) - 1
            if 0 <= idx < len(actions):
                act = actions[idx]
                if act["type"] == "login_num":
                    user_states[sender_id] = {"st": "impersonate", "src": "admin_num", "eid": act["num_id"]}
                    send_message(sender_id, "جاري التحميل...")
                    show_dashboard(sender_id)
                elif act["type"] == "delete_num":
                    delete_admin_number(act["num_id"])
                    send_message(sender_id, "تم الحذف بنجاح.")
                    show_admin_panel(sender_id)
                elif act["type"] == "back_admin":
                    show_admin_panel(sender_id)
            else:
                send_message(sender_id, "رقم غير صحيح.")
        except ValueError:
            send_message(sender_id, "ارسل رقماً للاختيار.")
        return

    # ── Fallback ──────────────────────────────────────────────
    send_message(sender_id, "ارسل 'سجلني' للقائمة الرئيسية.")

# ============================================================
# --- FLASK WEBHOOK ---
# ============================================================
@app.route('/webhook', methods=['GET'])
def verify_webhook():
    mode      = flask_request.args.get('hub.mode')
    token     = flask_request.args.get('hub.verify_token')
    challenge = flask_request.args.get('hub.challenge')
    if mode == 'subscribe' and token == VERIFY_TOKEN:
        logger.info("Webhook verified successfully.")
        return challenge, 200
    logger.warning("Webhook verification failed.")
    return 'Forbidden', 403

@app.route('/webhook', methods=['POST'])
def webhook():
    data = flask_request.get_json(silent=True)
    if not data or data.get('object') != 'page':
        return 'Not Found', 404
    Thread(target=process_events, args=(data,), daemon=True).start()
    return 'EVENT_RECEIVED', 200

def handle_text_event(psid, message):
    text = message.get('text', '').strip()
    if not text:
        return
    fetch_fb_profile(psid)
    try:
        handle_message(psid, text)
    except Exception as e:
        logger.exception("Error handling message from %s: %s", psid, e)
        try:
            send_message(psid, "حدث خطأ داخلي. حاول مجدداً او ارسل 'سجلني'.")
        except: pass

def handle_action(psid, payload):
    """Postback handler — treat payload as a text command."""
    try:
        handle_message(psid, payload)
    except Exception as e:
        logger.exception("Error handling postback from %s: %s", psid, e)

def process_events(data):
    for entry in data.get('entry', []):
        for event in entry.get('messaging', []):
            psid = event['sender']['id']

            # ── Deduplication ──────────────────────────
            mid = event.get('message', {}).get('mid') or \
                  event.get('postback', {}).get('mid', '')
            if mid:
                with processed_lock:
                    if mid in processed_mids:
                        logger.info("[SKIP] Duplicate mid: %s", mid)
                        continue
                    processed_mids.add(mid)
                    if len(processed_mids) > 2000:
                        processed_mids.clear()
            # ── End deduplication ──────────────────────

            logger.info("MSG from %s: %s", psid, event.get('message', {}).get('text', '[postback]'))

            if 'message' in event and not event['message'].get('is_echo'):
                handle_text_event(psid, event['message'])
            elif 'postback' in event:
                handle_action(psid, event['postback']['payload'])

# ============================================================
# --- MAIN ---
# ============================================================
# Add this line ↓ (runs with both Flask and Gunicorn)
os.makedirs('/app/data', exist_ok=True)
init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"Ooredoo FB Bot running on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)
