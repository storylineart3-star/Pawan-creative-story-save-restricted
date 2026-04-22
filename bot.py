import os
import re
import asyncio
import logging
import time
import requests
import signal
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
import shutil

# Database imports
from motor.motor_asyncio import AsyncIOMotorClient
import aiosqlite

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, CallbackQueryHandler
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    PhoneNumberUnoccupiedError
)

# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else []
DEFAULT_COOLDOWN = int(os.getenv("COOLDOWN", 10))
DEFAULT_AUTO_DELETE = int(os.getenv("AUTO_DELETE", 300))
MAX_DOWNLOAD_MB = int(os.getenv("MAX_DOWNLOAD_MB", 1024))
DIRECT_LIMIT_MB = 45
MONGO_URI = os.getenv("MONGO_URI")

# Webhook mode is required
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL environment variable is required for webhook mode.")

MAX_CONCURRENT = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== Database Abstraction Layer =====
# We'll use a class that supports both MongoDB and SQLite

class Database:
    def __init__(self):
        self.db_type = None
        self.mongo = None
        self.sqlite_path = "bot_data.db"
        self.initialized = False

    async def init(self):
        if self.initialized:
            return
        # Try MongoDB first
        if MONGO_URI:
            try:
                self.mongo = AsyncIOMotorClient(MONGO_URI)
                # Ping to check connection
                await self.mongo.admin.command('ping')
                self.db = self.mongo["telegram_bot"]
                self.db_type = "mongo"
                logger.info("Using MongoDB database")
                self.initialized = True
                return
            except Exception as e:
                logger.error(f"MongoDB connection failed: {e}. Falling back to SQLite.")
        # Fallback to SQLite
        self.db_type = "sqlite"
        async with aiosqlite.connect(self.sqlite_path) as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    joined_at TEXT,
                    last_activity TEXT,
                    request_count INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS requests (
                    user_id INTEGER,
                    timestamp TEXT,
                    link TEXT,
                    success INTEGER,
                    error TEXT
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value INTEGER
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    user_id INTEGER PRIMARY KEY,
                    session_string TEXT
                )
            ''')
            await conn.commit()
        logger.info("Using SQLite database (local file)")
        self.initialized = True

    # Config methods
    async def get_config(self, key: str, default: int) -> int:
        if self.db_type == "mongo":
            doc = await self.db.config.find_one({"_id": key})
            return doc["value"] if doc else default
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else default

    async def set_config(self, key: str, value: int):
        if self.db_type == "mongo":
            await self.db.config.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, value))
                await conn.commit()

    # User methods
    async def update_user(self, user: dict):
        user_id = user["id"]
        now = datetime.now(timezone.utc).isoformat()
        if self.db_type == "mongo":
            await self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "username": user.get("username"),
                        "first_name": user.get("first_name"),
                        "last_name": user.get("last_name"),
                        "last_activity": datetime.now(timezone.utc),
                    },
                    "$setOnInsert": {"joined_at": datetime.now(timezone.utc), "request_count": 0, "is_banned": False},
                    "$inc": {"request_count": 1},
                },
                upsert=True
            )
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute('''
                    INSERT INTO users (user_id, username, first_name, last_name, joined_at, last_activity, request_count)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        last_activity = excluded.last_activity,
                        request_count = request_count + 1
                ''', (user_id, user.get("username"), user.get("first_name"), user.get("last_name"), now, now))
                await conn.commit()

    async def is_banned(self, user_id: int) -> bool:
        if self.db_type == "mongo":
            user = await self.db.users.find_one({"user_id": user_id})
            return user.get("is_banned", False) if user else False
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    return bool(row[0]) if row else False

    async def log_request(self, user_id: int, link: str, success: bool, error: str = None):
        if self.db_type == "mongo":
            await self.db.requests.insert_one({
                "user_id": user_id,
                "timestamp": datetime.now(timezone.utc),
                "link": link,
                "success": success,
                "error": error
            })
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute(
                    "INSERT INTO requests (user_id, timestamp, link, success, error) VALUES (?, ?, ?, ?, ?)",
                    (user_id, datetime.now(timezone.utc).isoformat(), link, 1 if success else 0, error)
                )
                await conn.commit()

    async def get_user_session(self, user_id: int) -> Optional[str]:
        if self.db_type == "mongo":
            doc = await self.db.sessions.find_one({"user_id": user_id})
            return doc["session_string"] if doc else None
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else None

    async def save_user_session(self, user_id: int, session_string: str):
        if self.db_type == "mongo":
            await self.db.sessions.update_one(
                {"user_id": user_id},
                {"$set": {"session_string": session_string}},
                upsert=True
            )
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute(
                    "INSERT OR REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)",
                    (user_id, session_string)
                )
                await conn.commit()

    async def delete_user_session(self, user_id: int):
        if self.db_type == "mongo":
            await self.db.sessions.delete_one({"user_id": user_id})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
                await conn.commit()

    # Admin stats
    async def get_total_users(self) -> int:
        if self.db_type == "mongo":
            return await self.db.users.count_documents({})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT COUNT(*) FROM users") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0

    async def get_banned_users(self) -> int:
        if self.db_type == "mongo":
            return await self.db.users.count_documents({"is_banned": True})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0

    async def get_total_requests(self) -> int:
        if self.db_type == "mongo":
            return await self.db.requests.count_documents({})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT COUNT(*) FROM requests") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0

    async def get_today_requests(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        if self.db_type == "mongo":
            start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            return await self.db.requests.count_documents({"timestamp": {"$gte": start}})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT COUNT(*) FROM requests WHERE date(timestamp) = ?", (today,)) as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0

    async def get_all_users(self, limit: int, offset: int):
        if self.db_type == "mongo":
            cursor = self.db.users.find().sort("joined_at", -1).skip(offset).limit(limit)
            return await cursor.to_list(length=limit)
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute(
                    "SELECT user_id, username, request_count FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?",
                    (limit, offset)
                ) as cursor:
                    return await cursor.fetchall()

    async def get_user_by_id(self, user_id: int):
        if self.db_type == "mongo":
            return await self.db.users.find_one({"user_id": user_id})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    return await cursor.fetchone()

    async def set_user_banned(self, user_id: int, banned: bool):
        if self.db_type == "mongo":
            await self.db.users.update_one({"user_id": user_id}, {"$set": {"is_banned": banned}})
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                await conn.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (1 if banned else 0, user_id))
                await conn.commit()

    async def get_non_banned_users(self):
        if self.db_type == "mongo":
            cursor = self.db.users.find({"is_banned": False})
            return await cursor.to_list(length=None)
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                async with conn.execute("SELECT user_id FROM users WHERE is_banned = 0") as cursor:
                    return await cursor.fetchall()

# Initialize database
db = Database()

# ===== In‑memory structures =====
task_queue = asyncio.Queue()
active_tasks: Dict[int, asyncio.Task] = {}
queue_order: List[int] = []
user_position: Dict[int, int] = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# ===== Helper functions that use the db abstraction =====
async def get_cooldown() -> int:
    return await db.get_config("cooldown", DEFAULT_COOLDOWN)

async def get_auto_delete() -> int:
    return await db.get_config("auto_delete", DEFAULT_AUTO_DELETE)

async def update_user(user: dict):
    await db.update_user(user)

async def is_banned(user_id: int) -> bool:
    return await db.is_banned(user_id)

async def log_request(user_id: int, link: str, success: bool, error: str = None):
    await db.log_request(user_id, link, success, error)

async def get_user_session(user_id: int) -> Optional[str]:
    return await db.get_user_session(user_id)

async def save_user_session(user_id: int, session_string: str):
    await db.save_user_session(user_id, session_string)

async def delete_user_session(user_id: int):
    await db.delete_user_session(user_id)

# ===== Telethon client cache =====
clients: Dict[int, TelegramClient] = {}

async def get_client(user_id: int) -> Optional[TelegramClient]:
    if user_id in clients:
        return clients[user_id]
    session_str = await get_user_session(user_id)
    if not session_str:
        return None
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    clients[user_id] = client
    return client

async def logout_user(user_id: int):
    if user_id in clients:
        await clients[user_id].disconnect()
        del clients[user_id]
    await delete_user_session(user_id)

# ===== Cooldown management =====
cooldown_timestamps: Dict[int, float] = {}

async def check_cooldown(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return False
    cooldown_sec = await get_cooldown()
    last = cooldown_timestamps.get(user_id, 0)
    if time.time() - last < cooldown_sec:
        return True
    cooldown_timestamps[user_id] = time.time()
    return False

# ===== Queue position update =====
def update_positions():
    for idx, uid in enumerate(queue_order, start=1):
        user_position[uid] = idx

# ===== Worker that processes tasks from the queue =====
async def worker():
    while True:
        task_data = await task_queue.get()
        user_id = task_data["user_id"]
        update_obj = task_data["update"]
        context = task_data["context"]
        client = task_data["client"]
        entity = task_data["entity"]
        msg_id = task_data["msg_id"]
        progress_msg = task_data["progress_msg"]
        link = task_data["link"]

        async with semaphore:
            async def do_work():
                try:
                    await process_message(
                        update_obj, context, user_id, link,
                        client, entity, msg_id, progress_msg
                    )
                except asyncio.CancelledError:
                    await progress_msg.edit_text("❌ Task cancelled by user.")
                    await log_request(user_id, link, False, "Cancelled by user")
                    raise
                except Exception as e:
                    logger.exception(f"Error in worker for user {user_id}")
                    await progress_msg.edit_text(f"❌ Error: {str(e)}")
                    await log_request(user_id, link, False, str(e))

            task = asyncio.create_task(do_work())
            active_tasks[user_id] = task
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                active_tasks.pop(user_id, None)
                if user_id in queue_order:
                    queue_order.remove(user_id)
                update_positions()
                task_queue.task_done()
# ===== Background task that does the actual fetching and uploading =====
async def process_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
    user_id: int, link: str, client, entity, msg_id: int, progress_msg
):
    try:
        message = await client.get_messages(entity, ids=msg_id)
        if not message:
            await progress_msg.edit_text("❌ Message not found.")
            await log_request(user_id, link, False, "Message not found")
            return

        if message.text and not message.media:
            await progress_msg.delete()
            sent = await update.message.reply_text(message.text)
            auto_del = await get_auto_delete()
            asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
            await log_request(user_id, link, True)
            return

        if message.media:
            file_size = message.file.size if message.file else None
            if file_size:
                size_mb = file_size / (1024 * 1024)
                if size_mb > MAX_DOWNLOAD_MB:
                    await progress_msg.edit_text(f"❌ File too large ({size_mb:.1f} MB). Max {MAX_DOWNLOAD_MB} MB.")
                    await log_request(user_id, link, False, f"File too large: {size_mb} MB")
                    return

                if size_mb > DIRECT_LIMIT_MB:
                    await progress_msg.edit_text(f"📥 Downloading {size_mb:.1f} MB...")
                    last_percent = -1
                    async def dl_progress(current, total):
                        nonlocal last_percent
                        if total > 0:
                            percent = int(current * 100 / total)
                            if percent != last_percent:
                                last_percent = percent
                                await progress_msg.edit_text(f"📥 Downloading... {percent}%")
                    file_path = await client.download_media(message, progress_callback=dl_progress)
                    await progress_msg.edit_text("📤 Uploading to cloud (gofile.io)...")

                    try:
                        resp = requests.get('https://api.gofile.io/servers', timeout=10)
                        if resp.status_code != 200:
                            raise Exception("Failed to get upload server")
                        data = resp.json()
                        if data.get('status') != 'ok':
                            raise Exception(f"Server API error: {data.get('error', 'Unknown')}")
                        server = data['data']['servers'][0]['name']
                        upload_url = f'https://{server}.gofile.io/uploadFile'

                        with open(file_path, 'rb') as f:
                            up_resp = requests.post(upload_url, files={'file': f}, timeout=300)
                        if up_resp.status_code == 200:
                            up_data = up_resp.json()
                            if up_data.get('status') == 'ok':
                                download_link = up_data['data']['downloadPage']
                                direct = up_data['data'].get('directLink')
                                if direct:
                                    download_link = direct
                                await progress_msg.delete()
                                sent = await update.message.reply_text(
                                    f"✅ File uploaded to cloud:\n{download_link}\n\n"
                                    "⚠️ Note: The file will be deleted after 7 days of inactivity."
                                )
                                await log_request(user_id, link, True)
                                asyncio.create_task(delete_file_after(file_path, 60))
                                auto_del = await get_auto_delete()
                                asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                                return
                            else:
                                error_msg = up_data.get('error', 'Unknown error')
                                await progress_msg.edit_text(f"❌ Upload failed: {error_msg}")
                        else:
                            await progress_msg.edit_text(f"❌ Upload failed: HTTP {up_resp.status_code}")
                        await log_request(user_id, link, False, f"Upload failed")
                    except Exception as e:
                        await progress_msg.edit_text(f"❌ Upload failed: {str(e)}")
                        await log_request(user_id, link, False, str(e))
                        asyncio.create_task(delete_file_after(file_path, 60))
                        return
                else:
                    await progress_msg.edit_text(f"📥 Downloading {size_mb:.1f} MB...")
                    last_percent = -1
                    async def dl_progress(current, total):
                        nonlocal last_percent
                        if total > 0:
                            percent = int(current * 100 / total)
                            if percent != last_percent:
                                last_percent = percent
                                await progress_msg.edit_text(f"📥 Downloading... {percent}%")
                    file_path = await client.download_media(message, progress_callback=dl_progress)
                    await progress_msg.edit_text("📤 Uploading to Telegram...")
                    with open(file_path, "rb") as f:
                        if message.audio:
                            sent = await update.message.reply_audio(f, caption=message.text or "")
                        elif message.video:
                            sent = await update.message.reply_video(f, caption=message.text or "")
                        elif message.photo:
                            sent = await update.message.reply_photo(f, caption=message.text or "")
                        else:
                            sent = await update.message.reply_document(f, caption=message.text or "")
                    asyncio.create_task(delete_file_after(file_path, 60))
                    auto_del = await get_auto_delete()
                    asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                    await progress_msg.delete()
                    await log_request(user_id, link, True)
                    return
            else:
                file_path = await client.download_media(message)
                await progress_msg.edit_text("📤 Uploading...")
                with open(file_path, "rb") as f:
                    if message.audio:
                        sent = await update.message.reply_audio(f, caption=message.text or "")
                    elif message.video:
                        sent = await update.message.reply_video(f, caption=message.text or "")
                    elif message.photo:
                        sent = await update.message.reply_photo(f, caption=message.text or "")
                    else:
                        sent = await update.message.reply_document(f, caption=message.text or "")
                asyncio.create_task(delete_file_after(file_path, 60))
                auto_del = await get_auto_delete()
                asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                await progress_msg.delete()
                await log_request(user_id, link, True)
                return
    except Exception as e:
        logger.exception("Error in process_message")
        await progress_msg.edit_text(f"❌ Error: {str(e)}")
        await log_request(user_id, link, False, str(e))

# ===== Command: /start, /help, /myinfo, /logout =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update_user(user.to_dict())
    await update.message.reply_text(
        "👋 **Welcome to the Channel Media Saver Bot!**\n\n"
        "This bot uses a user account to fetch messages from public channels.\n"
        "First, use /login to connect your Telegram account.\n\n"
        f"📦 **File limits:**\n"
        f"- ≤{DIRECT_LIMIT_MB} MB: sent directly\n"
        f"- {DIRECT_LIMIT_MB} MB – {MAX_DOWNLOAD_MB} MB: uploaded to cloud\n"
        f"- >{MAX_DOWNLOAD_MB} MB: rejected\n\n"
        "ℹ️ Use /help for full guide.",
        parse_mode="Markdown"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cooldown = await get_cooldown()
    auto_del = await get_auto_delete()
    await update.message.reply_text(
        f"📘 **GUIDE**\n\n"
        "1. Use /login to connect your Telegram account.\n"
        "2. You must be a member of private channels/groups to save content.\n"
        "3. Send any Telegram message link.\n\n"
        f"⚠️ **Limits:**\n"
        f"- Cooldown: {cooldown} seconds\n"
        f"- File size: ≤{DIRECT_LIMIT_MB} MB → direct\n"
        f"- {DIRECT_LIMIT_MB} MB – {MAX_DOWNLOAD_MB} MB → cloud\n"
        f"- >{MAX_DOWNLOAD_MB} MB → rejected\n\n"
        "📌 **Commands:**\n"
        "/start /help /myinfo /login /logout /cancel\n\n"
        f"Messages auto‑delete after {auto_del} seconds.",
        parse_mode="Markdown"
    )

async def myinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await db.get_user_by_id(user_id)
    if not user:
        await update.message.reply_text("No data found. Send a link first.")
        return
    if db.db_type == "mongo":
        info = (
            f"👤 **Your Info**\n"
            f"User ID: `{user_id}`\n"
            f"Username: @{user.get('username', 'N/A')}\n"
            f"Requests: {user.get('request_count', 0)}\n"
            f"Joined: {user['joined_at'].strftime('%Y-%m-%d %H:%M')}\n"
            f"Banned: {'Yes' if user.get('is_banned') else 'No'}"
        )
    else:
        info = (
            f"👤 **Your Info**\n"
            f"User ID: `{user_id}`\n"
            f"Username: @{user[1] or 'N/A'}\n"
            f"Requests: {user[6]}\n"
            f"Joined: {user[4]}\n"
            f"Banned: {'Yes' if user[7] else 'No'}"
        )
    await update.message.reply_text(info, parse_mode="Markdown")

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in clients and not await get_user_session(user_id):
        await update.message.reply_text("ℹ️ You are not logged in.")
        return
    await logout_user(user_id)
    await update.message.reply_text("✅ Logged out. Your session is deleted.")

# ===== Login conversation =====
PHONE, CODE, PASSWORD = range(3)

async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📱 Send phone number with country code.\nExample: `+919999999999`", parse_mode="Markdown")
    return PHONE

async def login_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not re.match(r'^\+\d{7,15}$', phone):
        await update.message.reply_text("❌ Invalid phone number. Start again /login")
        return ConversationHandler.END
    context.user_data["phone"] = phone
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        await client.send_code_request(phone)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send code: {str(e)}")
        return ConversationHandler.END
    context.user_data["client"] = client
    await update.message.reply_text("🔢 Enter OTP like: `1 2 3 4 5` (spaces required)", parse_mode="Markdown")
    return CODE

async def login_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.replace(" ", "")
    if not code.isdigit():
        await update.message.reply_text("❌ Invalid OTP. Start again /login")
        return ConversationHandler.END
    client = context.user_data["client"]
    user_id = update.effective_user.id
    try:
        await client.sign_in(context.user_data["phone"], code)
    except SessionPasswordNeededError:
        await update.message.reply_text("🔑 Enter your 2FA password:")
        return PASSWORD
    except Exception as e:
        await update.message.reply_text(f"❌ Login failed: {str(e)}")
        return ConversationHandler.END
    session = client.session.save()
    await save_user_session(user_id, session)
    clients[user_id] = client
    await update.message.reply_text("✅ Login successful! You can now use the bot.", parse_mode="Markdown")
    return ConversationHandler.END

async def login_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text
    client = context.user_data["client"]
    user_id = update.effective_user.id
    try:
        await client.sign_in(password=password)
    except Exception as e:
        await update.message.reply_text(f"❌ 2FA failed: {str(e)}")
        return ConversationHandler.END
    session = client.session.save()
    await save_user_session(user_id, session)
    clients[user_id] = client
    await update.message.reply_text("✅ Login successful! Send any link now.", parse_mode="Markdown")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Login cancelled.")
    return ConversationHandler.END

# ===== Admin commands =====
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ Unauthorized.")
            return
        return await func(update, context)
    return wrapper

@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_users = await db.get_total_users()
    banned = await db.get_banned_users()
    total_req = await db.get_total_requests()
    today_req = await db.get_today_requests()
    cooldown = await get_cooldown()
    auto_del = await get_auto_delete()
    msg = (
        f"📊 **Stats**\n"
        f"Users: {total_users}\nBanned: {banned}\n"
        f"Total requests: {total_req}\nToday: {today_req}\n"
        f"Cooldown: {cooldown}s\nAuto‑delete: {auto_del}s"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

@admin_only
async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page = 0
    if context.args:
        try:
            page = int(context.args[0]) - 1
        except: pass
    limit = 10
    users = await db.get_all_users(limit, page * limit)
    if not users:
        await update.message.reply_text("No users.")
        return
    text = "**Users (latest):**\n"
    if db.db_type == "mongo":
        for u in users:
            text += f"• `{u['user_id']}` - @{u.get('username', 'N/A')} - {u.get('request_count',0)} reqs\n"
    else:
        for u in users:
            text += f"• `{u[0]}` - @{u[1] or 'N/A'} - {u[2]} reqs\n"
    text += f"\nPage {page+1}. Use `/users {page+2}` for next."
    await update.message.reply_text(text, parse_mode="Markdown")

@admin_only
async def user_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /user <id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID.")
        return
    user = await db.get_user_by_id(uid)
    if not user:
        await update.message.reply_text("Not found.")
        return
    if db.db_type == "mongo":
        info = f"👤 **User {uid}**\nUsername: @{user.get('username','N/A')}\nRequests: {user.get('request_count',0)}\nBanned: {user.get('is_banned',False)}"
    else:
        info = f"👤 **User {uid}**\nUsername: @{user[1] or 'N/A'}\nRequests: {user[6]}\nBanned: {bool(user[7])}"
    await update.message.reply_text(info, parse_mode="Markdown")

@admin_only
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ban <id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID.")
        return
    await db.set_user_banned(uid, True)
    await update.message.reply_text(f"✅ Banned {uid}.")

@admin_only
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /unban <id>")
        return
    try:
        uid = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID.")
        return
    await db.set_user_banned(uid, False)
    await update.message.reply_text(f"✅ Unbanned {uid}.")

@admin_only
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = await db.get_non_banned_users()
    if update.message.reply_to_message:
        msg = update.message.reply_to_message
        await update.message.reply_text("📢 Broadcasting...")
        count = 0
        for user in users:
            uid = user["user_id"] if isinstance(user, dict) else user[0]
            try:
                await msg.copy(uid)
                count += 1
                await asyncio.sleep(0.05)
            except: pass
        await update.message.reply_text(f"✅ Sent to {count} users.")
    else:
        if not context.args:
            await update.message.reply_text("Reply to a message with /broadcast or provide text.")
            return
        text = " ".join(context.args)
        await update.message.reply_text("📢 Broadcasting...")
        count = 0
        for user in users:
            uid = user["user_id"] if isinstance(user, dict) else user[0]
            try:
                await context.bot.send_message(uid, text)
                count += 1
                await asyncio.sleep(0.05)
            except: pass
        await update.message.reply_text(f"✅ Sent to {count} users.")

@admin_only
async def set_cooldown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setcooldown <seconds>")
        return
    try:
        sec = int(context.args[0])
        if sec < 1: raise ValueError
        await db.set_config("cooldown", sec)
        await update.message.reply_text(f"✅ Cooldown set to {sec}s.")
    except:
        await update.message.reply_text("Invalid number.")

@admin_only
async def set_autodelete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setautodelete <seconds>")
        return
    try:
        sec = int(context.args[0])
        if sec < 1: raise ValueError
        await db.set_config("auto_delete", sec)
        await update.message.reply_text(f"✅ Auto‑delete set to {sec}s.")
    except:
        await update.message.reply_text("Invalid number.")

# ===== Inline Cancel Button Handler =====
async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    if data.startswith("cancel_"):
        target_user = int(data.split("_")[1])
        if user_id != target_user:
            await query.edit_message_text("❌ You can only cancel your own requests.")
            return
        if user_id in active_tasks and not active_tasks[user_id].done():
            active_tasks[user_id].cancel()
            await query.edit_message_text("🛑 Your ongoing request has been cancelled.")
        elif user_id in queue_order:
            queue_order.remove(user_id)
            update_positions()
            await query.edit_message_text("🗑️ Your request has been removed from the queue.")
        else:
            await query.edit_message_text("ℹ️ No active or queued request found.")
    else:
        await query.edit_message_text("❌ Invalid action.")

# ===== Main link handler with queue and inline cancel button =====
async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text

    if "t.me" not in text:
        await update.message.reply_text("❌ Please send a valid Telegram message link.")
        return

    if await is_banned(user_id):
        await update.message.reply_text("⛔ You are banned.")
        return

    await update_user(update.effective_user.to_dict())

    if await check_cooldown(user_id):
        remaining = await get_cooldown()
        await update.message.reply_text(f"⏳ Please wait {remaining} seconds before another request.")
        return

    client = await get_client(user_id)
    if not client:
        await update.message.reply_text("⚠️ You need to login first. Use /login")
        return

    match = re.search(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', text)
    if not match:
        await update.message.reply_text("❌ Invalid link format. Use 'Copy Message Link'.")
        await log_request(user_id, text, False, "Invalid link format")
        return

    chat_part = match.group(1)
    msg_id = int(match.group(2))

    try:
        if chat_part.isdigit():
            entity = await client.get_entity(int(f"-100{chat_part}"))
        else:
            entity = await client.get_entity(chat_part)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to resolve channel: {str(e)}")
        await log_request(user_id, text, False, str(e))
        return

    queue_order.append(user_id)
    update_positions()
    pos = user_position[user_id]

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel Request", callback_data=f"cancel_{user_id}")]
    ])
    progress_msg = await update.message.reply_text(
        f"📥 Added to queue at position {pos}. You will be notified when processing starts.\n"
        f"Press the button below to cancel this request.",
        reply_markup=keyboard
    )

    await task_queue.put({
        "user_id": user_id,
        "update": update,
        "context": context,
        "client": client,
        "entity": entity,
        "msg_id": msg_id,
        "progress_msg": progress_msg,
        "link": text
    })

# ===== Auto‑delete helpers =====
async def delete_file_after(file_path, delay):
    await asyncio.sleep(delay)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except:
        pass

async def auto_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_id: int):
    await asyncio.sleep(await get_auto_delete())
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except:
        pass

# ===== /cancel command fallback =====
async def kill_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in active_tasks and not active_tasks[user_id].done():
        active_tasks[user_id].cancel()
        await update.message.reply_text("🛑 Your ongoing request has been cancelled.")
    elif user_id in queue_order:
        queue_order.remove(user_id)
        update_positions()
        await update.message.reply_text("🗑️ Your request has been removed from the queue.")
    else:
        await update.message.reply_text("ℹ️ No active or queued request found.")

# ===== Post-init function to start worker =====
async def post_init(application: Application):
    """Initialize database and start worker after application is ready."""
    await db.init()
    asyncio.create_task(worker())

# ===== Main function =====
def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("myinfo", myinfo))
    app.add_handler(CommandHandler("logout", logout))
    app.add_handler(CommandHandler("cancel", kill_request))

    conv = ConversationHandler(
        entry_points=[CommandHandler("login", login_start)],
        states={
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_phone)],
            CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_code)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_password)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(conv)

    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("users", users_list))
    app.add_handler(CommandHandler("user", user_details))
    app.add_handler(CommandHandler("ban", ban_user))
    app.add_handler(CommandHandler("unban", unban_user))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("setcooldown", set_cooldown))
    app.add_handler(CommandHandler("setautodelete", set_autodelete))

    app.add_handler(CallbackQueryHandler(cancel_callback, pattern="^cancel_"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))

    # Webhook mode (required)
    port = int(os.environ.get("PORT", 8080))
    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        url_path=BOT_TOKEN
    )

if __name__ == "__main__":
    main()
