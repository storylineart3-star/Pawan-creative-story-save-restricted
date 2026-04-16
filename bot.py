import os
import re
import asyncio
import logging
import time
import requests
from datetime import datetime, timezone
from typing import Optional
import shutil

import aiosqlite
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== DATABASE =====
DB_PATH = "bot_data.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
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
        await db.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                user_id INTEGER,
                timestamp TEXT,
                link TEXT,
                success INTEGER,
                error TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT
            )
        ''')
        await db.commit()

# ===== CONFIG HELPERS =====
async def get_config(key: str, default: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT value FROM config WHERE key = ?', (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_config(key: str, value: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)', (key, value))
        await db.commit()

async def get_cooldown() -> int:
    return await get_config("cooldown", DEFAULT_COOLDOWN)

async def get_auto_delete() -> int:
    return await get_config("auto_delete", DEFAULT_AUTO_DELETE)

# ===== USER HELPERS =====
async def update_user(user: dict):
    user_id = user["id"]
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO users (user_id, username, first_name, last_name, joined_at, last_activity, request_count)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                last_activity = excluded.last_activity,
                request_count = request_count + 1
        ''', (user_id, user.get("username"), user.get("first_name"), user.get("last_name"), now, now))
        await db.commit()

async def is_banned(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT is_banned FROM users WHERE user_id = ?', (user_id,)) as cursor:
            row = await cursor.fetchone()
            return bool(row[0]) if row else False

async def log_request(user_id: int, link: str, success: bool, error: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO requests (user_id, timestamp, link, success, error)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, datetime.now(timezone.utc).isoformat(), link, 1 if success else 0, error))
        await db.commit()

# ===== SESSION HELPERS =====
async def get_user_session(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT session_string FROM sessions WHERE user_id = ?', (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def save_user_session(user_id: int, session_string: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO sessions (user_id, session_string) VALUES (?, ?)', (user_id, session_string))
        await db.commit()

async def delete_user_session(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM sessions WHERE user_id = ?', (user_id,))
        await db.commit()

# ===== TELEGRAM CLIENT MANAGEMENT =====
clients = {}

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
    """Remove session and disconnect client."""
    if user_id in clients:
        await clients[user_id].disconnect()
        del clients[user_id]
    await delete_user_session(user_id)

# ===== COMMAND HANDLERS =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update_user(user.to_dict())
    await update.message.reply_text(
        "👋 **Welcome to the Channel Media Saver Bot!**\n\n"
        "This bot uses a user account to fetch messages from public channels.\n"
        "First, use /login to connect your Telegram account.\n\n"
        f"📦 **File limits:**\n"
        f"- ≤{DIRECT_LIMIT_MB} MB: sent directly\n"
        f"- {DIRECT_LIMIT_MB} MB – {MAX_DOWNLOAD_MB} MB: uploaded to cloud (temporary link)\n"
        f"- >{MAX_DOWNLOAD_MB} MB: rejected\n\n"
        "ℹ️ Use /help for full guide.",
        parse_mode="Markdown"
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cooldown = await get_cooldown()
    auto_delete = await get_auto_delete()
    await update.message.reply_text(
        f"📘 **GUIDE**\n\n"
        "1. Use /login to connect your Telegram account.\n"
        "2. We only ask to login for private channel/groups content. you must be a member of private channel/group to save content.\n"
        "3. Send any public Telegram message link.\n\n"
        f"⚠️ **Limits:**\n"
        f"- Cooldown: {cooldown} seconds between requests\n"
        "- Contact: @GamingHommie if you want personal bot.\n"
        f"- File size: ≤{DIRECT_LIMIT_MB} MB → sent via bot\n"
        f"- {DIRECT_LIMIT_MB} MB – {MAX_DOWNLOAD_MB} MB → uploaded to cloud\n"
        f"- >{MAX_DOWNLOAD_MB} MB → rejected\n\n"
        "📌 **Commands:**\n"
        "/start /help /myinfo /login /logout /cancel\n\n"
        f"Messages auto‑delete after {auto_delete} seconds.",
        parse_mode="Markdown"
    )

async def myinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
            user = await cursor.fetchone()
    if not user:
        await update.message.reply_text("No data found. Please send a link first.")
        return
    info = (
        f"👤 **Your Info**\n"
        f"User ID: `{user_id}`\n"
        f"Username: @{user[1] or 'N/A'}\n"
        f"First Name: {user[2] or 'N/A'}\n"
        f"Requests: {user[6]}\n"
        f"Joined: {user[4]}\n"
        f"Last Activity: {user[5]}\n"
        f"Banned: {'Yes' if user[7] else 'No'}"
    )
    await update.message.reply_text(info, parse_mode="Markdown")

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log out the user (clear session and disconnect client)."""
    user_id = update.effective_user.id
    if user_id not in clients and not await get_user_session(user_id):
        await update.message.reply_text("ℹ️ You are not logged in.")
        return
    await logout_user(user_id)
    await update.message.reply_text("✅ You have been logged out. Your session is deleted.")

# ===== LOGIN CONVERSATION =====
PHONE, CODE, PASSWORD = range(3)

async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📱 Send your phone number with country code.\nExample: `+919999999999` This process is Secure, we don't use or save your login data. Only used to Save Restricted Content from private Channels/Groups.", parse_mode="Markdown")
    return PHONE

async def login_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    if not re.match(r'^\+\d{7,15}$', phone):
        await update.message.reply_text("❌ Invalid phone number. Please include country code, e.g., +919999999999 & Start Again /login")
        return ConversationHandler.END
    context.user_data["phone"] = phone

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        await client.send_code_request(phone)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send code: {str(e)} Start Again /login")
        return ConversationHandler.END

    context.user_data["client"] = client

    await update.message.reply_text(
        "🔢 Enter the OTP like: `1 2 3 4 5`\n(Spaces are MUST)",
        parse_mode="Markdown"
    )
    return CODE

async def login_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.replace(" ", "")
    if not code.isdigit():
        await update.message.reply_text("❌ Invalid OTP. Please enter numbers only (with spaces).")
        return ConversationHandler.END

    client = context.user_data["client"]
    user_id = update.effective_user.id

    try:
        await client.sign_in(context.user_data["phone"], code)
    except PhoneCodeInvalidError:
        await update.message.reply_text("❌ Invalid OTP. Please try again. click /login")
        return ConversationHandler.END
    except SessionPasswordNeededError:
        await update.message.reply_text("🔑 Enter your 2FA password:")
        return PASSWORD
    except Exception as e:
        await update.message.reply_text(f"❌ Login failed: {str(e)} Start Again : /login")
        return ConversationHandler.END

    session = client.session.save()
    await save_user_session(user_id, session)
    clients[user_id] = client
    await update.message.reply_text("✅ **Login successful!** You can now use the bot.", parse_mode="Markdown")
    return ConversationHandler.END

async def login_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text
    client = context.user_data["client"]
    user_id = update.effective_user.id

    try:
        await client.sign_in(password=password)
    except Exception as e:
        await update.message.reply_text(f"❌ 2FA login failed: {str(e)}")
        return ConversationHandler.END

    session = client.session.save()
    await save_user_session(user_id, session)
    clients[user_id] = client
    await update.message.reply_text("✅ **Login successful! Send any Link Now, You must be a member of Private Channel/Group to Save Content.**", parse_mode="Markdown")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Login cancelled.")
    return ConversationHandler.END

# ===== ADMIN COMMANDS =====
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            await update.message.reply_text("⛔ You are not authorized to use this command.")
            return
        return await func(update, context)
    return wrapper

@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT COUNT(*) FROM users') as cursor:
            total_users = (await cursor.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM users WHERE is_banned = 1') as cursor:
            banned_users = (await cursor.fetchone())[0]
        async with db.execute('SELECT COUNT(*) FROM requests') as cursor:
            total_requests = (await cursor.fetchone())[0]
        today = datetime.now(timezone.utc).date().isoformat()
        async with db.execute('SELECT COUNT(*) FROM requests WHERE date(timestamp) = ?', (today,)) as cursor:
            today_requests = (await cursor.fetchone())[0]
    cooldown = await get_cooldown()
    auto_delete = await get_auto_delete()
    msg = (
        f"📊 **Bot Statistics**\n"
        f"Total Users: {total_users}\n"
        f"Banned Users: {banned_users}\n"
        f"Total Requests: {total_requests}\n"
        f"Requests Today: {today_requests}\n"
        f"Cooldown: {cooldown} sec\n"
        f"Auto‑delete: {auto_delete} sec"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

@admin_only
async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page = 0
    if context.args:
        try:
            page = int(context.args[0]) - 1
            if page < 0:
                page = 0
        except ValueError:
            pass
    limit = 10
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            'SELECT user_id, username, request_count FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?',
            (limit, page * limit)
        ) as cursor:
            users = await cursor.fetchall()
    if not users:
        await update.message.reply_text("No users found.")
        return
    text = "**Users (latest first):**\n"
    for u in users:
        text += f"• `{u[0]}` - @{u[1] or 'N/A'} - {u[2]} reqs\n"
    text += f"\nPage {page+1}. Use `/users {page+2}` for next page."
    await update.message.reply_text(text, parse_mode="Markdown")

@admin_only
async def user_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /user <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
            user = await cursor.fetchone()
    if not user:
        await update.message.reply_text("User not found.")
        return
    info = (
        f"👤 **User Details**\n"
        f"User ID: `{user_id}`\n"
        f"Username: @{user[1] or 'N/A'}\n"
        f"First Name: {user[2] or 'N/A'}\n"
        f"Requests: {user[6]}\n"
        f"Joined: {user[4]}\n"
        f"Last Activity: {user[5]}\n"
        f"Banned: {'Yes' if user[7] else 'No'}"
    )
    await update.message.reply_text(info, parse_mode="Markdown")

@admin_only
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        await db.commit()
    await update.message.reply_text(f"✅ User {user_id} banned.")

@admin_only
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        await db.commit()
    await update.message.reply_text(f"✅ User {user_id} unbanned.")

@admin_only
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        msg = update.message.reply_to_message
        await update.message.reply_text("📢 Starting broadcast...")
        count = 0
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute('SELECT user_id FROM users WHERE is_banned = 0') as cursor:
                users = await cursor.fetchall()
        for (uid,) in users:
            try:
                await msg.copy(uid)
                count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {uid}: {e}")
        await update.message.reply_text(f"✅ Broadcast sent to {count} users.")
    else:
        if not context.args:
            await update.message.reply_text("Usage: /broadcast <message> or reply to a message with /broadcast")
            return
        text = " ".join(context.args)
        await update.message.reply_text("📢 Starting broadcast...")
        count = 0
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute('SELECT user_id FROM users WHERE is_banned = 0') as cursor:
                users = await cursor.fetchall()
        for (uid,) in users:
            try:
                await context.bot.send_message(uid, text)
                count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Failed to send to {uid}: {e}")
        await update.message.reply_text(f"✅ Broadcast sent to {count} users.")

@admin_only
async def set_cooldown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setcooldown <seconds>")
        return
    try:
        seconds = int(context.args[0])
        if seconds < 1:
            raise ValueError
        await set_config("cooldown", seconds)
        await update.message.reply_text(f"✅ Cooldown set to {seconds} seconds.")
    except ValueError:
        await update.message.reply_text("Invalid number. Please provide a positive integer.")

@admin_only
async def set_autodelete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /setautodelete <seconds>")
        return
    try:
        seconds = int(context.args[0])
        if seconds < 1:
            raise ValueError
        await set_config("auto_delete", seconds)
        await update.message.reply_text(f"✅ Auto‑delete set to {seconds} seconds.")
    except ValueError:
        await update.message.reply_text("Invalid number. Please provide a positive integer.")

# ===== LINK HANDLER =====
last_used = {}
async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text

    if "t.me" not in text:
        await update.message.reply_text("❌ Please send a valid Telegram message link.")
        return

    if await is_banned(user_id):
        await update.message.reply_text("⛔ You have been banned from using this bot.")
        return

    await update_user(update.effective_user.to_dict())

    # Cooldown
    if user_id not in ADMIN_IDS:
        cooldown = await get_cooldown()
        now = time.time()
        if user_id in last_used and now - last_used[user_id] < cooldown:
            remaining = int(cooldown - (now - last_used[user_id]))
            await update.message.reply_text(f"⏳ Please wait {remaining} seconds.")
            return
        last_used[user_id] = now

    # Get user's Telethon client
    client = await get_client(user_id)
    if not client:
        await update.message.reply_text("⚠️ You need to login first. Use /login")
        return

    # Parse link
    match = re.search(r'https?://t\.me/(?:c/)?([^/]+)/(\d+)', text)
    if not match:
        await update.message.reply_text("❌ Invalid link format. Use 'Copy Message Link'.")
        await log_request(user_id, text, False, "Invalid link format")
        return

    chat_part = match.group(1)
    msg_id = int(match.group(2))

    # Resolve entity – this is quick
    try:
        if chat_part.isdigit():
            entity = await client.get_entity(int(f"-100{chat_part}"))
        else:
            entity = await client.get_entity(chat_part)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to resolve channel: {str(e)}")
        await log_request(user_id, text, False, str(e))
        return

    # Send a "Processing" message
    progress_msg = await update.message.reply_text("📥 Starting...")

    # Spawn background task
    asyncio.create_task(process_message(
        update, context, user_id, text, client, entity, msg_id, progress_msg
    ))


async def process_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
    user_id: int, link: str, client, entity, msg_id: int, progress_msg
):
    """Background task to fetch and deliver the message."""
    try:
        message = await client.get_messages(entity, ids=msg_id)
        if not message:
            await progress_msg.edit_text("❌ Message not found.")
            await log_request(user_id, link, False, "Message not found")
            return

        # Text‑only message
        if message.text and not message.media:
            await progress_msg.delete()
            sent = await update.message.reply_text(message.text)
            auto_del = await get_auto_delete()
            asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
            await log_request(user_id, link, True)
            return

        # Media message
        if message.media:
            file_size = message.file.size if message.file else None
            if file_size:
                size_mb = file_size / (1024 * 1024)
                if size_mb > MAX_DOWNLOAD_MB:
                    await progress_msg.edit_text(
                        f"❌ File too large ({size_mb:.1f} MB).\nMax allowed: {MAX_DOWNLOAD_MB} MB."
                    )
                    await log_request(user_id, link, False, f"File too large: {size_mb} MB")
                    return

                # Use safe threshold: files larger than DIRECT_LIMIT_MB go to cloud
                if size_mb > DIRECT_LIMIT_MB:
                    # Check disk space
                    total, used, free = shutil.disk_usage("/")
                    if free < file_size * 2:
                        await progress_msg.edit_text("❌ Not enough disk space to process this file.")
                        await log_request(user_id, link, False, "Disk space error")
                        return

                    await progress_msg.edit_text(f"📥 Downloading large file ({size_mb:.1f} MB)...")
                    last_percent = -1
                    async def download_progress(current, total):
                        nonlocal last_percent
                        if total > 0:
                            percent = int(current * 100 / total)
                            if percent != last_percent:
                                last_percent = percent
                                await progress_msg.edit_text(f"📥 Downloading... {percent}%")
                    file_path = await client.download_media(message, progress_callback=download_progress)
                    await progress_msg.edit_text("📤 Uploading to cloud (gofile.io)...")

                    # ---- Upload to gofile.io (RELIABLE FIX) ----
                    try:
                        # Step 1: Get an upload server
                        server_resp = requests.get('https://api.gofile.io/servers', timeout=10)
                        if server_resp.status_code != 200:
                            raise Exception("Failed to get upload server")
                        server_data = server_resp.json()
                        if server_data.get('status') != 'ok':
                            raise Exception(f"Server API error: {server_data.get('error', 'Unknown')}")
                        # Pick the first available server
                        server = server_data['data']['servers'][0]['name']
                        upload_url = f'https://{server}.gofile.io/uploadFile'

                        # Step 2: Upload the file
                        with open(file_path, 'rb') as f:
                            response = requests.post(
                                upload_url,
                                files={'file': f},
                                timeout=300  # 5 minutes timeout
                            )

                        if response.status_code == 200:
                            data = response.json()
                            if data.get('status') == 'ok':
                                # Get the download link
                                download_link = data['data']['downloadPage']
                                direct_link = data['data'].get('directLink')
                                if direct_link:
                                    download_link = direct_link
                                await progress_msg.delete()
                                sent = await update.message.reply_text(
                                    f"✅ File uploaded to cloud:\n{download_link}\n\n"
                                    "⚠️ Note: The file will be deleted after 7 days of inactivity or if not downloaded."
                                )
                                await log_request(user_id, link, True)
                                asyncio.create_task(delete_file_after(file_path, 60))
                                auto_del = await get_auto_delete()
                                asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                                return
                            else:
                                error_msg = data.get('error', 'Unknown error')
                                await progress_msg.edit_text(f"❌ Upload failed: {error_msg}")
                        else:
                            await progress_msg.edit_text(f"❌ Upload failed: HTTP {response.status_code}")
                        await log_request(user_id, link, False, f"Upload failed: HTTP {response.status_code}")
                    except Exception as e:
                        await progress_msg.edit_text(f"❌ Upload failed: {str(e)}")
                        await log_request(user_id, link, False, f"Upload failed: {str(e)}")
                        asyncio.create_task(delete_file_after(file_path, 60))
                        return
                    # ---- end gofile.io upload ----

                else:
                    # Direct send via Telegram (≤ DIRECT_LIMIT_MB)
                    await progress_msg.edit_text(f"📥 Downloading {size_mb:.1f} MB file...")
                    last_percent = -1
                    async def download_progress(current, total):
                        nonlocal last_percent
                        if total > 0:
                            percent = int(current * 100 / total)
                            if percent != last_percent:
                                last_percent = percent
                                await progress_msg.edit_text(f"📥 Downloading... {percent}%")
                    file_path = await client.download_media(message, progress_callback=download_progress)
                    await progress_msg.edit_text("📤 Uploading to Telegram...")
                    try:
                        with open(file_path, "rb") as f:
                            if message.audio:
                                sent = await update.message.reply_audio(f, caption=message.text if message.text else None)
                            elif message.video:
                                sent = await update.message.reply_video(f, caption=message.text if message.text else None)
                            elif message.photo:
                                sent = await update.message.reply_photo(f, caption=message.text if message.text else None)
                            else:
                                sent = await update.message.reply_document(f, caption=message.text if message.text else None)
                        asyncio.create_task(delete_file_after(file_path, 60))
                        auto_del = await get_auto_delete()
                        asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                        await progress_msg.delete()
                        await log_request(user_id, link, True)
                    except Exception as e:
                        # If direct send fails (e.g., timeout), fallback to cloud upload
                        logger.warning(f"Direct send failed: {e}. Falling back to cloud upload.")
                        await progress_msg.edit_text("Direct send failed, retrying with cloud upload...")
                        # Re‑upload the same file to cloud
                        try:
                            with open(file_path, 'rb') as f:
                                # Step 1: Get an upload server
                                server_resp = requests.get('https://api.gofile.io/servers', timeout=10)
                                if server_resp.status_code != 200:
                                    raise Exception("Failed to get upload server")
                                server_data = server_resp.json()
                                if server_data.get('status') != 'ok':
                                    raise Exception(f"Server API error: {server_data.get('error', 'Unknown')}")
                                server = server_data['data']['servers'][0]['name']
                                upload_url = f'https://{server}.gofile.io/uploadFile'

                                # Step 2: Upload the file
                                response = requests.post(
                                    upload_url,
                                    files={'file': f},
                                    timeout=300
                                )
                            if response.status_code == 200:
                                data = response.json()
                                if data.get('status') == 'ok':
                                    download_link = data['data']['downloadPage']
                                    direct_link = data['data'].get('directLink')
                                    if direct_link:
                                        download_link = direct_link
                                    await progress_msg.delete()
                                    sent = await update.message.reply_text(
                                        f"✅ File uploaded to cloud (fallback):\n{download_link}\n\n"
                                        "⚠️ Note: The file will be deleted after 7 days of inactivity."
                                    )
                                    await log_request(user_id, link, True)
                                    asyncio.create_task(delete_file_after(file_path, 60))
                                    auto_del = await get_auto_delete()
                                    asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                                    return
                            raise Exception("Cloud upload also failed")
                        except Exception as cloud_e:
                            await progress_msg.edit_text(f"❌ Both direct and cloud upload failed: {cloud_e}")
                            await log_request(user_id, link, False, str(cloud_e))
                            asyncio.create_task(delete_file_after(file_path, 60))
                    return
            else:
                # No file size info – download and send normally (assume small)
                file_path = await client.download_media(message)
                await progress_msg.edit_text("📤 Uploading...")
                with open(file_path, "rb") as f:
                    if message.audio:
                        sent = await update.message.reply_audio(f, caption=message.text if message.text else None)
                    elif message.video:
                        sent = await update.message.reply_video(f, caption=message.text if message.text else None)
                    elif message.photo:
                        sent = await update.message.reply_photo(f, caption=message.text if message.text else None)
                    else:
                        sent = await update.message.reply_document(f, caption=message.text if message.text else None)
                asyncio.create_task(delete_file_after(file_path, 60))
                auto_del = await get_auto_delete()
                asyncio.create_task(auto_delete(context, sent.chat_id, sent.message_id))
                await progress_msg.delete()
                await log_request(user_id, link, True)
                return
    except Exception as e:
        logger.exception("Error processing message")
        await progress_msg.edit_text(f"❌ Error: {str(e)}")
        await log_request(user_id, link, False, str(e))

async def delete_file_after(file_path, delay):
    await asyncio.sleep(delay)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception:
        pass

async def auto_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg_id: int):
    await asyncio.sleep(await get_auto_delete())
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception:
        pass

# ===== MAIN =====
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(init_db())

    app = Application.builder().token(BOT_TOKEN).build()

    # User commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("myinfo", myinfo))
    app.add_handler(CommandHandler("logout", logout))

    # Login conversation
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

    # Admin commands
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("users", users_list))
    app.add_handler(CommandHandler("user", user_details))
    app.add_handler(CommandHandler("ban", ban_user))
    app.add_handler(CommandHandler("unban", unban_user))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("setcooldown", set_cooldown))
    app.add_handler(CommandHandler("setautodelete", set_autodelete))

    # Link handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))

    # Determine run mode
    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        port = int(os.environ.get("PORT", 8080))
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            webhook_url=f"{webhook_url}/{BOT_TOKEN}",
            url_path=BOT_TOKEN
        )
    else:
        app.run_polling()

if __name__ == "__main__":
    main()
