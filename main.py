import asyncio
import logging
import csv
import io
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, ChatAdminRequired, UserNotParticipant, PeerIdInvalid, UserPrivacyRestricted
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# Enable logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - Replace with your actual values
API_ID = "your_api_id"  # Get from https://my.telegram.org
API_HASH = "your_api_hash"  # Get from https://my.telegram.org
BOT_TOKEN = "your_bot_token"  # Get from @BotFather
OWNER_ID = 123456789  # Your Telegram user ID (integer)
MONGO_URI = "mongodb://localhost:27017/"  # Or your MongoDB Atlas URI

# Initialize MongoDB with error handling
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.admin.command('ping')
    logger.info("MongoDB connection established successfully.")
except ConnectionFailure:
    logger.error("Failed to connect to MongoDB. Exiting.")
    exit(1)

db = mongo_client["telegram_bot_db"]
active_setups_collection = db["active_setups"]  # Support multiple channels
added_bots_collection = db["added_bots"]  # Track added bots per channel
bot_logs_collection = db["bot_logs"]  # For logging actions
channel_configs_collection = db["channel_configs"]  # Channel-specific configs (e.g., max_bots)

# Initialize Pyrogram Client
app = Client("my_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Helper function to log actions
async def log_action(action: str, details: dict):
    log_entry = {
        "timestamp": datetime.utcnow(),
        "action": action,
        "details": details,
        "owner_id": OWNER_ID
    }
    bot_logs_collection.insert_one(log_entry)

# Helper function to get all setups for owner
async def get_all_setups():
    docs = list(active_setups_collection.find({"owner": OWNER_ID}))
    return {doc["channel_id"]: {"channel": doc["channel"], "post_link": doc["post_link"], "max_bots": doc.get("max_bots", 20)} for doc in docs}

# Helper function to get active setup (default to first if none selected)
async def get_active_setup():
    # Assume we store current_active in a separate doc or use latest
    doc = active_setups_collection.find_one({"owner": OWNER_ID, "is_active": True})
    if doc:
        return doc["channel"], doc["post_link"], doc["channel_id"], doc.get("max_bots", 20)
    # Fallback to first setup
    setups = await get_all_setups()
    if setups:
        first = list(setups.values())[0]
        return first["channel"], first["post_link"], list(setups.keys())[0], first["max_bots"]
    return None, None, None, 20

# Helper function to save setup (support multiple)
async def save_setup(channel_id: str, channel: str, post_link: str, max_bots: int = 20, is_active: bool = False):
    active_setups_collection.update_one(
        {"owner": OWNER_ID, "channel_id": channel_id},
        {"$set": {"channel": channel, "post_link": post_link, "max_bots": max_bots, "is_active": is_active, "updated_at": datetime.utcnow()}},
        upsert=True
    )
    if is_active:
        # Deactivate others
        active_setups_collection.update_many(
            {"owner": OWNER_ID, "channel_id": {"$ne": channel_id}},
            {"$set": {"is_active": False}}
        )
    await log_action("setup_saved", {"channel_id": channel_id, "channel": channel, "is_active": is_active})

# Helper function to set active channel
async def set_active_channel(channel_id: str):
    await save_setup(channel_id, "", "", is_active=True)  # Just toggle active

# Helper function to get added bots count for a channel
async def get_added_bots_count(channel_id: str):
    doc = added_bots_collection.find_one({"channel_id": channel_id})
    if doc:
        return len(doc["bots"])
    return 0

# Helper function to get added bots list for channel
async def get_added_bots_list(channel_id: str) -> List[Dict]:
    doc = added_bots_collection.find_one({"channel_id": channel_id})
    if doc and doc["bots"]:
        bots = []
        for bot_id in doc["bots"]:
            try:
                user = await app.get_users(bot_id)
                bots.append({"id": bot_id, "username": user.username, "first_name": user.first_name})
            except:
                bots.append({"id": bot_id, "username": "unknown", "first_name": "Unknown"})
        return bots
    return []

# Helper function to add bot to channel's added list
async def add_bot_to_channel(channel_id: str, bot_id: int, bot_username: str):
    added_bots_collection.update_one(
        {"channel_id": channel_id},
        {"$addToSet": {"bots": bot_id}, "$set": {"updated_at": datetime.utcnow(), "last_bot_username": bot_username}},
        upsert=True
    )

# Helper function to remove bot from channel
async def remove_bot_from_channel(channel_id: str, bot_id: int):
    result = added_bots_collection.update_one(
        {"channel_id": channel_id},
        {"$pull": {"bots": bot_id}, "$set": {"updated_at": datetime.utcnow()}}
    )
    return result.modified_count > 0

# Helper function to verify bot permissions
async def verify_bot_permissions(channel_id: str, bot_id: int) -> bool:
    try:
        member = await app.get_chat_member(channel_id, bot_id)
        return (member.status in ['administrator'] and
                member.privileges.can_manage_chat and
                member.privileges.can_delete_messages and
                member.privileges.can_promote_members)
    except:
        return False

# Advanced error handler decorator
def error_handler(func):
    async def wrapper(client, message_or_query, *args, **kwargs):
        try:
            return await func(client, message_or_query, *args, **kwargs)
        except FloodWait as e:
            logger.warning(f"Flood wait: {e.value} seconds")
            await asyncio.sleep(e.value)
            return await wrapper(client, message_or_query, *args, **kwargs)
        except (ChatAdminRequired, UserNotParticipant, UserPrivacyRestricted) as e:
            logger.error(f"Permission error: {e}")
            if isinstance(message_or_query, Message):
                await message_or_query.reply("❌ Bot lacks required admin permissions in the channel.")
            else:
                await message_or_query.message.edit("❌ Bot lacks required admin permissions in the channel.")
        except PeerIdInvalid:
            if isinstance(message_or_query, Message):
                await message_or_query.reply("❌ Invalid channel or bot username.")
            else:
                await message_or_query.message.edit("❌ Invalid channel or bot username.")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if isinstance(message_or_query, Message):
                await message_or_query.reply("❌ An unexpected error occurred. Please try again.")
            else:
                await message_or_query.message.edit("❌ An unexpected error occurred. Please try again.")
    return wrapper

# Handler for setting channel via forwarding (owner only, adds to multiple setups)
@app.on_message(filters.forwarded & filters.user(OWNER_ID))
@error_handler
async def set_channel_from_forward(client: Client, message: Message):
    if message.forward_from_chat and message.forward_from_chat.type in ['channel', 'supergroup']:
        chat = message.forward_from_chat
        channel_id = str(chat.id)
        if chat.username:
            channel_username = f"@{chat.username}"
            post_link = f"https://t.me/{chat.username}/{message.forward_from_message_id}"
        else:
            post_link = f"https://t.me/c/{abs(int(channel_id))}/{message.forward_from_message_id}"
            channel_username = channel_id
        
        await save_setup(channel_id, channel_username, post_link)
        await set_active_channel(channel_id)  # Set as active
        await message.reply(f"✅ Channel added/set as active: {channel_username}\nStored post link: {post_link}\n\nNow you can add bots!")
        await log_action("channel_set_forward", {"channel_id": channel_id, "channel": channel_username})
    else:
        await message.reply("❌ Forward a message from a channel/supergroup to add/set it.")

# Command for owner to add channel manually
@app.on_message(filters.command("addchannel") & filters.user(OWNER_ID))
@error_handler
async def add_channel_manual(client: Client, message: Message):
    if len(message.command) < 3:
        await message.reply("Usage: /addchannel @channel_username <post_message_id> [max_bots=20]\nExample: /addchannel @mychannel 123 15")
        return
    
    channel_username = message.command[1]
    try:
        post_id = int(message.command[2])
        post_link = f"https://t.me/{channel_username.replace('@', '')}/{post_id}"
        max_bots = int(message.command[3]) if len(message.command) > 3 else 20
    except ValueError:
        await message.reply("❌ Invalid post ID or max_bots. Must be numbers.")
        return
    
    channel_id = channel_username if channel_username.startswith('-') else None
    if not channel_id:
        try:
            chat = await client.get_chat(channel_username)
            channel_id = str(chat.id)
        except:
            await message.reply("❌ Invalid channel or no access.")
            return
    
    await save_setup(channel_id, channel_username, post_link, max_bots)
    await set_active_channel(channel_id)
    await message.reply(f"✅ Channel added and set active: {channel_username}\nMax bots: {max_bots}\nStored post: {post_link}")
    await log_action("channel_added_manual", {"channel_id": channel_id, "channel": channel_username})

# Command to switch active channel /switchchannel <channel_id or name>
@app.on_message(filters.command("switchchannel") & filters.user(OWNER_ID))
@error_handler
async def switch_channel(client: Client, message: Message):
    if len(message.command) < 2:
        setups = await get_all_setups()
        if not setups:
            await message.reply("❌ No channels added yet. Use /addchannel or forward a message.")
            return
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"{v['channel']} ({k})", callback_data=f"switch_{k}")] for k, v in setups.items()])
        await message.reply("Select channel to switch:", reply_markup=kb)
        return
    
    channel_id = message.command[1]
    setups = await get_all_setups()
    if channel_id in setups:
        await set_active_channel(channel_id)
        await message.reply(f"✅ Switched to active channel: {setups[channel_id]['channel']}")
    else:
        await message.reply("❌ Channel not found.")

# Callback for switch channel
@app.on_callback_query(filters.regex(r"switch_") & filters.user(OWNER_ID))
@error_handler
async def switch_callback(client: Client, callback: CallbackQuery):
    channel_id = callback.data.split("_")[1]
    await set_active_channel(channel_id)
    await callback.message.edit(f"✅ Switched to active channel ID: {channel_id}")
    await callback.answer()

# Command for status /status
@app.on_message(filters.command("status") & filters.user(OWNER_ID))
@error_handler
async def status(client: Client, message: Message):
    setups = await get_all_setups()
    if not setups:
        await message.reply("❌ No channels set up yet.")
        return
    
    status_text = "📊 **All Channel Statuses**\n\n"
    for cid, setup in setups.items():
        count = await get_added_bots_count(cid)
        status_text += f"• {setup['channel']} ({cid})\n  Added: {count}/{setup['max_bots']}\n  Post: {setup['post_link'][:50]}...\n\n"
    
    status_text += f"• Active: {next((setup['channel'] for setup in setups.values()), 'None')}\n"
    status_text += f"• Total Logs: {bot_logs_collection.count_documents({})}"
    
    await message.reply(status_text)
    await log_action("status_viewed", {"channels_count": len(setups)})

# Command to list bots for active /listbots [channel_id]
@app.on_message(filters.command("listbots") & filters.user(OWNER_ID))
@error_handler
async def list_bots(client: Client, message: Message):
    channel_id, _, _, _ = await get_active_setup()
    if len(message.command) > 1:
        channel_id = message.command[1]
    
    if not channel_id:
        await message.reply("❌ No channel selected.")
        return
    
    bots = await get_added_bots_list(channel_id)
    if not bots:
        await message.reply("No bots added yet.")
        return
    
    list_text = f"🤖 **Bots for {channel_id}** ({len(bots)}):\n\n"
    for bot in bots:
        list_text += f"• @{bot['username'] or bot['first_name']} (ID: {bot['id']})\n"
    
    # Export to CSV button
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📤 Export CSV", callback_data=f"export_{channel_id}")]])
    await message.reply(list_text, reply_markup=kb)
    await log_action("bots_listed", {"channel_id": channel_id, "count": len(bots)})

# Callback for export CSV
@app.on_callback_query(filters.regex(r"export_") & filters.user(OWNER_ID))
@error_handler
async def export_csv(client: Client, callback: CallbackQuery):
    channel_id = callback.data.split("_")[1]
    bots = await get_added_bots_list(channel_id)
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Username", "First Name", "ID"])
    for bot in bots:
        writer.writerow([bot['username'] or '', bot['first_name'] or '', bot['id']])
    
    csv_content = output.getvalue()
    output.close()
    
    # Send as document
    await callback.message.reply_document(
        document=io.BytesIO(csv_content.encode('utf-8')),
        file_name=f"bots_{channel_id}.csv",
        caption=f"📊 Exported bots for {channel_id}"
    )
    await callback.message.edit("✅ CSV exported and sent!")
    await callback.answer()

# Command to remove bot /removebot @bot [channel_id]
@app.on_message(filters.command("removebot") & filters.user(OWNER_ID))
@error_handler
async def remove_bot(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Usage: /removebot @botusername [channel_id]")
        return
    
    bot_username = message.command[1]
    channel_id, _, _, _ = await get_active_setup()
    if len(message.command) > 2:
        channel_id = message.command[2]
    
    if not channel_id:
        await message.reply("❌ No channel selected.")
        return
    
    try:
        bot_user = await client.get_users(bot_username)
        bot_id = bot_user.id
    except:
        await message.reply("❌ Invalid bot username.")
        return
    
    removed = await remove_bot_from_channel(channel_id, bot_id)
    if removed:
        # Demote
        try:
            await client.promote_chat_member(
                chat_id=channel_id,
                user_id=bot_id,
                is_anonymous=False,
                can_manage_chat=False,
                can_delete_messages=False,
                can_manage_video_chats=False,
                can_restrict_members=False,
                can_promote_members=False,
                can_change_info=False,
                can_invite_users=False,
                can_pin_messages=False,
                can_post_stories=False
            )
            await message.reply(f"✅ Bot {bot_username} removed from {channel_id}.")
        except:
            await message.reply(f"✅ Bot {bot_username} removed from tracking, but demotion failed.")
        await log_action("bot_removed", {"channel_id": channel_id, "bot_id": bot_id})
    else:
        await message.reply("❌ Bot not found in added list.")

# Bulk add command /bulkadd <bot1,bot2,...> [channel_id]
@app.on_message(filters.command("bulkadd") & filters.user(OWNER_ID))
@error_handler
async def bulk_add_bots(client: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("Usage: /bulkadd @bot1,@bot2,@bot3 [channel_id]\nExample: /bulkadd @bot1,@bot2")
        return
    
    bot_list = [b.strip() for b in message.command[1].split(',')]
    channel_id, post_link, _, max_bots = await get_active_setup()
    if len(message.command) > 2:
        channel_id = message.command[2]
    
    if not channel_id:
        await message.reply("❌ No channel selected.")
        return
    
    current_count = await get_added_bots_count(channel_id)
    available_slots = max_bots - current_count
    if len(bot_list) > available_slots:
        await message.reply(f"❌ Only {available_slots} slots left. Max: {max_bots}")
        return
    
    success = 0
    failed = []
    for username in bot_list:
        if username.startswith('@'):
            username = username
        else:
            username = '@' + username
        
        try:
            user = await client.get_users(username)
            if not user.is_bot:
                failed.append(f"{username}: Not a bot")
                continue
            user_id = user.id
            
            # Check duplicate
            existing = await get_added_bots_list(channel_id)
            if any(b['id'] == user_id for b in existing):
                failed.append(f"{username}: Already added")
                continue
            
            # Promote
            await client.promote_chat_member(
                chat_id=channel_id,
                user_id=user_id,
                is_anonymous=False,
                can_manage_chat=True,
                can_delete_messages=True,
                can_manage_video_chats=True,
                can_restrict_members=True,
                can_promote_members=True,
                can_change_info=True,
                can_invite_users=True,
                can_pin_messages=True,
                can_post_stories=True
            )
            
            # Verify
            if not await verify_bot_permissions(channel_id, user_id):
                failed.append(f"{username}: Promotion failed (verify perms)")
                continue
            
            await add_bot_to_channel(channel_id, user_id, username)
            success += 1
            await log_action("bot_bulk_added", {"channel_id": channel_id, "bot_id": user_id, "bot_username": username})
            
        except Exception as e:
            failed.append(f"{username}: {str(e)[:50]}")
    
    # Notify owner
    if success > 0:
        await message.reply(f"✅ Bulk added {success} bots to {channel_id}.\nFailed: {len(failed)}\n\n🔗 Post: {post_link}")
        if failed:
            await message.reply("❌ Failures:\n" + "\n".join(failed))
    else:
        await message.reply("❌ No bots added successfully.")

# Handler for single bot add (non-owner can use if allowed, but here owner only for simplicity)
@app.on_message(filters.text & ~filters.command(["start", "status", "addchannel", "switchchannel", "listbots", "removebot", "bulkadd"]) & filters.user(OWNER_ID))
@error_handler
async def add_bot_as_admin(client: Client, message: Message):
    text = message.text.strip()
    if text.startswith('@'):
        username = text
    else:
        username = '@' + text
    
    channel, post_link, channel_id, max_bots = await get_active_setup()
    if not channel_id:
        await message.reply("❌ No active channel set. Use forward or /addchannel.")
        return
    
    count = await get_added_bots_count(channel_id)
    if count >= max_bots:
        await message.reply(f"❌ Channel full ({max_bots} bots). Use /bulkadd or remove some.")
        return
    
    try:
        user = await client.get_users(username)
        if not user.is_bot:
            await message.reply("❌ Not a bot.")
            return
        user_id = user.id
        
        # Duplicate check
        existing = await get_added_bots_list(channel_id)
        if any(b['id'] == user_id for b in existing):
            await message.reply("❌ Already added.")
            return
        
        # Promote
        await client.promote_chat_member(
            chat_id=channel_id,
            user_id=user_id,
            is_anonymous=False,
            can_manage_chat=True,
            can_delete_messages=True,
            can_manage_video_chats=True,
            can_restrict_members=True,
            can_promote_members=True,
            can_change_info=True,
            can_invite_users=True,
            can_pin_messages=True,
            can_post_stories=True
        )
        
        # Verify
        if await verify_bot_permissions(channel_id, user_id):
            await add_bot_to_channel(channel_id, user_id, username)
            reply = f"✅ Bot {username} added & verified as admin in {channel}!\n\n🔗 Post: {post_link}"
            await message.reply(reply)
            await log_action("bot_added", {"channel_id": channel_id, "bot_id": user_id, "bot_username": username})
        else:
            await message.reply("❌ Added but permissions not fully verified. Check manually.")
            
    except Exception as e:
        logger.error(f"Add bot error: {e}")
        await message.reply("❌ Failed to add bot. Details logged.")

# Command to clear old logs /clearlogs <days>
@app.on_message(filters.command("clearlogs") & filters.user(OWNER_ID))
async def clear_logs(client: Client, message: Message):
    days = int(message.command[1]) if len(message.command) > 1 else 30
    cutoff = datetime.utcnow() - timedelta(days=days)
    deleted = bot_logs_collection.delete_many({"timestamp": {"$lt": cutoff}}).deleted_count
    await message.reply(f"🧹 Cleared {deleted} old logs (> {days} days).")
    await log_action("logs_cleared", {"days": days, "deleted": deleted})

# Start command with menu
@app.on_message(filters.command("start"))
async def start(client: Client, message: Message):
    if message.from_user.id == OWNER_ID:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Status", callback_data="menu_status")],
            [InlineKeyboardButton("➕ Add Channel", callback_data="menu_addchannel")],
            [InlineKeyboardButton("🔄 Switch Channel", callback_data="menu_switch")],
            [InlineKeyboardButton("🤖 List Bots", callback_data="menu_listbots")],
            [InlineKeyboardButton("📤 Export CSV", callback_data="menu_export")]
        ])
        welcome = """🚀 **Ultra Advanced Bot Adder v2.0**

**New Features:**
• Multiple channels support with switching.
• Bulk add bots via /bulkadd.
• Permission verification after add.
• CSV export for bot lists.
• Log clearing & detailed status.

**Quick Start:**
1. Forward channel msg or /addchannel.
2. Send bot username or /bulkadd.
3. Use menu below!"""
        await message.reply(welcome, reply_markup=kb)
    else:
        await message.reply("👋 Contact owner for access.")

# Menu callbacks (simple for now)
@app.on_callback_query(filters.regex(r"menu_") & filters.user(OWNER_ID))
async def menu_callback(client: Client, callback: CallbackQuery):
    action = callback.data.split("_")[1]
    if action == "status":
        await status(client, callback.message)  # Reuse
    elif action == "addchannel":
        await callback.answer("Use /addchannel or forward a message.", show_alert=True)
    elif action == "switch":
        await switch_channel(client, callback.message)
    elif action == "listbots":
        await list_bots(client, callback.message)
    elif action == "export":
        await callback.answer("Use /listbots first for export.", show_alert=True)
    await callback.answer()

# Graceful shutdown
async def shutdown():
    logger.info("Shutting down...")
    mongo_client.close()
    await app.stop()

# Run the bot
if __name__ == "__main__":
    logger.info("Starting ultra advanced bot...")
    app.run()
