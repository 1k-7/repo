import os
import random
import string
import asyncio
from time import time as time_now
from time import monotonic
from functools import partial # Import partial
import datetime
from Script import script
from hydrogram import Client, filters, enums
from hydrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from database.ia_filterdb import db_count_documents, second_db_count_documents, get_file_details, delete_files
from database.users_chats_db import db
from datetime import datetime, timedelta, timezone # Added timezone
import pytz # Added pytz
# Removed IS_PREMIUM imports
from info import (URL, BIN_CHANNEL, SECOND_FILES_DATABASE_URL, INDEX_CHANNELS, ADMINS,
                  IS_VERIFY, VERIFY_TUTORIAL, VERIFY_EXPIRE, SHORTLINK_API, SHORTLINK_URL,
                  DELETE_TIME, SUPPORT_LINK, UPDATES_LINK, LOG_CHANNEL, PICS, IS_STREAM,
                  PM_FILE_DELETE_TIME, BOT_ID) # Added BOT_ID
from utils import (get_settings, get_size, is_subscribed, is_check_admin, get_shortlink,
                   get_verify_status, update_verify_status, save_group_settings, temp,
                   get_readable_time, get_wish, get_seconds) # Removed is_premium
# Import collections for cleanup command
from database.ia_filterdb import collection as primary_collection, second_collection
from hydrogram.errors import MessageNotModified, FloodWait # Import specific errors
import logging # Add logging

logger = logging.getLogger(__name__)

async def del_stk(s):
    await asyncio.sleep(3)
    await s.delete()

@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    loop = asyncio.get_running_loop() # Get loop for sync calls
    # Group Join Handling
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        # Wrap sync db.get_chat
        chat_exists = await loop.run_in_executor(None, db.get_chat, message.chat.id)
        if not chat_exists:
            try:
                total = await client.get_chat_members_count(message.chat.id)
                username = f'@{message.chat.username}' if message.chat.username else 'ᴘʀɪᴠᴀᴛᴇ'
                await client.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(message.chat.title, message.chat.id, username, total))
                # Wrap sync db.add_chat
                await loop.run_in_executor(None, db.add_chat, message.chat.id, message.chat.title)
            except Exception as e:
                logger.error(f"Error logging new group {message.chat.id}: {e}")
        wish = get_wish()
        user = message.from_user.mention if message.from_user else "ᴅᴇᴀʀ"
        btn = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK),
                 InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
        await message.reply(text=f"<b>ʜᴇʏ {user}, <i>{wish}</i>\nʜᴏᴡ ᴄᴀɴ ɪ ʜᴇʟᴘ ʏᴏᴜ?</b>", reply_markup=InlineKeyboardMarkup(btn))
        return

    # New User Handling in PM
    user_id = message.from_user.id
    # Wrap sync db.is_user_exist
    user_exists = await loop.run_in_executor(None, db.is_user_exist, user_id)
    if not user_exists:
        try:
             # Wrap sync db.add_user
             await loop.run_in_executor(None, db.add_user, user_id, message.from_user.first_name)
             await client.send_message(LOG_CHANNEL, script.NEW_USER_TXT.format(message.from_user.mention, user_id))
        except Exception as e:
             logger.error(f"Error adding new user {user_id}: {e}")

    # Verify Status Check (Uses async wrapper get_verify_status)
    verify_status = await get_verify_status(user_id)
    if verify_status['is_verified'] and isinstance(verify_status['expire_time'], datetime) and datetime.now(pytz.utc) > verify_status['expire_time'].replace(tzinfo=pytz.utc):
        logger.info(f"Verification expired for user {user_id}")
        await update_verify_status(user_id, is_verified=False) # Uses async wrapper

    # --- Start Parameter Handling ---
    if len(message.command) == 1 or message.command[1] == 'start':
        buttons = [[ InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ", url=f'http://t.me/{temp.U_NAME}?startgroup=start') ],
                   [ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ],
                   [ InlineKeyboardButton('❔ ʜᴇʟᴘ', callback_data='help'),
                     InlineKeyboardButton('🔍 ɪɴʟɪɴᴇ', switch_inline_query_current_chat=''),
                     InlineKeyboardButton('ℹ️ ᴀʙᴏᴜᴛ', callback_data='about') ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await message.reply_photo(
            photo=random.choice(PICS),
            caption=script.START_TXT.format(message.from_user.mention, get_wish()),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
        return

    mc = message.command[1] # Parameter

    # Removed premium command check
    
    if mc.startswith('settings'):
        try:
             _, group_id_str = mc.split("_", 1)
             group_id = int(group_id_str)
        except (ValueError, IndexError): return await message.reply("Invalid settings link.")
        if not await is_check_admin(client, group_id, user_id): return await message.reply("ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.")
        try:
             btn = await get_grp_stg(group_id) # Uses async get_settings wrapper
             chat = await client.get_chat(group_id)
             await message.reply(f"⚙️ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        except Exception as e: logger.error(f"Error opening settings via PM {group_id}: {e}"); await message.reply("Could not fetch settings.")
        return

    if mc == 'inline_fsub':
        btn = await is_subscribed(client, message) # Uses async wrapper
        if btn: await message.reply("❗ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ(s) ʙᴇʟᴏᴡ.", reply_markup=InlineKeyboardMarkup(btn))
        else: await message.reply("✅ ʏᴏᴜ ᴀʀᴇ ᴀʟʀᴇᴀᴅʏ sᴜʙsᴄʀɪʙᴇᴅ.")
        return

    if mc.startswith('verify_'):
        try: _, token = mc.split("_", 1)
        except ValueError: return await message.reply("Invalid verification link.")
        verify_status = await get_verify_status(user_id) # Async wrapper
        if verify_status['verify_token'] != token: return await message.reply("❌ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ᴛᴏᴋᴇɴ ɪs ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ.")
        expiry_time = datetime.now(pytz.utc) + timedelta(seconds=VERIFY_EXPIRE)
        await update_verify_status(user_id, is_verified=True, expire_time=expiry_time, verify_token="") # Async wrapper
        link_to_get = verify_status.get("link", "")
        reply_markup = InlineKeyboardMarkup([[ InlineKeyboardButton("📌 ɢᴇᴛ ғɪʟᴇ", url=f'https://t.me/{temp.U_NAME}?start={link_to_get}') ]]) if link_to_get else None
        await message.reply(f"✅ ᴠᴇʀɪғɪᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ!\n\nᴇxᴘɪʀᴇs: {expiry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
                           reply_markup=reply_markup, protect_content=True)
        return
    
    # --- File Request Handling ---
    verify_status = await get_verify_status(user_id) # Async wrapper
    is_expired = isinstance(verify_status['expire_time'], datetime) and datetime.now(pytz.utc) > verify_status['expire_time'].replace(tzinfo=pytz.utc)
    # Removed premium check
    if IS_VERIFY and (not verify_status['is_verified'] or is_expired):
        if is_expired: await update_verify_status(user_id, is_verified=False) # Async wrapper
        token = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        await update_verify_status(user_id, verify_token=token, link="" if mc == 'inline_verify' else mc) # Async wrapper
        try:
             settings = await get_settings(int(mc.split("_")[1])) # Async wrapper
             short_url, short_api = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API)
             tutorial = settings.get('tutorial', VERIFY_TUTORIAL)
        except (IndexError, ValueError, TypeError): short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL
        verify_link = f'https://t.me/{temp.U_NAME}?start=verify_{token}'
        try: short_link = await get_shortlink(short_url, short_api, verify_link) # Async
        except Exception as e: logger.error(f"Verify shortlink error: {e}"); short_link = verify_link
        btn_verify = [[ InlineKeyboardButton("🧿 ᴠᴇʀɪғʏ", url=short_link) ], [ InlineKeyboardButton('❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ', url=tutorial) ]]
        await message.reply("🔐 ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ!", reply_markup=InlineKeyboardMarkup(btn_verify), protect_content=True)
        return

    btn_fsub = await is_subscribed(client, message) # Async wrapper
    if btn_fsub:
        btn_fsub.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
        await message.reply_photo(photo=random.choice(PICS), caption=f"👋 {message.from_user.mention},\n\nᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴍʏ ᴄʜᴀɴɴᴇʟ(s) ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ. 👇",
            reply_markup=InlineKeyboardMarkup(btn_fsub), parse_mode=enums.ParseMode.HTML )
        return 
        
    try:
        if mc.startswith('all'):
            _, grp_id, key = mc.split("_", 2)
            grp_id = int(grp_id)
            files = temp.FILES.get(key)
            if not files: return await message.reply('❌ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ ᴏʀ ɪɴᴠᴀʟɪᴅ.')
            settings = await get_settings(grp_id) # Async wrapper
            sent_messages = []
            total_files_msg = await message.reply(f"<b><i>🗂️ sᴇɴᴅɪɴɢ <code>{len(files)}</code> ғɪʟᴇs...</i></b>")
            for file_doc in files:
                file_id = file_doc['_id']; caption_text = file_doc.get('caption', '')
                CAPTION = settings.get('caption', script.FILE_CAPTION)
                try: f_caption = CAPTION.format(file_name=file_doc.get('file_name','N/A'), file_size=get_size(file_doc.get('file_size',0)), file_caption=caption_text)
                except Exception as e: logger.error(f"Caption format error {grp_id}: {e}"); f_caption = file_doc.get('file_name','N/A')
                stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]] if IS_STREAM else []
                other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
                reply_markup = InlineKeyboardMarkup(stream_btn + other_btns)
                try:
                    msg_sent = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup)
                    sent_messages.append(msg_sent.id)
                    await asyncio.sleep(0.5)
                except FloodWait as e: logger.warning(f"FloodWait send file {file_id}: sleep {e.value}s"); await asyncio.sleep(e.value); msg_sent = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup); sent_messages.append(msg_sent.id) # Retry
                except Exception as e: logger.error(f"Error send file {file_id} to {user_id}: {e}")
            pm_delete_time = PM_FILE_DELETE_TIME; time_readable = get_readable_time(pm_delete_time)
            info_msg = await message.reply(f"⚠️ ɴᴏᴛᴇ: ғɪʟᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_readable}</b>.", quote=True)
            await asyncio.sleep(pm_delete_time)
            try: await client.delete_messages(chat_id=user_id, message_ids=sent_messages + [total_files_msg.id])
            except Exception as e: logger.error(f"Error auto-delete batch {user_id}: {e}")
            btns_after_del = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ғɪʟᴇs ᴀɢᴀɪɴ', callback_data=f"get_del_send_all_files#{grp_id}#{key}") ]]
            try: await info_msg.edit("❗️ ᴛʜᴇ ғɪʟᴇs ʜᴀᴠᴇ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(btns_after_del))
            except: pass
            return

        elif mc.startswith(('file_', 'shortlink_')):
            if mc.startswith('file_'): type_, grp_id, file_id = mc.split("_", 2)
            else: type_, grp_id, file_id = mc.split("_", 2)
            grp_id = int(grp_id)
            settings = await get_settings(grp_id) # Async wrapper
            files_ = await get_file_details(file_id) # Async wrapper
            if not files_: return await message.reply('❌ ɴᴏ sᴜᴄʜ ғɪʟᴇ ᴇxɪsᴛs.')
            file_doc = files_[0] if isinstance(files_, list) and files_ else None
            if not file_doc: return await message.reply('❌ ᴇʀʀᴏʀ ɢᴇᴛᴛɪɴɢ ғɪʟᴇ ᴅᴇᴛᴀɪʟs.')
            # Removed premium check for shortlink
            if type_ != 'shortlink' and settings.get('shortlink', False):
                short_url, short_api, tutorial = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API), settings.get('tutorial', TUTORIAL)
                original_link = f"https://t.me/{temp.U_NAME}?start=shortlink_{grp_id}_{file_id}"
                try: short_link = await get_shortlink(short_url, short_api, original_link)
                except Exception as e: logger.error(f"File shortlink error {file_id}: {e}"); short_link = original_link
                btn_short = [[ InlineKeyboardButton("♻️ ɢᴇᴛ ғɪʟᴇ", url=short_link) ], [ InlineKeyboardButton("❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ", url=tutorial) ]]
                file_name_display = file_doc.get('file_name', 'ғɪʟᴇ'); file_size_display = get_size(file_doc.get('file_size', 0))
                await message.reply(f"[{file_size_display}] {file_name_display}\n\nʏᴏᴜʀ ғɪʟᴇ ɪs ʀᴇᴀᴅʏ. 👇", reply_markup=InlineKeyboardMarkup(btn_short), protect_content=True)
                return
            CAPTION = settings.get('caption', script.FILE_CAPTION); caption_text = file_doc.get('caption', '')
            try: f_caption = CAPTION.format(file_name=file_doc.get('file_name','N/A'), file_size=get_size(file_doc.get('file_size',0)), file_caption=caption_text)
            except Exception as e: logger.error(f"Caption format error {grp_id}: {e}"); f_caption = file_doc.get('file_name','N/A')
            stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]] if IS_STREAM else []
            other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
            reply_markup = InlineKeyboardMarkup(stream_btn + other_btns)
            try:
                vp = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup)
            except FloodWait as e: logger.warning(f"FloodWait send file {file_id}: sleep {e.value}s"); await asyncio.sleep(e.value); vp = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup) # Retry
            except Exception as e: logger.error(f"Error send file {file_id} to {user_id}: {e}"); await message.reply("❌ sᴏʀʀʏ, ᴇʀʀᴏʀ sᴇɴᴅɪɴɢ ғɪʟᴇ."); return
            pm_delete_time = PM_FILE_DELETE_TIME; time_readable = get_readable_time(pm_delete_time)
            msg = await vp.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜɪs ғɪʟᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_readable}</b>.", quote=True)
            await asyncio.sleep(pm_delete_time)
            btns_after_del = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ғɪʟᴇ ᴀɢᴀɪɴ', callback_data=f"get_del_file#{grp_id}#{file_id}") ]]
            try: await msg.delete()
            except: pass
            try: await vp.delete(); logger.info(f"Auto-deleted file {file_id} for user {user_id}")
            except Exception as e: logger.error(f"Error auto-deleting file {vp.id}: {e}")
            try: await message.reply("❗️ ᴛʜᴇ ғɪʟᴇ ʜᴀs ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(btns_after_del))
            except Exception as final_reply_e: logger.warning(f"Could not send 'file gone' msg {user_id}: {final_reply_e}")
            return
        else: await message.reply("❓ ɪɴᴠᴀʟɪᴅ sᴛᴀʀᴛ ᴘᴀʀᴀᴍᴇᴛᴇʀ.")
    except Exception as e: logger.error(f"Error processing start '{mc}': {e}", exc_info=True); await message.reply("❌ ᴇʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ ʀᴇǫᴜᴇsᴛ.")

@Client.on_message(filters.command('link'))
async def link(bot, message):
    msg = message.reply_to_message
    if not msg: return await message.reply('⚠️ ᴘʟᴇᴀsᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇᴅɪᴀ ғɪʟᴇ.')
    media = getattr(msg, msg.media.value, None) if msg.media else None
    if not media or not hasattr(media, 'file_id'): return await message.reply('⚠️ ᴜɴsᴜᴘᴘᴏʀᴛᴇᴅ ғɪʟᴇ ᴛʏᴘᴇ.')
    try:
        if not IS_STREAM: return await message.reply('🖥️ sᴛʀᴇᴀᴍɪɴɢ ᴅɪsᴀʙʟᴇᴅ.')
        try: stream_msg = await bot.send_cached_media(chat_id=BIN_CHANNEL, file_id=media.file_id)
        except Exception as e: logger.error(f"BIN_CHANNEL send error: {e}"); return await message.reply("❌ ʙɪɴ ᴄʜᴀɴɴᴇʟ ᴇʀʀᴏʀ.")
        watch_url = f"{URL}watch/{stream_msg.id}"; download_url = f"{URL}download/{stream_msg.id}"
        btn=[[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ", url=watch_url), InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download_url)],
             [ InlineKeyboardButton('❌ ᴄʟᴏsᴇ', callback_data='close_data') ]]
        await message.reply('✅ ʟɪɴᴋs ɢᴇɴᴇʀᴀᴛᴇᴅ:', reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e: logger.error(f"Link command error: {e}", exc_info=True); await message.reply('❌ ᴇʀʀᴏʀ.')

@Client.on_message(filters.command('index_channels') & filters.user(ADMINS))
async def channels_info(bot, message):
    ids = INDEX_CHANNELS
    if not ids: return await message.reply("ɴᴏ ɪɴᴅᴇx ᴄʜᴀɴɴᴇʟs sᴇᴛ.")
    text = '**ɪɴᴅᴇxᴇᴅ ᴄʜᴀɴɴᴇʟs:**\n\n'
    for id_ in ids:
        try: chat = await bot.get_chat(id_)
        except Exception as e: logger.warning(f"Could not get chat {id_}: {e}"); text += f'Unknown Channel (ID: `{id_}`) - Error: {e}\n'; continue
        text += f'{chat.title} (`{id_}`)\n'
    text += f'\n**ᴛᴏᴛᴀʟ:** {len(ids)}'
    await message.reply(text)

@Client.on_message(filters.command('stats') & filters.user(ADMINS))
async def stats(bot, message):
    loop = asyncio.get_running_loop()
    sts_msg = await message.reply("📊 ɢᴀᴛʜᴇʀɪɴɢ sᴛᴀᴛs...")

    async def get_stat_safe(func, *args):
        try:
            call_func = partial(func, *args) if args else func
            result = await loop.run_in_executor(None, call_func)
            return result
        except Exception as e: logger.error(f"Stat error {func.__name__}: {e}"); return "ᴇʀʀ"

    files = await get_stat_safe(db_count_documents)
    users = await get_stat_safe(db.total_users_count)
    chats = await get_stat_safe(db.total_chat_count)
    # prm = await get_stat_safe(db.get_premium_count) # Removed
    used_files_db_size_raw = await get_stat_safe(db.get_files_db_size)
    used_data_db_size_raw = await get_stat_safe(db.get_data_db_size)
    used_files_db_size = get_size(used_files_db_size_raw) if isinstance(used_files_db_size_raw, (int, float)) else used_files_db_size_raw
    used_data_db_size = get_size(used_data_db_size_raw) if isinstance(used_data_db_size_raw, (int, float)) else used_data_db_size_raw

    secnd_files = '-'; secnd_files_db_used_size = '-'
    if SECOND_FILES_DATABASE_URL and second_collection:
        secnd_files = await get_stat_safe(second_db_count_documents)
        secnd_files_db_used_size_raw = await get_stat_safe(db.get_second_files_db_size)
        secnd_files_db_used_size = get_size(secnd_files_db_used_size_raw) if isinstance(secnd_files_db_used_size_raw, (int, float)) else secnd_files_db_used_size_raw

    uptime = get_readable_time(time_now() - temp.START_TIME)
    
    total_f = 0
    if isinstance(files, int): total_f += files
    if isinstance(secnd_files, int): total_f += secnd_files
    total_files_str = str(total_f) if (isinstance(files, int) and (secnd_files == '-' or isinstance(secnd_files, int))) else "ᴇʀʀ"

    await sts_msg.edit(script.STATUS_TXT.format(users, chats, used_data_db_size, total_files_str, files, used_files_db_size, secnd_files, secnd_files_db_used_size, uptime))    

async def get_grp_stg(group_id):
    settings = await get_settings(group_id) # Use async wrapper
    btn = [[ InlineKeyboardButton('ɪᴍᴅʙ ᴛᴇᴍᴘʟᴀᴛᴇ', callback_data=f'imdb_setgs#{group_id}') ],
           [ InlineKeyboardButton('sʜᴏʀᴛʟɪɴᴋ', callback_data=f'shortlink_setgs#{group_id}') ],
           [ InlineKeyboardButton('ғɪʟᴇ ᴄᴀᴘᴛɪᴏɴ', callback_data=f'caption_setgs#{group_id}') ],
           [ InlineKeyboardButton('ᴡᴇʟᴄᴏᴍᴇ ᴍᴇssᴀɢᴇ', callback_data=f'welcome_setgs#{group_id}') ],
           [ InlineKeyboardButton('ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ', callback_data=f'tutorial_setgs#{group_id}') ],
           [ InlineKeyboardButton(f'ᴘᴏsᴛᴇʀ {"✅" if settings.get("imdb", IMDB) else "❌"}', callback_data=f'bool_setgs#imdb#{settings.get("imdb", IMDB)}#{group_id}') ],
           [ InlineKeyboardButton(f'sᴘᴇʟʟ ᴄʜᴇᴄᴋ {"✅" if settings.get("spell_check", SPELL_CHECK) else "❌"}', callback_data=f'bool_setgs#spell_check#{settings.get("spell_check", SPELL_CHECK)}#{group_id}') ],
           [ InlineKeyboardButton(f'ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ {"✅" if settings.get("auto_delete", AUTO_DELETE) else "❌"}', callback_data=f'bool_setgs#auto_delete#{settings.get("auto_delete", AUTO_DELETE)}#{group_id}') ],
           [ InlineKeyboardButton(f'ᴡᴇʟᴄᴏᴍᴇ {"✅" if settings.get("welcome", WELCOME) else "❌"}', callback_data=f'bool_setgs#welcome#{settings.get("welcome", WELCOME)}#{group_id}') ],
           [ InlineKeyboardButton(f'sʜᴏʀᴛʟɪɴᴋ {"✅" if settings.get("shortlink", SHORTLINK) else "❌"}', callback_data=f'bool_setgs#shortlink#{settings.get("shortlink", SHORTLINK)}#{group_id}') ],
           [ InlineKeyboardButton(f'ʀᴇsᴜʟᴛ ᴘᴀɢᴇ {"ʟɪɴᴋ" if settings.get("links", LINK_MODE) else "ʙᴜᴛᴛᴏɴ"}', callback_data=f'bool_setgs#links#{settings.get("links", LINK_MODE)}#{group_id}') ]]
    return btn
    
@Client.on_message(filters.command('settings'))
async def settings(client, message):
    group_id = message.chat.id
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, group_id, message.from_user.id): return await message.reply('ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ.')
        btn = [[ InlineKeyboardButton("ᴏᴘᴇɴ ʜᴇʀᴇ", callback_data='open_group_settings') ],
               [ InlineKeyboardButton("ᴏᴘᴇɴ ɪɴ ᴘᴍ", callback_data='open_pm_settings') ]]
        await message.reply('ᴏᴘᴇɴ sᴇᴛᴛɪɴɢs ᴍᴇɴᴜ:', reply_markup=InlineKeyboardMarkup(btn))
    elif message.chat.type == enums.ChatType.PRIVATE:
        loop = asyncio.get_running_loop()
        cons = await loop.run_in_executor(None, db.get_connections, message.from_user.id) # Wrap sync
        if not cons: return await message.reply("ɴᴏ ɢʀᴏᴜᴘs ғᴏᴜɴᴅ! ᴜsᴇ /connect ɪɴ ᴀ ɢʀᴏᴜᴘ.")
        buttons = []
        for con_id in cons:
            try: chat = await client.get_chat(con_id); buttons.append([InlineKeyboardButton(text=chat.title, callback_data=f'back_setgs#{chat.id}')])
            except: pass # Ignore groups bot isn't in or access errors
        await message.reply('sᴇʟᴇᴄᴛ ɢʀᴏᴜᴘ ᴛᴏ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs:', reply_markup=InlineKeyboardMarkup(buttons))

@Client.on_message(filters.command('connect'))
async def connect(client, message):
    loop = asyncio.get_running_loop()
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, message.chat.id, message.from_user.id): return await message.reply("ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ.")
        await loop.run_in_executor(None, db.add_connect, message.chat.id, message.from_user.id) # Wrap sync
        await message.reply('✅ sᴜᴄᴄᴇssғᴜʟʟʏ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ ᴘᴍ.')
    elif message.chat.type == enums.ChatType.PRIVATE:
        if len(message.command) > 1:
            try: grp_id = int(message.command[1])
            except ValueError: return await message.reply("ɪɴᴠᴀʟɪᴅ ɢʀᴏᴜᴘ ɪᴅ.")
            if not await is_check_admin(client, grp_id, message.from_user.id): return await message.reply('ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.')
            chat = await client.get_chat(grp_id)
            await loop.run_in_executor(None, db.add_connect, grp_id, message.from_user.id) # Wrap sync
            await message.reply(f'✅ sᴜᴄᴄᴇssғᴜʟʟʏ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ {chat.title}.')
        else: await message.reply('Usage: /connect group_id\n(Get group ID from /id command in your group)')

@Client.on_message(filters.command('delete') & filters.user(ADMINS))
async def delete_file(bot, message):
    try: query = message.text.split(" ", 1)[1]
    except: return await message.reply("Command Incomplete!\nUsage: /delete query")
    btn = [[ InlineKeyboardButton("ʏᴇs", callback_data=f"delete_{query}") ], [ InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_data") ]]
    await message.reply_text(f"ᴅᴏ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴅᴇʟᴇᴛᴇ ᴀʟʟ ғɪʟᴇs ғᴏʀ `{query}` ?", reply_markup=InlineKeyboardMarkup(btn))
 
@Client.on_message(filters.command('img_2_link') & filters.user(ADMINS)) # Added admin filter for safety
async def img_2_link(bot, message):
    reply_to_message = message.reply_to_message
    if not reply_to_message or not reply_to_message.photo: return await message.reply('ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ.')
    text = await message.reply_text("ᴘʀᴏᴄᴇssɪɴɢ....")   
    path = await reply_to_message.download()
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(None, upload_image, path) # Wrap blocking call
        if not response: return await text.edit("Upload failed!")    
        await text.edit(f"<b>❤️ ʟɪɴᴋ:\n`{response}`</b>", disable_web_page_preview=True)
    except Exception as e: logger.error(f"img_2_link error: {e}"); await text.edit("An error occurred.")
    finally:
        try: os.remove(path) # Clean up file
        except: pass

@Client.on_message(filters.command('ping') & filters.user(ADMINS)) # Added admin filter
async def ping(client, message):
    start_time = monotonic()
    msg = await message.reply("👀")
    end_time = monotonic()
    await msg.edit(f'<b>ᴘᴏɴɢ!\n{round((end_time - start_time) * 1000)} ms</b>')
    
# Removed all premium commands: myplan, plan, add_prm, rm_prm, prm_list

@Client.on_message(filters.command('set_fsub') & filters.user(ADMINS))
async def set_fsub(bot, message):
    try: _, ids = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_fsub -100xxx -100xxx')
    title = ""
    for id_ in ids.split(' '):
        try: chat = await bot.get_chat(int(id_)); title += f'{chat.title}\n'
        except Exception as e: return await message.reply(f'ERROR fetching chat {id_}: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'FORCE_SUB_CHANNELS', ids) # Wrap sync
    await message.reply(f'✅ Added force subscribe channels:\n{title}')
        
@Client.on_message(filters.command('set_req_fsub') & filters.user(ADMINS))
async def set_req_fsub(bot, message):
    try: _, id_ = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_req_fsub -100xxx')
    try: chat = await bot.get_chat(int(id_))
    except Exception as e: return await message.reply(f'ERROR fetching chat {id_}: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'REQUEST_FORCE_SUB_CHANNELS', id_) # Wrap sync
    await message.reply(f'✅ Added request force subscribe channel: {chat.title}')

@Client.on_message(filters.command('off_auto_filter') & filters.user(ADMINS))
async def off_auto_filter(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', False) # Wrap sync
    await message.reply('✅ Turned off auto filter.')

@Client.on_message(filters.command('on_auto_filter') & filters.user(ADMINS))
async def on_auto_filter(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', True) # Wrap sync
    await message.reply('✅ Turned on auto filter.')

@Client.on_message(filters.command('off_pm_search') & filters.user(ADMINS))
async def off_pm_search(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', False) # Wrap sync
    await message.reply('✅ Turned off PM search.')

@Client.on_message(filters.command('on_pm_search') & filters.user(ADMINS))
async def on_pm_search(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', True) # Wrap sync
    await message.reply('✅ Turned on PM search.')
