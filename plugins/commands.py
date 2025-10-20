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
# Import the specific db functions needed, ensure they are sync
from database.ia_filterdb import get_file_details, delete_files, db_count_documents, second_db_count_documents
from database.users_chats_db import db # Sync db object
from datetime import datetime, timedelta, timezone # Added timezone
import pytz # Added pytz
from info import (URL, BIN_CHANNEL, SECOND_FILES_DATABASE_URL, INDEX_CHANNELS, ADMINS,
                  IS_VERIFY, VERIFY_TUTORIAL, VERIFY_EXPIRE, SHORTLINK_API, SHORTLINK_URL,
                  DELETE_TIME, SUPPORT_LINK, UPDATES_LINK, LOG_CHANNEL, PICS, IS_STREAM,
                  PM_FILE_DELETE_TIME, BOT_ID) # Added BOT_ID
# Use the async wrappers from utils
from utils import (get_settings, get_size, is_subscribed, is_check_admin, get_shortlink,
                   get_verify_status, update_verify_status, save_group_settings, temp,
                   get_readable_time, get_wish, get_seconds) # Removed sync DB calls directly
# Import collections for cleanup command
from database.ia_filterdb import collection as primary_collection, second_collection
from hydrogram.errors import MessageNotModified, FloodWait # Import specific errors
import logging # Add logging

logger = logging.getLogger(__name__)

async def del_stk(s):
    await asyncio.sleep(3)
    try: await s.delete()
    except: pass # Ignore if already deleted

@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    loop = asyncio.get_running_loop() # Get loop for sync calls
    # Group Join Handling
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        # Wrap sync db.get_chat
        chat_status = await loop.run_in_executor(None, db.get_chat, message.chat.id) # Corrected call
        # Check if chat exists by looking at a known field or if it returns default
        # A better check might involve checking if the document exists directly
        chat_exists = await loop.run_in_executor(None, lambda: db.grp.find_one({'id': message.chat.id}) is not None)
        if not chat_exists:
            try:
                total = await client.get_chat_members_count(message.chat.id)
                username = f'@{message.chat.username}' if message.chat.username else 'ᴘʀɪᴠᴀᴛᴇ'
                await client.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(message.chat.title, message.chat.id, username, total))
                # Wrap sync db.add_chat
                await loop.run_in_executor(None, db.add_chat, message.chat.id, message.chat.title)
            except Exception as e:
                logger.error(f"Error logging/adding group {message.chat.id}: {e}")
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
    # Correct expiry check
    expire_time = verify_status.get('expire_time')
    is_expired = isinstance(expire_time, datetime) and datetime.now(timezone.utc) > expire_time.replace(tzinfo=timezone.utc)
    if verify_status.get('is_verified') and is_expired:
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

    if mc.startswith('settings'):
        try: _, group_id_str = mc.split("_", 1); group_id = int(group_id_str)
        except (ValueError, IndexError): return await message.reply("Invalid link.")
        if not await is_check_admin(client, group_id, user_id): return await message.reply("ɴᴏᴛ ᴀᴅᴍɪɴ.")
        try: btn = await get_grp_stg(group_id); chat = await client.get_chat(group_id); await message.reply(f"⚙️ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        except Exception as e: logger.error(f"PM settings link error {group_id}: {e}"); await message.reply("Error fetch settings."); return

    elif mc == 'inline_fsub':
        btn = await is_subscribed(client, message);
        if btn: await message.reply("❗ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ(s).", reply_markup=InlineKeyboardMarkup(btn))
        else: await message.reply("✅ ᴀʟʀᴇᴀᴅʏ sᴜʙsᴄʀɪʙᴇᴅ."); return

    elif mc.startswith('verify_'):
        try: _, token = mc.split("_", 1)
        except ValueError: return await message.reply("Invalid link.")
        verify_status = await get_verify_status(user_id);
        if verify_status.get('verify_token') != token: return await message.reply("❌ ᴛᴏᴋᴇɴ ɪɴᴠᴀʟɪᴅ/ᴇxᴘɪʀᴇᴅ.")
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=VERIFY_EXPIRE)
        await update_verify_status(user_id, is_verified=True, expire_time=expiry_time, verify_token="") # await
        link_to_get = verify_status.get("link", "")
        reply_markup = InlineKeyboardMarkup([[ InlineKeyboardButton("📌 ɢᴇᴛ ғɪʟᴇ", url=f'https://t.me/{temp.U_NAME}?start={link_to_get}') ]]) if link_to_get else None
        await message.reply(f"✅ ᴠᴇʀɪғɪᴇᴅ!\n\nᴇxᴘɪʀᴇs: {expiry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}", reply_markup=reply_markup, protect_content=True); return

    # --- File Request Handling ---
    verify_status = await get_verify_status(user_id) # await
    expire_time = verify_status.get('expire_time') # Get expire time again after potential update
    is_expired = isinstance(expire_time, datetime) and datetime.now(timezone.utc) > expire_time.replace(tzinfo=timezone.utc)

    if IS_VERIFY and (not verify_status.get('is_verified') or is_expired):
        if is_expired: await update_verify_status(user_id, is_verified=False) # await
        token = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        # Store original file request param (`mc`) in link field
        await update_verify_status(user_id, verify_token=token, link="" if mc == 'inline_verify' else mc) # await
        try:
             # Need group ID to get settings, extract carefully
             grp_id_for_settings = None
             if mc.startswith(('file_', 'shortlink_', 'all_')):
                  parts = mc.split("_")
                  if len(parts) >= 2 and parts[1].lstrip('-').isdigit():
                       grp_id_for_settings = int(parts[1])

             if grp_id_for_settings:
                 settings = await get_settings(grp_id_for_settings) # await
                 short_url, short_api = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API)
                 tutorial = settings.get('tutorial', VERIFY_TUTORIAL)
             else: # Fallback if group ID couldn't be extracted
                  short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL

        except (IndexError, ValueError, TypeError): short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL

        verify_link = f'https://t.me/{temp.U_NAME}?start=verify_{token}'
        try: short_link = await get_shortlink(short_url, short_api, verify_link) # await
        except Exception as e: logger.error(f"Verify shortlink error: {e}"); short_link = verify_link
        btn_verify = [[ InlineKeyboardButton("🧿 ᴠᴇʀɪғʏ", url=short_link) ], [ InlineKeyboardButton('❓ ʜᴏᴡ ᴛᴏ', url=tutorial) ]]
        await message.reply("🔐 ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ!", reply_markup=InlineKeyboardMarkup(btn_verify), protect_content=True); return

    btn_fsub = await is_subscribed(client, message); # await
    if btn_fsub:
        btn_fsub.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
        await message.reply_photo(random.choice(PICS), caption=f"👋 {message.from_user.mention},\n\nᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ(s) 👇", reply_markup=InlineKeyboardMarkup(btn_fsub)); return

    try:
        if mc.startswith('all'):
            _, grp_id, key = mc.split("_", 2); grp_id = int(grp_id)
            files = temp.FILES.get(key)
            if not files: return await message.reply('❌ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ.')
            settings = await get_settings(grp_id); # await
            sent = []; total_msg = await message.reply(f"<b><i>🗂️ sᴇɴᴅɪɴɢ <code>{len(files)}</code> ғɪʟᴇs...</i></b>")
            for file in files:
                fid = file['_id']; cap = file.get('caption', '')
                CAPTION = settings.get('caption', script.FILE_CAPTION)
                try: f_cap = CAPTION.format(file_name=file.get('file_name','N/A'), file_size=get_size(file.get('file_size',0)), file_caption=cap)
                except Exception as e: logger.error(f"Caption format err {grp_id}: {e}"); f_cap = file.get('file_name','N/A')
                stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{fid}") ]] if IS_STREAM else []
                other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
                markup = InlineKeyboardMarkup(stream_btn + other_btns)
                try: msg = await client.send_cached_media(user_id, fid, caption=f_cap[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=markup); sent.append(msg.id); await asyncio.sleep(0.5)
                except FloodWait as e: logger.warning(f"Flood send {fid}: {e.value}s"); await asyncio.sleep(e.value); msg = await client.send_cached_media(user_id, fid, caption=f_cap[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=markup); sent.append(msg.id) # Retry
                except Exception as e: logger.error(f"Error send file {fid} to {user_id}: {e}")
            pm_del = PM_FILE_DELETE_TIME; time_r = get_readable_time(pm_del)
            info = await message.reply(f"⚠️ ɴᴏᴛᴇ: ғɪʟᴇs ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_r}</b>.", quote=True)
            await asyncio.sleep(pm_del);
            try: await client.delete_messages(user_id, sent + [total_msg.id])
            except Exception as e: logger.error(f"Error auto-del batch {user_id}: {e}")
            del_btns = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ᴀɢᴀɪɴ', callback_data=f"get_del_send_all_files#{grp_id}#{key}") ]]
            try: await info.edit("❗️ ғɪʟᴇs ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(del_btns))
            except: pass; return

        elif mc.startswith(('file_', 'shortlink_')):
            if mc.startswith('file_'): type_, grp_id, file_id = mc.split("_", 2)
            else: type_, grp_id, file_id = mc.split("_", 2)
            grp_id = int(grp_id)
            settings = await get_settings(grp_id); # await
            files_ = await get_file_details(file_id); # await
            if not files_: return await message.reply('❌ ɴᴏ ғɪʟᴇ.')
            file_doc = files_[0] if isinstance(files_, list) and files_ else None
            if not file_doc: return await message.reply('❌ ᴇʀʀᴏʀ ɢᴇᴛ ᴅᴇᴛᴀɪʟs.')
            if type_ != 'shortlink' and settings.get('shortlink', False):
                s_url, s_api, tut = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API), settings.get('tutorial', TUTORIAL)
                o_link = f"https://t.me/{temp.U_NAME}?start=shortlink_{grp_id}_{file_id}"
                try: s_link = await get_shortlink(s_url, s_api, o_link) # await
                except Exception as e: logger.error(f"Shortlink file {file_id} error: {e}"); s_link = o_link
                s_btn = [[ InlineKeyboardButton("♻️ ɢᴇᴛ ғɪʟᴇ", url=s_link) ], [ InlineKeyboardButton("❓ ʜᴏᴡ ᴛᴏ", url=tut) ]]
                fname = file_doc.get('file_name', 'ғɪʟᴇ'); fsize = get_size(file_doc.get('file_size', 0))
                await message.reply(f"[{fsize}] {fname}\n\n👇", reply_markup=InlineKeyboardMarkup(s_btn), protect_content=True); return
            CAPTION = settings.get('caption', script.FILE_CAPTION); cap_txt = file_doc.get('caption', '')
            try: f_cap = CAPTION.format(file_name=file_doc.get('file_name','N/A'), file_size=get_size(file_doc.get('file_size',0)), file_caption=cap_txt)
            except Exception as e: logger.error(f"Caption format error {grp_id}: {e}"); f_cap = file_doc.get('file_name','N/A')
            stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]] if IS_STREAM else []
            other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
            markup = InlineKeyboardMarkup(stream_btn + other_btns)
            vp = None # Initialize vp
            try: vp = await client.send_cached_media(user_id, file_id, caption=f_cap[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=markup)
            except FloodWait as e: logger.warning(f"Flood send file {file_id}: {e.value}s"); await asyncio.sleep(e.value); vp = await client.send_cached_media(user_id, file_id, caption=f_cap[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=markup) # Retry
            except Exception as e: logger.error(f"Error send file {file_id} to {user_id}: {e}"); await message.reply("❌ ᴇʀʀᴏʀ sᴇɴᴅɪɴɢ."); return
            pm_del = PM_FILE_DELETE_TIME; time_r = get_readable_time(pm_del)
            msg_timer = await vp.reply(f"⚠️ ɴᴏᴛᴇ: ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_r}</b>.", quote=True) if vp else await message.reply(f"⚠️ ɴᴏᴛᴇ: ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_r}</b>.", quote=True)
            await asyncio.sleep(pm_del)
            del_btns = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ᴀɢᴀɪɴ', callback_data=f"get_del_file#{grp_id}#{file_id}") ]]
            try: await msg_timer.delete()
            except: pass
            # **** CORRECTED SYNTAX ****
            if vp:
                try:
                    await vp.delete()
                    logger.info(f"Auto-deleted file {file_id} user {user_id}")
                except Exception as e:
                    logger.error(f"Error auto-deleting file {vp.id}: {e}")
            # **** END CORRECTION ****
            try: await message.reply("❗️ ғɪʟᴇ ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(del_btns))
            except Exception as e: logger.warning(f"Could not send 'file gone' {user_id}: {e}"); return
        else: await message.reply("❓ ɪɴᴠᴀʟɪᴅ sᴛᴀʀᴛ.")
    except Exception as e: logger.error(f"Error processing start '{mc}': {e}", exc_info=True); await message.reply("❌ ᴇʀʀᴏʀ.")


@Client.on_message(filters.command('link'))
async def link_cmd(bot, message): # Renamed
    msg = message.reply_to_message
    if not msg: return await message.reply('⚠️ ʀᴇᴘʟʏ ᴛᴏ ᴍᴇᴅɪᴀ.')
    media = getattr(msg, msg.media.value, None) if msg.media else None
    if not media or not hasattr(media, 'file_id'): return await message.reply('⚠️ ᴜɴsᴜᴘᴘᴏʀᴛᴇᴅ.')
    try:
        if not IS_STREAM: return await message.reply('🖥️ sᴛʀᴇᴀᴍɪɴɢ ᴅɪsᴀʙʟᴇᴅ.')
        try: stream_msg = await bot.send_cached_media(BIN_CHANNEL, media.file_id)
        except Exception as e: logger.error(f"BIN_CHANNEL err: {e}"); return await message.reply("❌ ʙɪɴ ᴇʀʀᴏʀ.")
        watch = f"{URL}watch/{stream_msg.id}"; download = f"{URL}download/{stream_msg.id}"
        btn=[[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ", url=watch), InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download)], [ InlineKeyboardButton('❌ ᴄʟᴏsᴇ', callback_data='close_data') ]]
        await message.reply('✅ ʟɪɴᴋs:', reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e: logger.error(f"Link cmd error: {e}", exc_info=True); await message.reply('❌ ᴇʀʀᴏʀ.')

@Client.on_message(filters.command('index_channels') & filters.user(ADMINS))
async def channels_info_cmd(bot, message): # Renamed
    ids = INDEX_CHANNELS; text = '**ɪɴᴅᴇxᴇᴅ ᴄʜᴀɴɴᴇʟs:**\n\n'
    if not ids: return await message.reply("ɴᴏ ɪɴᴅᴇx ᴄʜᴀɴɴᴇʟs.")
    for id_ in ids:
        try: chat = await bot.get_chat(id_); text += f'{chat.title} (`{id_}`)\n'
        except Exception as e: logger.warning(f"Could not get {id_}: {e}"); text += f'Unknown (`{id_}`) - Err: {e}\n'
    await message.reply(text + f'\n**ᴛᴏᴛᴀʟ:** {len(ids)}')

@Client.on_message(filters.command('stats') & filters.user(ADMINS))
async def stats_cmd(bot, message): # Renamed
    loop = asyncio.get_running_loop()
    sts_msg = await message.reply("📊 ɢᴀᴛʜᴇʀɪɴɢ sᴛᴀᴛs...")

    async def get_stat_safe(func, *args):
        try: call_func = partial(func, *args) if args else func; return await loop.run_in_executor(None, call_func)
        except Exception as e: logger.error(f"Stat err {func.__name__}: {e}"); return "ᴇʀʀ"

    files = await get_stat_safe(db_count_documents)
    users = await get_stat_safe(db.total_users_count)
    chats = await get_stat_safe(db.total_chat_count)
    used_files_db_size_raw = await get_stat_safe(db.get_files_db_size)
    used_data_db_size_raw = await get_stat_safe(db.get_data_db_size)
    used_files_db_size = get_size(used_files_db_size_raw) if isinstance(used_files_db_size_raw, (int, float)) else used_files_db_size_raw
    used_data_db_size = get_size(used_data_db_size_raw) if isinstance(used_data_db_size_raw, (int, float)) else used_data_db_size_raw

    secnd_files = '-'; secnd_files_db_used_size = '-'
    # ** FIX: Check second_collection is not None **
    if SECOND_FILES_DATABASE_URL and second_collection is not None:
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
    settings = await get_settings(group_id) # Async wrapper
    btn = [[ InlineKeyboardButton('ɪᴍᴅʙ ᴛᴇᴍᴘʟᴀᴛᴇ', callback_data=f'imdb_setgs#{group_id}') ], [ InlineKeyboardButton('sʜᴏʀᴛʟɪɴᴋ', callback_data=f'shortlink_setgs#{group_id}') ], [ InlineKeyboardButton('ғɪʟᴇ ᴄᴀᴘᴛɪᴏɴ', callback_data=f'caption_setgs#{group_id}') ], [ InlineKeyboardButton('ᴡᴇʟᴄᴏᴍᴇ ᴍᴇssᴀɢᴇ', callback_data=f'welcome_setgs#{group_id}') ], [ InlineKeyboardButton('ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ', callback_data=f'tutorial_setgs#{group_id}') ], [ InlineKeyboardButton(f'ᴘᴏsᴛᴇʀ {"✅" if settings.get("imdb", IMDB) else "❌"}', callback_data=f'bool_setgs#imdb#{settings.get("imdb", IMDB)}#{group_id}') ], [ InlineKeyboardButton(f'sᴘᴇʟʟ ᴄʜᴇᴄᴋ {"✅" if settings.get("spell_check", SPELL_CHECK) else "❌"}', callback_data=f'bool_setgs#spell_check#{settings.get("spell_check", SPELL_CHECK)}#{group_id}') ], [ InlineKeyboardButton(f'ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ {"✅" if settings.get("auto_delete", AUTO_DELETE) else "❌"}', callback_data=f'bool_setgs#auto_delete#{settings.get("auto_delete", AUTO_DELETE)}#{group_id}') ], [ InlineKeyboardButton(f'ᴡᴇʟᴄᴏᴍᴇ {"✅" if settings.get("welcome", WELCOME) else "❌"}', callback_data=f'bool_setgs#welcome#{settings.get("welcome", WELCOME)}#{group_id}') ], [ InlineKeyboardButton(f'sʜᴏʀᴛʟɪɴᴋ {"✅" if settings.get("shortlink", SHORTLINK) else "❌"}', callback_data=f'bool_setgs#shortlink#{settings.get("shortlink", SHORTLINK)}#{group_id}') ], [ InlineKeyboardButton(f'ʀᴇsᴜʟᴛ ᴘᴀɢᴇ {"ʟɪɴᴋ" if settings.get("links", LINK_MODE) else "ʙᴜᴛᴛᴏɴ"}', callback_data=f'bool_setgs#links#{settings.get("links", LINK_MODE)}#{group_id}') ]]
    return btn

@Client.on_message(filters.command('settings'))
async def settings_cmd(client, message): # Renamed function
    group_id = message.chat.id
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, group_id, message.from_user.id): return await message.reply('ɴᴏᴛ ᴀᴅᴍɪɴ.')
        btn = [[ InlineKeyboardButton("ᴏᴘᴇɴ ʜᴇʀᴇ", callback_data='open_group_settings') ], [ InlineKeyboardButton("ᴏᴘᴇɴ ɪɴ ᴘᴍ", callback_data='open_pm_settings') ]]
        await message.reply('ᴏᴘᴇɴ sᴇᴛᴛɪɴɢs:', reply_markup=InlineKeyboardMarkup(btn))
    elif message.chat.type == enums.ChatType.PRIVATE:
        loop = asyncio.get_running_loop()
        cons = await loop.run_in_executor(None, db.get_connections, message.from_user.id) # Wrap sync
        if not cons: return await message.reply("ɴᴏ ɢʀᴏᴜᴘs! /connect ɪɴ ɢʀᴏᴜᴘ.")
        buttons = []
        for con_id in cons:
            try: chat = await client.get_chat(con_id); buttons.append([InlineKeyboardButton(text=chat.title, callback_data=f'back_setgs#{chat.id}')])
            except: pass
        await message.reply('sᴇʟᴇᴄᴛ ɢʀᴏᴜᴘ:', reply_markup=InlineKeyboardMarkup(buttons))

@Client.on_message(filters.command('connect'))
async def connect_cmd(client, message): # Renamed function
    loop = asyncio.get_running_loop()
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, message.chat.id, message.from_user.id): return await message.reply("ɴᴏᴛ ᴀᴅᴍɪɴ.")
        await loop.run_in_executor(None, db.add_connect, message.chat.id, message.from_user.id) # Wrap sync
        await message.reply('✅ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ ᴘᴍ.')
    elif message.chat.type == enums.ChatType.PRIVATE:
        if len(message.command) > 1:
            try: grp_id = int(message.command[1])
            except ValueError: return await message.reply("ɪɴᴠᴀʟɪᴅ ɪᴅ.")
            try:
                 if not await is_check_admin(client, grp_id, message.from_user.id): return await message.reply('ɴᴏᴛ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.')
                 chat = await client.get_chat(grp_id)
                 await loop.run_in_executor(None, db.add_connect, grp_id, message.from_user.id) # Wrap sync
                 await message.reply(f'✅ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ {chat.title}.')
            except Exception as e:
                 logger.error(f"Connect command error getting chat {grp_id}: {e}")
                 await message.reply("Could not connect to that group ID.")
        else: await message.reply('Usage: /connect group_id')

@Client.on_message(filters.command('delete') & filters.user(ADMINS))
async def delete_cmd(bot, message): # Renamed function
    try: query = message.text.split(" ", 1)[1]
    except: return await message.reply("Usage: /delete query")
    btn = [[ InlineKeyboardButton("ʏᴇs", callback_data=f"delete_{query}") ], [ InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_data") ]]
    await message.reply(f"ᴅᴇʟᴇᴛᴇ ᴀʟʟ ғɪʟᴇs ғᴏʀ `{query}`?", reply_markup=InlineKeyboardMarkup(btn))

@Client.on_message(filters.command('img_2_link') & filters.user(ADMINS))
async def img_2_link_cmd(bot, message): # Renamed function
    r = message.reply_to_message
    if not r or not r.photo: return await message.reply('ʀᴇᴘʟʏ ᴛᴏ ᴘʜᴏᴛᴏ.')
    txt = await message.reply("⏳"); path = await r.download()
    loop = asyncio.get_running_loop()
    try: link = await loop.run_in_executor(None, upload_image, path) # Wrap sync
    except Exception as e: logger.error(f"img_2_link upload err: {e}"); link = None
    finally: try: os.remove(path) except: pass
    if not link: return await txt.edit("Upload failed!")
    await txt.edit(f"<b>❤️ ʟɪɴᴋ:\n`{link}`</b>", disable_web_page_preview=True);

@Client.on_message(filters.command('ping') & filters.user(ADMINS))
async def ping_cmd(client, message): # Renamed function
    start = monotonic(); msg = await message.reply("👀"); end = monotonic()
    await msg.edit(f'<b>ᴘᴏɴɢ!\n{round((end - start) * 1000)} ms</b>')

@Client.on_message(filters.command(['cleanmultdb', 'cleandb']) & filters.user(ADMINS))
async def clean_multi_db_duplicates(bot, message):
    # ** FIX: Check second_collection is not None **
    if not SECOND_FILES_DATABASE_URL or second_collection is None:
        return await message.reply("⚠️ sᴇᴄᴏɴᴅᴀʀʏ ᴅʙ ɴᴏᴛ ᴄᴏɴғɪɢᴜʀᴇᴅ.")

    sts_msg = await message.reply("🧹 sᴛᴀʀᴛɪɴɢ ᴄʟᴇᴀɴᴜᴘ...")
    loop = asyncio.get_running_loop()
    removed = 0; checked = 0; errors = 0; start = time_now()

    try:
        logger.info("Fetching primary IDs for cleanup...")
        # ** FIX: Wrap find call **
        primary_cursor = await loop.run_in_executor(None, partial(primary_collection.find, {}, {'_id': 1}))
        primary_ids = await loop.run_in_executor(None, lambda: {doc['_id'] for doc in primary_cursor})
        primary_count = len(primary_ids)
        logger.info(f"Found {primary_count} primary IDs.")
        if primary_count == 0: return await sts_msg.edit("🧹 ᴘʀɪᴍᴀʀʏ ᴅʙ ᴇᴍᴘᴛʏ.")

        logger.info("Iterating secondary DB...")
        # ** FIX: Wrap find call **
        # ** FIX: Call find on collection, not database **
        secondary_cursor = await loop.run_in_executor(None, partial(second_collection.find, {}, {'_id': 1}))

        BATCH_SIZE = 1000
        def secondary_iterator(): # Generator runs within executor
            for doc in secondary_cursor: yield doc
        doc_generator = await loop.run_in_executor(None, secondary_iterator)

        ids_to_remove = []; last_update_time = time_now()

        for doc in doc_generator:
            checked += 1
            if doc['_id'] in primary_ids: ids_to_remove.append(doc['_id'])
            if len(ids_to_remove) >= BATCH_SIZE:
                try:
                    del_res = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove}}))
                    deleted_now = del_res.deleted_count; removed += deleted_now
                    logger.info(f"Removed {deleted_now} duplicates (Batch).")
                except Exception as del_e: logger.error(f"Error remove batch: {del_e}"); errors += len(ids_to_remove)
                ids_to_remove = [] # Reset batch
            current_time = time_now()
            if current_time - last_update_time > 15:
                 elapsed = get_readable_time(current_time - start)
                 try: await sts_msg.edit_text(f"🧹 ᴄʟᴇᴀɴɪɴɢ...\n~ ᴄʜᴋ:<code>{checked}</code>|ʀᴍᴠ:<code>{removed}</code>|ᴇʀʀ:<code>{errors}</code>\n~ ᴇʟᴀᴘ:<code>{elapsed}</code>")
                 except FloodWait as e: await asyncio.sleep(e.value)
                 except MessageNotModified: pass
                 last_update_time = current_time

        # Process final batch
        if ids_to_remove:
            try:
                del_res = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove}}))
                deleted_now = del_res.deleted_count; removed += deleted_now
                logger.info(f"Removed {deleted_now} duplicates (Final Batch).")
            except Exception as del_e: logger.error(f"Error remove final batch: {del_e}"); errors += len(ids_to_remove)

        elapsed = get_readable_time(time_now() - start)
        await sts_msg.edit_text(f"✅ ᴄʟᴇᴀɴᴜᴘ ᴄᴏᴍᴘʟᴇᴛᴇ!\n\nᴛᴏᴏᴋ:<code>{elapsed}</code>\nᴄʜᴋ:<code>{checked}</code>|ʀᴍᴠ:<code>{removed}</code>|ᴇʀʀ:<code>{errors}</code>")

    except Exception as e: logger.error(f"/cleanmultdb error: {e}", exc_info=True); await sts_msg.edit(f"❌ ᴇʀʀᴏʀ: {e}")

@Client.on_message(filters.command('set_fsub') & filters.user(ADMINS))
async def set_fsub_cmd(bot, message): # Renamed
    try: _, ids = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_fsub -100xxx -100xxx')
    title = ""; valid_ids = []
    for id_ in ids.split():
        try: chat = await bot.get_chat(int(id_)); title += f'{chat.title}\n'; valid_ids.append(id_.strip())
        except Exception as e: return await message.reply(f'ERROR {id_}: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'FORCE_SUB_CHANNELS', " ".join(valid_ids)) # Wrap sync
    await message.reply(f'✅ Added FSub:\n{title}')

@Client.on_message(filters.command('set_req_fsub') & filters.user(ADMINS))
async def set_req_fsub_cmd(bot, message): # Renamed
    try: _, id_ = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_req_fsub -100xxx')
    try: chat = await bot.get_chat(int(id_))
    except Exception as e: return await message.reply(f'ERROR {id_}: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'REQUEST_FORCE_SUB_CHANNELS', id_.strip()) # Wrap sync
    await message.reply(f'✅ Added Req FSub: {chat.title}')

@Client.on_message(filters.command('off_auto_filter') & filters.user(ADMINS))
async def off_auto_filter_cmd(bot, message): # Renamed
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', False); await message.reply('✅ Auto Filter OFF.') # Wrap sync

@Client.on_message(filters.command('on_auto_filter') & filters.user(ADMINS))
async def on_auto_filter_cmd(bot, message): # Renamed
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', True); await message.reply('✅ Auto Filter ON.') # Wrap sync

@Client.on_message(filters.command('off_pm_search') & filters.user(ADMINS))
async def off_pm_search_cmd(bot, message): # Renamed
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', False); await message.reply('✅ PM Search OFF.') # Wrap sync

@Client.on_message(filters.command('on_pm_search') & filters.user(ADMINS))
async def on_pm_search_cmd(bot, message): # Renamed
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', True); await message.reply('✅ PM Search ON.') # Wrap sync
