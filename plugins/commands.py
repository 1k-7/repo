import os
import random
import string
import asyncio
from time import time as time_now
from time import monotonic
from functools import partial
from Script import script
from hydrogram import Client, filters, enums
from hydrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from database.ia_filterdb import get_file_details, delete_files, db_count_documents, second_db_count_documents
from database.users_chats_db import db
from datetime import datetime, timedelta, timezone
import pytz
from info import (URL, BIN_CHANNEL, SECOND_FILES_DATABASE_URL, INDEX_CHANNELS, ADMINS,
                  IS_VERIFY, VERIFY_TUTORIAL, VERIFY_EXPIRE, SHORTLINK_API, SHORTLINK_URL,
                  DELETE_TIME, SUPPORT_LINK, UPDATES_LINK, LOG_CHANNEL, PICS, IS_STREAM,
                  PM_FILE_DELETE_TIME, BOT_ID, PROTECT_CONTENT, TUTORIAL, # Added PROTECT_CONTENT and TUTORIAL
                  IMDB, SPELL_CHECK, AUTO_DELETE, WELCOME, SHORTLINK, LINK_MODE # Added group setting defaults
                  )
from utils import (get_settings, get_size, is_subscribed, is_check_admin, get_shortlink,
                   get_verify_status, update_verify_status, save_group_settings, temp,
                   get_readable_time, get_wish, get_seconds, upload_image)
from database.ia_filterdb import collection as primary_collection, second_collection
from hydrogram.errors import MessageNotModified, FloodWait
import logging

logger = logging.getLogger(__name__)

async def del_stk(s):
    await asyncio.sleep(3)
    try: await s.delete()
    except: pass

@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    loop = asyncio.get_running_loop()
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        # Check if group exists, add if not
        chat_exists = await loop.run_in_executor(None, lambda: db.grp.find_one({'id': message.chat.id}) is not None)
        if not chat_exists:
            try:
                total = await client.get_chat_members_count(message.chat.id)
                username = f'@{message.chat.username}' if message.chat.username else 'ᴘʀɪᴠᴀᴛᴇ'
                # Log new group
                await client.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(message.chat.title, message.chat.id, username, total))
                # Add group to DB
                await loop.run_in_executor(None, db.add_chat, message.chat.id, message.chat.title)
            except Exception as e:
                logger.error(f"Error logging/adding group {message.chat.id}: {e}")
        # Send welcome message in group
        wish = get_wish(); user = message.from_user.mention if message.from_user else "ᴅᴇᴀʀ"
        btn = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
        await message.reply(f"<b>ʜᴇʏ {user}, <i>{wish}</i>\nʜᴏᴡ ᴄᴀɴ ɪ ʜᴇʟᴘ ʏᴏᴜ?</b>", reply_markup=InlineKeyboardMarkup(btn)); return

    # --- PM Start Logic ---
    user_id = message.from_user.id
    # Check if user exists, add if not
    user_exists = await loop.run_in_executor(None, db.is_user_exist, user_id)
    if not user_exists:
        try:
            await loop.run_in_executor(None, db.add_user, user_id, message.from_user.first_name)
            await client.send_message(LOG_CHANNEL, script.NEW_USER_TXT.format(message.from_user.mention, user_id))
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")

    # Check and update verification status if expired
    verify_status = await get_verify_status(user_id)
    expire_time = verify_status.get('expire_time')
    is_expired = isinstance(expire_time, datetime) and datetime.now(timezone.utc) > expire_time.replace(tzinfo=timezone.utc)
    if verify_status.get('is_verified') and is_expired:
        logger.info(f"Verification expired user {user_id}"); await update_verify_status(user_id, is_verified=False)

    # Handle simple /start command
    if len(message.command) == 1 or message.command[1] == 'start':
        buttons = [[ InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ ᴛᴏ ʏᴏᴜʀ ɢʀᴏᴜᴘ", url=f'http://t.me/{temp.U_NAME}?startgroup=start') ], [ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ], [ InlineKeyboardButton('❔ ʜᴇʟᴘ', callback_data='help'), InlineKeyboardButton('🔍 ɪɴʟɪɴᴇ', switch_inline_query_current_chat=''), InlineKeyboardButton('ℹ️ ᴀʙᴏᴜᴛ', callback_data='about') ]]
        await message.reply_photo(random.choice(PICS), caption=script.START_TXT.format(message.from_user.mention, get_wish()), reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML); return

    # Handle deep links (start parameters)
    mc = message.command[1]

    # Handle settings deep link
    if mc.startswith('settings'):
        try: _, group_id_str = mc.split("_", 1); group_id = int(group_id_str)
        except (ValueError, IndexError): return await message.reply("Invalid settings link.")
        if not await is_check_admin(client, group_id, user_id): return await message.reply("You are not an admin in that group.")
        try:
            btn = await get_grp_stg(group_id)
            chat = await client.get_chat(group_id)
            await message.reply(f"⚙️ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        except Exception as e:
            logger.error(f"PM settings link error {group_id}: {e}"); await message.reply("Error fetching settings."); return

    # Handle inline force subscribe check deep link
    elif mc == 'inline_fsub':
        btn = await is_subscribed(client, message);
        if btn: await message.reply("❗ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ(s) ʙᴇʟᴏᴡ ᴛᴏ ᴜsᴇ ᴍᴇ.", reply_markup=InlineKeyboardMarkup(btn))
        else: await message.reply("✅ You are already subscribed."); return

    # Handle verification token deep link
    elif mc.startswith('verify_'):
        try: _, token = mc.split("_", 1)
        except ValueError: return await message.reply("Invalid verification link.")
        verify_status = await get_verify_status(user_id);
        if verify_status.get('verify_token') != token: return await message.reply("❌ ᴛᴏᴋᴇɴ ɪɴᴠᴀʟɪᴅ/ᴇxᴘɪʀᴇᴅ.")
        # Mark as verified and set expiry
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=VERIFY_EXPIRE)
        await update_verify_status(user_id, is_verified=True, expire_time=expiry_time, verify_token="")
        link_to_get = verify_status.get("link", "") # Get the original link they wanted
        reply_markup = InlineKeyboardMarkup([[ InlineKeyboardButton("📌 ɢᴇᴛ ғɪʟᴇ", url=f'https://t.me/{temp.U_NAME}?start={link_to_get}') ]]) if link_to_get else None
        await message.reply(f"✅ ᴠᴇʀɪғɪᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ!\n\nYour access expires on: {expiry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}", reply_markup=reply_markup, protect_content=True); return

    # --- Verification Check ---
    verify_status = await get_verify_status(user_id) # Re-fetch status
    expire_time = verify_status.get('expire_time')
    is_expired = isinstance(expire_time, datetime) and datetime.now(timezone.utc) > expire_time.replace(tzinfo=timezone.utc)
    if IS_VERIFY and (not verify_status.get('is_verified') or is_expired):
        if is_expired: await update_verify_status(user_id, is_verified=False) # Mark as not verified if expired
        token = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        # Store the original deep link (mc) the user was trying to access
        await update_verify_status(user_id, verify_token=token, link="" if mc == 'inline_verify' else mc)
        try:
             # Try to get group-specific shortlink/tutorial settings if applicable
             grp_id_for_settings = None
             if mc.startswith(('file_', 'shortlink_', 'all_')):
                  parts = mc.split("_")
                  if len(parts) >= 2 and parts[1].lstrip('-').isdigit(): grp_id_for_settings = int(parts[1])
             if grp_id_for_settings:
                 settings = await get_settings(grp_id_for_settings)
                 short_url, short_api, tutorial = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API), settings.get('tutorial', VERIFY_TUTORIAL)
             else: # Use global defaults
                 short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Error getting group settings for verification link ({mc}): {e}. Using defaults.")
            short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL

        verify_link = f'https://t.me/{temp.U_NAME}?start=verify_{token}'
        try: # Try to shorten the verification link
            short_link = await get_shortlink(short_url, short_api, verify_link)
        except Exception as e:
            logger.error(f"Verify shortlink error: {e}"); short_link = verify_link # Use original if shortening fails
        btn_verify = [[ InlineKeyboardButton("🧿 ᴠᴇʀɪғʏ ɴᴏᴡ", url=short_link) ], [ InlineKeyboardButton('❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ ʟɪɴᴋ', url=tutorial) ]]
        await message.reply("🔐 ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ!\n\nPlease verify by clicking the button below to continue.", reply_markup=InlineKeyboardMarkup(btn_verify), protect_content=True); return

    # --- Force Subscribe Check ---
    btn_fsub = await is_subscribed(client, message);
    if btn_fsub:
        btn_fsub.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
        await message.reply_photo(random.choice(PICS), caption=f"👋 Hey {message.from_user.mention},\n\nYou need to join the channel(s) below to get files 👇", reply_markup=InlineKeyboardMarkup(btn_fsub)); return

    # --- Process File/Batch Requests ---
    try:
        # Handle batch file request (/start all_...)
        if mc.startswith('all'):
            _, grp_id, key = mc.split("_", 2); grp_id = int(grp_id)
            files = temp.FILES.get(key)
            if not files: return await message.reply('❌ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ or invalid.')
            settings = await get_settings(grp_id);
            sent = []; total_msg = await message.reply(f"<b><i>🗂️ Sending <code>{len(files)}</code> files one by one... Please wait.</i></b>")
            for file in files:
                fid = file['_id']; cap = file.get('caption', '')
                CAPTION = settings.get('caption', script.FILE_CAPTION)
                try: f_cap = CAPTION.format(file_name=file.get('file_name','N/A'), file_size=get_size(file.get('file_size',0)), file_caption=cap)
                except Exception as e: logger.error(f"Caption format err {grp_id}: {e}"); f_cap = file.get('file_name','N/A')
                stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{fid}") ]] if IS_STREAM else []
                other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
                markup = InlineKeyboardMarkup(stream_btn + other_btns)
                try:
                    msg = await client.send_cached_media(
                        user_id,
                        fid,
                        caption=f_cap[:1024],
                        protect_content=settings.get('file_secure', PROTECT_CONTENT), # Use imported default
                        reply_markup=markup
                    )
                    sent.append(msg.id); await asyncio.sleep(0.5)
                except FloodWait as e:
                    logger.warning(f"Flood send batch file {fid}: {e.value}s"); await asyncio.sleep(e.value);
                    msg = await client.send_cached_media(
                        user_id,
                        fid,
                        caption=f_cap[:1024],
                        protect_content=settings.get('file_secure', PROTECT_CONTENT), # Use imported default
                        reply_markup=markup
                    )
                    sent.append(msg.id)
                except Exception as e: logger.error(f"Error send file {fid} to {user_id} (batch): {e}")
            # Auto-delete logic for batch
            pm_del = PM_FILE_DELETE_TIME; time_r = get_readable_time(pm_del)
            info = await message.reply(f"⚠️ ɴᴏᴛᴇ: These files will be deleted automatically after <b>{time_r}</b> to prevent copyright issues.", quote=True)
            await asyncio.sleep(pm_del);
            try: await client.delete_messages(user_id, sent + [total_msg.id]) # Delete sent files and the "Sending..." message
            except Exception as e: logger.error(f"Error auto-del batch {user_id}: {e}")
            del_btns = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ᴀɢᴀɪɴ', callback_data=f"get_del_send_all_files#{grp_id}#{key}") ]]
            try: await info.edit("❗️ ғɪʟᴇs ᴅᴇʟᴇᴛᴇᴅ due to copyright. Click below to get them again.", reply_markup=InlineKeyboardMarkup(del_btns))
            except: pass; return

        # Handle single file request (/start file_... or /start shortlink_...)
        elif mc.startswith(('file_', 'shortlink_')):
            type_, grp_id, file_id = mc.split("_", 2) # Handles both file_ and shortlink_
            grp_id = int(grp_id)
            settings = await get_settings(grp_id);
            files_ = await get_file_details(file_id);
            if not files_: return await message.reply('❌ No file found with that ID.')
            file_doc = files_[0] if isinstance(files_, list) and files_ else None
            if not file_doc: return await message.reply('❌ Error retrieving file details.')

            # Check if shortlink is enabled for this group and the link type isn't already 'shortlink'
            if type_ != 'shortlink' and settings.get('shortlink', SHORTLINK): # Use imported default
                s_url, s_api, tut = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API), settings.get('tutorial', TUTORIAL) # Use imported default for tutorial
                o_link = f"https://t.me/{temp.U_NAME}?start=shortlink_{grp_id}_{file_id}" # Link to bypass shortener check
                try: s_link = await get_shortlink(s_url, s_api, o_link)
                except Exception as e: logger.error(f"Shortlink file {file_id} error: {e}"); s_link = o_link
                s_btn = [[ InlineKeyboardButton("♻️ ɢᴇᴛ ғɪʟᴇ ʟɪɴᴋ", url=s_link) ], [ InlineKeyboardButton("❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ", url=tut) ]]
                fname = file_doc.get('file_name', 'ғɪʟᴇ'); fsize = get_size(file_doc.get('file_size', 0))
                await message.reply(f"[{fsize}] {fname}\n\n👇 Click the button below to get the file link.", reply_markup=InlineKeyboardMarkup(s_btn), protect_content=True); return

            # Proceed to send the file directly
            CAPTION = settings.get('caption', script.FILE_CAPTION); cap_txt = file_doc.get('caption', '')
            try: f_cap = CAPTION.format(file_name=file_doc.get('file_name','N/A'), file_size=get_size(file_doc.get('file_size',0)), file_caption=cap_txt)
            except Exception as e: logger.error(f"Caption format error {grp_id}: {e}"); f_cap = file_doc.get('file_name','N/A')
            stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]] if IS_STREAM else []
            other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
            markup = InlineKeyboardMarkup(stream_btn + other_btns)
            vp = None
            try:
                vp = await client.send_cached_media(
                    user_id,
                    file_id,
                    caption=f_cap[:1024],
                    protect_content=settings.get('file_secure', PROTECT_CONTENT), # Use imported default
                    reply_markup=markup
                )
            except FloodWait as e:
                logger.warning(f"Flood send file {file_id}: {e.value}s"); await asyncio.sleep(e.value);
                vp = await client.send_cached_media(
                    user_id,
                    file_id,
                    caption=f_cap[:1024],
                    protect_content=settings.get('file_secure', PROTECT_CONTENT), # Use imported default
                    reply_markup=markup
                )
            except Exception as e:
                logger.error(f"Error send file {file_id} to {user_id}: {e}") # Log the actual error
                await message.reply("❌ ᴇʀʀᴏʀ sᴇɴᴅɪɴɢ.") # User-friendly message
                return
            # Auto-delete logic for single file
            pm_del = PM_FILE_DELETE_TIME; time_r = get_readable_time(pm_del)
            msg_timer = await vp.reply(f"⚠️ ɴᴏᴛᴇ: This file will be deleted automatically after <b>{time_r}</b>.", quote=True) if vp else await message.reply(f"⚠️ ɴᴏᴛᴇ: This file will be deleted automatically after <b>{time_r}</b>.", quote=True)
            await asyncio.sleep(pm_del)
            del_btns = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ᴀɢᴀɪɴ', callback_data=f"get_del_file#{grp_id}#{file_id}") ]]
            try: await msg_timer.delete() # Delete the timer message
            except: pass
            if vp:
                try:
                    await vp.delete() # Delete the file message
                    logger.info(f"Auto-deleted file {file_id} user {user_id}")
                except Exception as e:
                    logger.error(f"Error auto-deleting file {vp.id}: {e}")
            try: await message.reply("❗️ ғɪʟᴇ ᴅᴇʟᴇᴛᴇᴅ due to copyright. Click below to get it again.", reply_markup=InlineKeyboardMarkup(del_btns))
            except Exception as e: logger.warning(f"Could not send 'file gone' {user_id}: {e}"); return
        else:
            await message.reply("❓ Invalid start command parameter.")
    except Exception as e:
        logger.error(f"Error processing start command '{mc}': {e}", exc_info=True)
        await message.reply("❌ An unexpected error occurred.")


@Client.on_message(filters.command('link'))
async def link_cmd(bot, message):
    msg = message.reply_to_message
    if not msg: return await message.reply('⚠️ Please reply to a media file to get stream/download links.')
    media = getattr(msg, msg.media.value, None) if msg.media else None
    if not media or not hasattr(media, 'file_id'): return await message.reply('⚠️ This message does not contain a supported media file.')
    try:
        if not IS_STREAM: return await message.reply('🖥️ Streaming is currently disabled.')
        try:
            stream_msg = await bot.send_cached_media(BIN_CHANNEL, media.file_id) # Cache in BIN_CHANNEL
        except Exception as e:
            logger.error(f"Error caching media to BIN_CHANNEL {BIN_CHANNEL}: {e}"); return await message.reply("❌ Error generating links. Could not access BIN channel.")
        watch = f"{URL}watch/{stream_msg.id}"; download = f"{URL}download/{stream_msg.id}"
        btn=[[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ", url=watch), InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download)], [ InlineKeyboardButton('❌ ᴄʟᴏsᴇ', callback_data='close_data') ]]
        await message.reply('✅ Links generated successfully:', reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e:
        logger.error(f"Link cmd error: {e}", exc_info=True); await message.reply('❌ An error occurred while generating links.')

@Client.on_message(filters.command('index_channels') & filters.user(ADMINS))
async def channels_info_cmd(bot, message):
    ids = INDEX_CHANNELS; text = '**ɪɴᴅᴇxᴇᴅ ᴄʜᴀɴɴᴇʟs:**\n\n'
    if not ids: return await message.reply("⚠️ No channels are configured for indexing.")
    for id_ in ids:
        try: chat = await bot.get_chat(id_); text += f' • {chat.title} (`{id_}`)\n'
        except Exception as e: logger.warning(f"Could not get chat info for {id_}: {e}"); text += f' • Unknown Channel (`{id_}`) - Error: {e}\n'
    await message.reply(text + f'\n**ᴛᴏᴛᴀʟ ɪɴᴅᴇx ᴄʜᴀɴɴᴇʟs:** {len(ids)}')

@Client.on_message(filters.command('stats') & filters.user(ADMINS))
async def stats_cmd(bot, message):
    loop = asyncio.get_running_loop()
    sts_msg = await message.reply("📊 Gathering bot statistics...")
    async def get_stat_safe(func, *args):
        try:
            call_func = partial(func, *args) if args else func
            return await loop.run_in_executor(None, call_func)
        except Exception as e:
            logger.error(f"Stat collection error ({func.__name__ if hasattr(func, '__name__') else 'unknown'}): {e}")
            return "ᴇʀʀ" # Return error string
    # Fetch stats concurrently where possible
    files, users, chats, used_files_db_size_raw, used_data_db_size_raw = await asyncio.gather(
        get_stat_safe(db_count_documents),
        get_stat_safe(db.total_users_count),
        get_stat_safe(db.total_chat_count),
        get_stat_safe(db.get_files_db_size),
        get_stat_safe(db.get_data_db_size)
    )
    # Format sizes
    used_files_db_size = get_size(used_files_db_size_raw) if isinstance(used_files_db_size_raw, (int, float)) else used_files_db_size_raw
    used_data_db_size = get_size(used_data_db_size_raw) if isinstance(used_data_db_size_raw, (int, float)) else used_data_db_size_raw
    # Fetch secondary DB stats if configured
    secnd_files = '-'; secnd_files_db_used_size = '-'
    if SECOND_FILES_DATABASE_URL and second_collection is not None:
        secnd_files, secnd_files_db_used_size_raw = await asyncio.gather(
            get_stat_safe(second_db_count_documents),
            get_stat_safe(db.get_second_files_db_size)
        )
        secnd_files_db_used_size = get_size(secnd_files_db_used_size_raw) if isinstance(secnd_files_db_used_size_raw, (int, float)) else secnd_files_db_used_size_raw
    # Calculate total files
    total_f = 0
    if isinstance(files, int): total_f += files
    if isinstance(secnd_files, int): total_f += secnd_files
    total_files_str = str(total_f) if (isinstance(files, int) and (secnd_files == '-' or isinstance(secnd_files, int))) else "ᴇʀʀ"
    # Get uptime
    uptime = get_readable_time(time_now() - temp.START_TIME)
    # Edit message with results
    await sts_msg.edit(script.STATUS_TXT.format(users, chats, used_data_db_size, total_files_str, files, used_files_db_size, secnd_files, secnd_files_db_used_size, uptime))

async def get_grp_stg(group_id):
    """Generates the settings inline keyboard for a group."""
    settings = await get_settings(group_id)
    # Define buttons using f-strings for status and callback data
    btn = [
        [InlineKeyboardButton('ɪᴍᴅʙ ᴛᴇᴍᴘʟᴀᴛᴇ', callback_data=f'imdb_setgs#{group_id}')],
        [InlineKeyboardButton('sʜᴏʀᴛʟɪɴᴋ sᴇᴛᴛɪɴɢs', callback_data=f'shortlink_setgs#{group_id}')],
        [InlineKeyboardButton('ғɪʟᴇ ᴄᴀᴘᴛɪᴏɴ', callback_data=f'caption_setgs#{group_id}')],
        [InlineKeyboardButton('ᴡᴇʟᴄᴏᴍᴇ ᴍᴇssᴀɢᴇ', callback_data=f'welcome_setgs#{group_id}')],
        [InlineKeyboardButton('ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ (ғᴏʀ sʜᴏʀᴛʟɪɴᴋ/ᴠᴇʀɪғʏ)', callback_data=f'tutorial_setgs#{group_id}')],
        # Boolean toggles
        [InlineKeyboardButton(f'ᴘᴏsᴛᴇʀ {"✅ Enabled" if settings.get("imdb", IMDB) else "❌ Disabled"}', callback_data=f'bool_setgs#imdb#{settings.get("imdb", IMDB)}#{group_id}')],
        [InlineKeyboardButton(f'sᴘᴇʟʟ ᴄʜᴇᴄᴋ {"✅ Enabled" if settings.get("spell_check", SPELL_CHECK) else "❌ Disabled"}', callback_data=f'bool_setgs#spell_check#{settings.get("spell_check", SPELL_CHECK)}#{group_id}')],
        [InlineKeyboardButton(f'ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ {"✅ Enabled" if settings.get("auto_delete", AUTO_DELETE) else "❌ Disabled"}', callback_data=f'bool_setgs#auto_delete#{settings.get("auto_delete", AUTO_DELETE)}#{group_id}')],
        [InlineKeyboardButton(f'ᴡᴇʟᴄᴏᴍᴇ ᴍsɢ {"✅ Enabled" if settings.get("welcome", WELCOME) else "❌ Disabled"}', callback_data=f'bool_setgs#welcome#{settings.get("welcome", WELCOME)}#{group_id}')],
        [InlineKeyboardButton(f'sʜᴏʀᴛʟɪɴᴋ (ғɪʟᴇ ᴀᴄᴄᴇss) {"✅ Enabled" if settings.get("shortlink", SHORTLINK) else "❌ Disabled"}', callback_data=f'bool_setgs#shortlink#{settings.get("shortlink", SHORTLINK)}#{group_id}')],
        [InlineKeyboardButton(f'ʀᴇsᴜʟᴛ ᴘᴀɢᴇ {"🔗 Link Mode" if settings.get("links", LINK_MODE) else "🔘 Button Mode"}', callback_data=f'bool_setgs#links#{settings.get("links", LINK_MODE)}#{group_id}')]
    ]
    return btn

@Client.on_message(filters.command('settings'))
async def settings_cmd(client, message):
    group_id = message.chat.id
    user_id = message.from_user.id
    # Handle /settings in groups
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, group_id, user_id): return await message.reply('❌ Only admins can manage group settings.')
        btn = [[ InlineKeyboardButton("🔧 ᴏᴘᴇɴ sᴇᴛᴛɪɴɢs ʜᴇʀᴇ", callback_data='open_group_settings') ], [ InlineKeyboardButton("🔒 ᴏᴘᴇɴ sᴇᴛᴛɪɴɢs ɪɴ ᴘᴍ", callback_data='open_pm_settings') ]]
        await message.reply('Choose where to open the settings menu:', reply_markup=InlineKeyboardMarkup(btn))
    # Handle /settings in PM
    elif message.chat.type == enums.ChatType.PRIVATE:
        loop = asyncio.get_running_loop()
        cons = await loop.run_in_executor(None, db.get_connections, user_id)
        if not cons: return await message.reply("You haven't connected to any groups yet! Use /connect in a group where you are an admin.")
        buttons = []
        for con_id in cons:
            try:
                chat = await client.get_chat(con_id)
                # Check if user is still admin in the connected group
                if await is_check_admin(client, con_id, user_id):
                    buttons.append([InlineKeyboardButton(text=chat.title, callback_data=f'back_setgs#{chat.id}')])
                else: # Remove connection if no longer admin
                    await loop.run_in_executor(None, db.del_connect, con_id, user_id)
            except Exception as e:
                logger.warning(f"Error getting chat {con_id} for PM settings: {e}")
                await loop.run_in_executor(None, db.del_connect, con_id, user_id) # Remove if chat inaccessible
        if not buttons: return await message.reply("You are no longer an admin in any connected groups. Use /connect in a group.")
        await message.reply('Select the group you want to manage settings for:', reply_markup=InlineKeyboardMarkup(buttons))

@Client.on_message(filters.command('connect'))
async def connect_cmd(client, message):
    loop = asyncio.get_running_loop()
    user_id = message.from_user.id
    # Handle /connect in groups
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        group_id = message.chat.id
        if not await is_check_admin(client, group_id, user_id): return await message.reply("❌ Only admins can connect this group to their PM.")
        await loop.run_in_executor(None, db.add_connect, group_id, user_id)
        await message.reply('✅ Successfully connected this group to your PM for settings management.')
    # Handle /connect in PM (with group ID)
    elif message.chat.type == enums.ChatType.PRIVATE:
        if len(message.command) > 1:
            try: grp_id = int(message.command[1])
            except ValueError: return await message.reply("⚠️ Invalid group ID provided. Please provide a valid numerical ID.")
            try:
                 # Check admin status in the target group
                 if not await is_check_admin(client, grp_id, user_id): return await message.reply('❌ You are not an admin in that group.')
                 chat = await client.get_chat(grp_id)
                 await loop.run_in_executor(None, db.add_connect, grp_id, user_id)
                 await message.reply(f'✅ Successfully connected to group: {chat.title}.')
            except Exception as e:
                logger.error(f"Connect cmd error get chat {grp_id}: {e}")
                await message.reply("❌ Could not connect. Ensure the ID is correct and I am in the group.")
        else: await message.reply('Usage: /connect <group_id>')

@Client.on_message(filters.command('delete') & filters.user(ADMINS))
async def delete_cmd(bot, message):
    try: query = message.text.split(" ", 1)[1]
    except IndexError: return await message.reply("Usage: /delete <search query>\n\nThis will delete all files matching the query from the database.")
    btn = [[ InlineKeyboardButton("⚠️ YES, DELETE", callback_data=f"delete_{query}") ], [ InlineKeyboardButton("❌ CANCEL", callback_data="close_data") ]]
    await message.reply(f"❓ Are you sure you want to delete all files matching `{query}`? This action cannot be undone.", reply_markup=InlineKeyboardMarkup(btn))

@Client.on_message(filters.command('img_2_link') & filters.user(ADMINS))
async def img_2_link_cmd(bot, message):
    r = message.reply_to_message
    if not r or not r.photo: return await message.reply('⚠️ Please reply to a photo to get its direct link.')
    txt = await message.reply("⏳ Uploading...")
    path = None # Initialize path
    link = None
    try:
        path = await r.download() # Download the photo
        loop = asyncio.get_running_loop()
        link = await loop.run_in_executor(None, upload_image, path) # Upload using the utility function
    except Exception as e:
        logger.error(f"img_2_link upload err: {e}")
    finally:
        try: # Ensure the downloaded file is removed
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as rm_err:
             logger.error(f"Error removing downloaded image {path}: {rm_err}")

    if not link: return await txt.edit("❌ Upload failed!")
    await txt.edit(f"<b>✅ Link Generated:\n`{link}`</b>", disable_web_page_preview=True);

@Client.on_message(filters.command('ping') & filters.user(ADMINS))
async def ping_cmd(client, message):
    start = monotonic(); msg = await message.reply("👀 Pinging..."); end = monotonic()
    await msg.edit(f'<b>ᴘᴏɴɢ!\n⏱️ {round((end - start) * 1000)} ms</b>')

@Client.on_message(filters.command(['cleanmultdb', 'cleandb']) & filters.user(ADMINS))
async def clean_multi_db_duplicates(bot, message):
    if not SECOND_FILES_DATABASE_URL or second_collection is None:
        return await message.reply("⚠️ Secondary database is not configured. Cannot perform cleanup.")
    sts_msg = await message.reply("🧹 Starting cross-database duplicate cleanup...\nThis might take a while.")
    loop = asyncio.get_running_loop(); removed = 0; checked = 0; errors = 0; start = time_now()
    try:
        logger.info("Fetching all primary DB IDs for cleanup...")
        primary_cursor = await loop.run_in_executor(None, partial(primary_collection.find, {}, {'_id': 1}))
        primary_ids = await loop.run_in_executor(None, lambda: {doc['_id'] for doc in primary_cursor})
        primary_count = len(primary_ids)
        logger.info(f"Found {primary_count} unique IDs in the primary database.")
        if primary_count == 0:
            return await sts_msg.edit("🧹 Primary database is empty. No cleanup needed.")

        logger.info("Iterating through secondary DB to find duplicates...")
        secondary_cursor = await loop.run_in_executor(None, partial(second_collection.find, {}, {'_id': 1}))

        BATCH_SIZE = 1000 # Process IDs in batches for efficiency
        def secondary_iterator(): # Generator to avoid loading all secondary IDs into memory
            for doc in secondary_cursor: yield doc
        doc_generator = await loop.run_in_executor(None, secondary_iterator)

        ids_to_remove = []; last_update_time = time_now()
        for doc in doc_generator:
            checked += 1
            if doc['_id'] in primary_ids: ids_to_remove.append(doc['_id']) # Add duplicate ID to removal list

            # Remove in batches
            if len(ids_to_remove) >= BATCH_SIZE:
                try:
                    del_res = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove}}))
                    deleted_now = del_res.deleted_count if del_res else 0; removed += deleted_now
                    logger.info(f"Removed {deleted_now} duplicates from secondary DB (Batch). Total removed: {removed}")
                except Exception as del_e: logger.error(f"Error removing batch from secondary DB: {del_e}"); errors += len(ids_to_remove)
                ids_to_remove = [] # Reset batch list

            # Update status message periodically
            current_time = time_now()
            if current_time - last_update_time > 15: # Update every 15 seconds
                 elapsed = get_readable_time(current_time - start)
                 status_text = f"🧹 Cleaning duplicates...\n~ Checked (DB2): <code>{checked}</code>\n~ Removed (DB2): <code>{removed}</code>\n~ Errors: <code>{errors}</code>\n~ Elapsed: <code>{elapsed}</code>"
                 try: await sts_msg.edit_text(status_text)
                 except FloodWait as e: await asyncio.sleep(e.value)
                 except MessageNotModified: pass
                 except Exception as edit_e: logger.warning(f"Cleanup status edit error: {edit_e}")
                 last_update_time = current_time

        # Remove any remaining IDs in the last batch
        if ids_to_remove:
            try:
                del_res = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove}}))
                deleted_now = del_res.deleted_count if del_res else 0; removed += deleted_now
                logger.info(f"Removed {deleted_now} duplicates from secondary DB (Final Batch). Total removed: {removed}")
            except Exception as del_e: logger.error(f"Error removing final batch from secondary DB: {del_e}"); errors += len(ids_to_remove)

        elapsed = get_readable_time(time_now() - start)
        await sts_msg.edit_text(f"✅ Cross-DB cleanup complete!\n\n⏱️ Took: <code>{elapsed}</code>\n\n📊 Stats:\n~ Checked (DB2): <code>{checked}</code>\n~ Removed (DB2): <code>{removed}</code>\n~ Errors: <code>{errors}</code>")
    except Exception as e: logger.error(f"/cleanmultdb error: {e}", exc_info=True); await sts_msg.edit(f"❌ An error occurred during cleanup: {e}")

@Client.on_message(filters.command('dbequal') & filters.user(ADMINS))
async def equalize_databases(bot, message):
    if not SECOND_FILES_DATABASE_URL or second_collection is None:
        return await message.reply("⚠️ Secondary database is not configured. Cannot equalize.")
    
    sts_msg = await message.reply("⚖️ Starting database equalization...\nThis will migrate files from DB1 to DB2 to balance counts. This might take a very long time depending on the number of files.")
    loop = asyncio.get_running_loop(); moved_count = 0; error_count = 0; start_time = time_now()

    try:
        # Get initial counts
        total_db1, total_db2 = await asyncio.gather(
            loop.run_in_executor(None, db_count_documents),
            loop.run_in_executor(None, second_db_count_documents)
        )
        
        if total_db1 == "ᴇʀʀ" or total_db2 == "ᴇʀʀ":
             return await sts_msg.edit("❌ Error fetching database counts. Cannot proceed.")
             
        total_db1 = int(total_db1); total_db2 = int(total_db2)

        if total_db1 == 0:
            return await sts_msg.edit("✅ Primary database (DB1) is already empty. No migration needed.")
            
        target_count_per_db = (total_db1 + total_db2) // 2
        files_to_move_count = total_db1 - target_count_per_db

        if files_to_move_count <= 0:
            return await sts_msg.edit(f"✅ Databases are already balanced or DB1 has fewer files.\n\nDB1 Count: `{total_db1}`\nDB2 Count: `{total_db2}`\nTarget: `{target_count_per_db}`")

        await sts_msg.edit(f"⚖️ Starting migration...\n\nInitial Counts:\n • DB1: `{total_db1}`\n • DB2: `{total_db2}`\n\nTarget per DB: `{target_count_per_db}`\nWill attempt to move `{files_to_move_count}` files from DB1 to DB2.")
        
        # Fetch documents to move (use cursor to avoid loading all into memory)
        files_to_move_cursor = await loop.run_in_executor(None, lambda: primary_collection.find().limit(files_to_move_count))
        
        BATCH_SIZE = 500 # Adjust batch size based on performance/memory
        docs_batch = []; last_update_time = time_now()

        for doc in files_to_move_cursor:
            docs_batch.append(doc)
            if len(docs_batch) >= BATCH_SIZE:
                try:
                    # Insert batch into DB2
                    await loop.run_in_executor(None, partial(second_collection.insert_many, docs_batch, ordered=False))
                    # Get IDs of inserted docs
                    ids_to_delete = [d['_id'] for d in docs_batch]
                    # Delete batch from DB1
                    await loop.run_in_executor(None, partial(primary_collection.delete_many, {'_id': {'$in': ids_to_delete}}))
                    moved_count += len(docs_batch)
                    logger.info(f"Moved batch of {len(docs_batch)} files from DB1 to DB2. Total moved: {moved_count}")
                except Exception as e:
                    logger.error(f"Error moving batch DB1->DB2: {e}")
                    error_count += len(docs_batch) # Assume all in batch failed if insert_many or delete_many fails
                finally:
                    docs_batch = [] # Clear batch

                # Update status periodically
                current_time = time_now()
                if current_time - last_update_time > 15:
                    elapsed = get_readable_time(current_time - start_time)
                    progress_text = f"⚖️ Migrating DB1 -> DB2...\n\nMoved: `{moved_count}` / `{files_to_move_count}`\nErrors: `{error_count}`\nElapsed: `{elapsed}`"
                    try: await sts_msg.edit(progress_text)
                    except FloodWait as e: await asyncio.sleep(e.value)
                    except MessageNotModified: pass
                    except Exception as edit_e: logger.warning(f"Equalize status edit error: {edit_e}")
                    last_update_time = current_time

        # Process the final batch if any documents remain
        if docs_batch:
            try:
                await loop.run_in_executor(None, partial(second_collection.insert_many, docs_batch, ordered=False))
                ids_to_delete = [d['_id'] for d in docs_batch]
                await loop.run_in_executor(None, partial(primary_collection.delete_many, {'_id': {'$in': ids_to_delete}}))
                moved_count += len(docs_batch)
                logger.info(f"Moved final batch of {len(docs_batch)} files from DB1 to DB2. Total moved: {moved_count}")
            except Exception as e:
                logger.error(f"Error moving final batch DB1->DB2: {e}")
                error_count += len(docs_batch)

        elapsed = get_readable_time(time_now() - start_time)
        # Get final counts
        final_total_db1, final_total_db2 = await asyncio.gather(
            loop.run_in_executor(None, db_count_documents),
            loop.run_in_executor(None, second_db_count_documents)
        )
        await sts_msg.edit(f"✅ DB equalization complete!\n\n⏱️ Took: `{elapsed}`\n\n📊 Results:\n • Moved: `{moved_count}`\n • Errors: `{error_count}`\n\nFinal Counts:\n • DB1: `{final_total_db1}`\n • DB2: `{final_total_db2}`")

    except Exception as e:
        logger.error(f"/dbequal error: {e}", exc_info=True)
        await sts_msg.edit(f"❌ An error occurred during equalization: {e}")

@Client.on_message(filters.command('set_fsub') & filters.user(ADMINS))
async def set_fsub_cmd(bot, message):
    try: _, ids_text = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_fsub -100xxx -100xxx ... (Separate IDs with spaces)')
    title = ""; valid_ids = []
    for id_str in ids_text.split():
        try:
            chat_id = int(id_str)
            chat = await bot.get_chat(chat_id)
            title += f' • {chat.title} (`{chat_id}`)\n'; valid_ids.append(str(chat_id))
        except ValueError: return await message.reply(f'⚠️ Invalid ID: `{id_str}`. IDs must be integers.')
        except Exception as e: return await message.reply(f'❌ Error getting chat info for `{id_str}`: {e}')
    if not valid_ids: return await message.reply('⚠️ No valid channel IDs provided.')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'FORCE_SUB_CHANNELS', " ".join(valid_ids))
    await message.reply(f'✅ Force Subscribe channels updated:\n{title}')

@Client.on_message(filters.command('set_req_fsub') & filters.user(ADMINS))
async def set_req_fsub_cmd(bot, message):
    try: _, id_str = message.text.split(' ', 1)
    except ValueError: return await message.reply('Usage: /set_req_fsub <channel_id>')
    try:
        chat_id = int(id_str)
        chat = await bot.get_chat(chat_id)
    except ValueError: return await message.reply(f'⚠️ Invalid ID: `{id_str}`. ID must be an integer.')
    except Exception as e: return await message.reply(f'❌ Error getting chat info for `{id_str}`: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'REQUEST_FORCE_SUB_CHANNELS', str(chat_id))
    await message.reply(f'✅ Request Join Channel set to: {chat.title} (`{chat_id}`)')

@Client.on_message(filters.command('off_auto_filter') & filters.user(ADMINS))
async def off_auto_filter_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', False); await message.reply('✅ Global Auto Filter has been **Disabled**.')

@Client.on_message(filters.command('on_auto_filter') & filters.user(ADMINS))
async def on_auto_filter_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', True); await message.reply('✅ Global Auto Filter has been **Enabled**.')

@Client.on_message(filters.command('off_pm_search') & filters.user(ADMINS))
async def off_pm_search_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', False); await message.reply('✅ PM File Search has been **Disabled**.')

@Client.on_message(filters.command('on_pm_search') & filters.user(ADMINS))
async def on_pm_search_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', True); await message.reply('✅ PM File Search has been **Enabled**.')
