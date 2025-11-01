import os
import random
import string
import asyncio
from time import time as time_now
from time import monotonic
import datetime # Keep this, needed for timedelta
from Script import script # Keep this for default texts if needed
from hydrogram import Client, filters, enums
from hydrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
# Use the new get_total_files_count and import collections list
from database.ia_filterdb import (
    get_total_files_count, get_file_details, delete_files, 
    file_db_collections, get_active_collection_with_index # <-- Import new items
)
from database.users_chats_db import db
from datetime import datetime, timedelta, timezone # Keep datetime, add timezone
import pytz # Keep pytz
from info import (URL, BIN_CHANNEL, INDEX_CHANNELS, ADMINS,
                  IS_VERIFY, VERIFY_TUTORIAL, VERIFY_EXPIRE, SHORTLINK_API, SHORTLINK_URL,
                  DELETE_TIME, SUPPORT_LINK, UPDATES_LINK, LOG_CHANNEL, PICS, IS_STREAM,
                  PM_FILE_DELETE_TIME, BOT_ID, PROTECT_CONTENT, TUTORIAL, # Keep PROTECT_CONTENT and TUTORIAL
                  IMDB, SPELL_CHECK, AUTO_DELETE, WELCOME, SHORTLINK, LINK_MODE # Keep group setting defaults
                  )
from utils import (get_settings, get_size, is_subscribed, is_check_admin, get_shortlink,
                   get_verify_status, update_verify_status, save_group_settings, temp,
                   get_readable_time, get_wish, get_seconds, upload_image)
from hydrogram.errors import MessageNotModified, FloodWait
import logging
from functools import partial # Import partial
from pymongo.errors import BulkWriteError # <-- Import BulkWriteError

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
        btn = [[ InlineKeyboardButton('• ᴜᴘᴅᴀᴛᴇꜱ •', url=UPDATES_LINK), InlineKeyboardButton('• ꜱᴜᴘᴘᴏʀᴛ •', url=SUPPORT_LINK) ]]
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
        buttons = [[ InlineKeyboardButton("➕ ᴀᴅᴅ ᴛᴏ ɢʀᴏᴜᴘ", url=f'http://t.me/{temp.U_NAME}?startgroup=start') ], [ InlineKeyboardButton('• ᴜᴘᴅᴀᴛᴇꜱ •', url=UPDATES_LINK), InlineKeyboardButton('• ꜱᴜᴘᴘᴏʀᴛ •', url=SUPPORT_LINK) ], [ InlineKeyboardButton('• ʜᴇʟᴘ •', callback_data='help'), InlineKeyboardButton('🔍 ɪɴʟɪɴᴇ', switch_inline_query_current_chat=''), InlineKeyboardButton('• ᴀʙᴏᴜᴛ •', callback_data='about') ]]
        await message.reply_photo(random.choice(PICS), caption=script.START_TXT.format(message.from_user.mention, get_wish()), reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.HTML); return

    # Handle deep links (start parameters)
    mc = message.command[1]

    # Handle settings deep link
    if mc.startswith('settings'):
        try: _, group_id_str = mc.split("_", 1); group_id = int(group_id_str)
        except (ValueError, IndexError): return await message.reply("ɪɴᴠᴀʟɪᴅ ꜱᴇᴛᴛɪɴɢꜱ ʟɪɴᴋ.")
        if not await is_check_admin(client, group_id, user_id): return await message.reply("ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.")
        try:
            btn = await get_grp_stg(group_id)
            chat = await client.get_chat(group_id)
            await message.reply(f"⚙️ ꜱᴇᴛᴛɪɴɢꜱ ꜰᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        except Exception as e:
            logger.error(f"PM settings link error {group_id}: {e}"); await message.reply("ᴇʀʀᴏʀ ꜰᴇᴛᴄʜɪɴɢ ꜱᴇᴛᴛɪɴɢꜱ."); return

    # Handle inline force subscribe check deep link
    elif mc == 'inline_fsub':
        btn = await is_subscribed(client, message);
        if btn: await message.reply("❗ᴘʟᴇᴀꜱᴇ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ(ꜱ) ʙᴇʟᴏᴡ ᴛᴏ ᴜꜱᴇ ᴍᴇ.", reply_markup=InlineKeyboardMarkup(btn))
        else: await message.reply("✔️ ʏᴏᴜ ᴀʀᴇ ᴀʟʀᴇᴀᴅʏ ꜱᴜʙꜱᴄʀɪʙᴇᴅ."); return

    # Handle verification token deep link
    elif mc.startswith('verify_'):
        try: _, token = mc.split("_", 1)
        except ValueError: return await message.reply("ɪɴᴠᴀʟɪᴅ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ʟɪɴᴋ.")
        verify_status = await get_verify_status(user_id);
        if verify_status.get('verify_token') != token: return await message.reply("❌ ᴛᴏᴋᴇɴ ɪɴᴠᴀʟɪᴅ/ᴇхᴘɪʀᴇᴅ.")
        # Mark as verified and set expiry
        expiry_time = datetime.now(timezone.utc) + timedelta(seconds=VERIFY_EXPIRE)
        await update_verify_status(user_id, is_verified=True, expire_time=expiry_time, verify_token="")
        link_to_get = verify_status.get("link", "") # Get the original link they wanted
        reply_markup = InlineKeyboardMarkup([[ InlineKeyboardButton("ɢᴇᴛ ꜰɪʟᴇ", url=f'https://t.me/{temp.U_NAME}?start={link_to_get}') ]]) if link_to_get else None
        await message.reply(f"✔️ ᴠᴇʀɪꜰɪᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ!\n\nʏᴏᴜʀ ᴀᴄᴄᴇꜱꜱ ᴇхᴘɪʀᴇꜱ ᴏɴ: {expiry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}", reply_markup=reply_markup, protect_content=True); return

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
        btn_verify = [[ InlineKeyboardButton("🧿 ᴠᴇʀɪꜰʏ ɴᴏᴡ", url=short_link) ], [ InlineKeyboardButton('❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ ʟɪɴᴋ', url=tutorial) ]]
        await message.reply("🔐 ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ!\n\nᴘʟᴇᴀꜱᴇ ᴠᴇʀɪꜰʏ ʙʏ ᴄʟɪᴄᴋɪɴɢ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ᴄᴏɴᴛɪɴᴜᴇ.", reply_markup=InlineKeyboardMarkup(btn_verify), protect_content=True); return

    # --- Force Subscribe Check ---
    btn_fsub = await is_subscribed(client, message);
    if btn_fsub:
        btn_fsub.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
        await message.reply_photo(random.choice(PICS), caption=f"👋 ʜᴇʏ {message.from_user.mention},\n\nʏᴏᴜ ɴᴇᴇᴅ ᴛᴏ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ(ꜱ) ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ꜰɪʟᴇꜱ 👇", reply_markup=InlineKeyboardMarkup(btn_fsub)); return

    # --- Process File/Batch Requests ---
    try:
        # Handle batch file request (/start all_...)
        if mc.startswith('all'):
            _, grp_id, key = mc.split("_", 2); grp_id = int(grp_id)
            files = temp.FILES.get(key)
            if not files: return await message.reply('❌ ʟɪɴᴋ ᴇхᴘɪʀᴇᴅ ᴏʀ ɪɴᴠᴀʟɪᴅ.')
            settings = await get_settings(grp_id);
            sent = []; total_msg = await message.reply(f"<b><i>🗂️ ꜱᴇɴᴅɪɴɢ <code>{len(files)}</code> ꜰɪʟᴇꜱ ᴏɴᴇ ʙʏ ᴏɴᴇ... ᴘʟᴇᴀꜱᴇ ᴡᴀɪᴛ.</i></b>")
            for file in files:
                fid = file['_id']; cap = file.get('caption', '')
                CAPTION = settings.get('caption', script.FILE_CAPTION)
                try: f_cap = CAPTION.format(file_name=file.get('file_name','N/A'), file_size=get_size(file.get('file_size',0)), file_caption=cap)
                except Exception as e: logger.error(f"Caption format err {grp_id}: {e}"); f_cap = file.get('file_name','N/A')
                stream_btn = [[ InlineKeyboardButton("• ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ •", callback_data=f"stream#{fid}") ]] if IS_STREAM else []
                other_btns = [[ InlineKeyboardButton('• ᴜᴘᴅᴀᴛᴇꜱ •', url=UPDATES_LINK), InlineKeyboardButton('💬 ꜱᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
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
            info = await message.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜᴇꜱᴇ ꜰɪʟᴇꜱ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴀꜰᴛᴇʀ <b>{time_r}</b> ᴛᴏ ᴘʀᴇᴠᴇɴᴛ ᴄᴏᴘʏʀɪɢʜᴛ ɪꜱꜱᴜᴇꜱ.", quote=True)
            await asyncio.sleep(pm_del);
            try: await client.delete_messages(user_id, sent + [total_msg.id]) # Delete sent files and the "Sending..." message
            except Exception as e: logger.error(f"Error auto-del batch {user_id}: {e}")
            del_btns = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ᴀɢᴀɪɴ', callback_data=f"get_del_send_all_files#{grp_id}#{key}") ]]
            try: await info.edit("❗️ ꜰɪʟᴇꜱ ᴅᴇʟᴇᴛᴇᴅ ᴛᴏ ᴀᴠᴏɪᴅ ᴄᴏᴘʏʀɪɢʜᴛ. ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ᴛʜᴇᴍ ᴀɢᴀɪɴ.", reply_markup=InlineKeyboardMarkup(del_btns))
            except: pass; return

        # Handle single file request (/start file_... or /start shortlink_...)
        elif mc.startswith(('file_', 'shortlink_')):
            type_, grp_id, file_id = mc.split("_", 2) # Handles both file_ and shortlink_
            grp_id = int(grp_id)
            settings = await get_settings(grp_id);
            files_ = await get_file_details(file_id);
            if not files_: return await message.reply('❌ ɴᴏ ꜰɪʟᴇ ꜰᴏᴜɴᴅ ᴡɪᴛʜ ᴛʜᴀᴛ ɪᴅ.')
            file_doc = files_[0] if isinstance(files_, list) and files_ else None
            if not file_doc: return await message.reply('❌ ᴇʀʀᴏʀ ʀᴇᴛʀɪᴇᴠɪɴɢ ꜰɪʟᴇ ᴅᴇᴛᴀɪʟꜱ.')

            # Check if shortlink is enabled for this group and the link type isn't already 'shortlink'
            if type_ != 'shortlink' and settings.get('shortlink', SHORTLINK): # Use imported default
                s_url, s_api, tut = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API), settings.get('tutorial', TUTORIAL) # Use imported default for tutorial
                o_link = f"https://t.me/{temp.U_NAME}?start=shortlink_{grp_id}_{file_id}" # Link to bypass shortener check
                try: s_link = await get_shortlink(s_url, s_api, o_link)
                except Exception as e: logger.error(f"Shortlink file {file_id} error: {e}"); s_link = o_link
                s_btn = [[ InlineKeyboardButton("ɢᴇᴛ ꜰɪʟᴇ ʟɪɴᴋ", url=s_link) ], [ InlineKeyboardButton("❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ", url=tut) ]]
                fname = file_doc.get('file_name', 'ꜰɪʟᴇ'); fsize = get_size(file_doc.get('file_size', 0))
                await message.reply(f"[{fsize}] {fname}\n\n👇 ᴄʟɪᴄᴋ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ᴛʜᴇ ꜰɪʟᴇ ʟɪɴᴋ.", reply_markup=InlineKeyboardMarkup(s_btn), protect_content=True); return

            # Proceed to send the file directly
            CAPTION = settings.get('caption', script.FILE_CAPTION); cap_txt = file_doc.get('caption', '')
            try: f_cap = CAPTION.format(file_name=file_doc.get('file_name','N/A'), file_size=get_size(file_doc.get('file_size',0)), file_caption=cap_txt)
            except Exception as e: logger.error(f"Caption format error {grp_id}: {e}"); f_cap = file_doc.get('file_name','N/A')
            stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]] if IS_STREAM else []
            other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇꜱ', url=UPDATES_LINK), InlineKeyboardButton('💬 ꜱᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
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
                await message.reply("❌ ᴇʀʀᴏʀ ꜱᴇɴᴅɪɴɢ.") # User-friendly message
                return
            # Auto-delete logic for single file
            pm_del = PM_FILE_DELETE_TIME; time_r = get_readable_time(pm_del)
            msg_timer = await vp.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜɪꜱ ꜰɪʟᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴀꜰᴛᴇʀ <b>{time_r}</b>.", quote=True) if vp else await message.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜɪꜱ ꜰɪʟᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴀꜰᴛᴇʀ <b>{time_r}</b>.", quote=True)
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
            try: await message.reply("❗️ ꜰɪʟᴇ ᴅᴇʟᴇᴛᴇᴅ ᴅᴜᴇ ᴛᴏ ᴄᴏᴘʏʀɪɢʜᴛ. ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ɪᴛ ᴀɢᴀɪɴ.", reply_markup=InlineKeyboardMarkup(del_btns))
            except Exception as e: logger.warning(f"Could not send 'file gone' {user_id}: {e}"); return
        else:
            await message.reply("❓ ɪɴᴠᴀʟɪᴅ ꜱᴛᴀʀᴛ ᴄᴏᴍᴍᴀɴᴅ ᴘᴀʀᴀᴍᴇᴛᴇʀ.")
    except Exception as e:
        logger.error(f"Error processing start command '{mc}': {e}", exc_info=True)
        await message.reply("❌ ᴀɴ ᴜɴᴇхᴘᴇᴄᴛᴇᴅ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ.")


@Client.on_message(filters.command('link'))
async def link_cmd(bot, message):
    msg = message.reply_to_message
    if not msg: return await message.reply('⚠️ ᴘʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇᴅɪᴀ ꜰɪʟᴇ ᴛᴏ ɢᴇᴛ ꜱᴛʀᴇᴀᴍ/ᴅᴏᴡɴʟᴏᴀᴅ ʟɪɴᴋꜱ.')
    media = getattr(msg, msg.media.value, None) if msg.media else None
    if not media or not hasattr(media, 'file_id'): return await message.reply('⚠️ ᴛʜɪꜱ ᴍᴇꜱꜱGE ᴅᴏᴇꜱ ɴᴏᴛ ᴄᴏɴᴛᴀɪɴ ᴀ ꜱᴜᴘᴘᴏʀᴛᴇᴅ ᴍᴇᴅɪᴀ ꜰɪʟᴇ.')
    try:
        if not IS_STREAM: return await message.reply('🖥️ ꜱᴛʀᴇᴀᴍɪɴɢ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ᴅɪꜱᴀʙʟᴇᴅ.')
        try:
            stream_msg = await bot.send_cached_media(BIN_CHANNEL, media.file_id) # Cache in BIN_CHANNEL
        except Exception as e:
            logger.error(f"Error caching media to BIN_CHANNEL {BIN_CHANNEL}: {e}"); return await message.reply("❌ ᴇʀʀᴏʀ ɢᴇɴᴇʀᴀᴛɪɴɢ ʟɪɴᴋꜱ. ᴄᴏᴜʟᴅ ɴᴏᴛ ᴀᴄᴄᴇꜱꜱ ʙɪɴ ᴄʜᴀɴɴᴇʟ.")
        watch = f"{URL}watch/{stream_msg.id}"; download = f"{URL}download/{stream_msg.id}"
        btn=[[ InlineKeyboardButton("• ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ •", url=watch), InlineKeyboardButton("• ᴅᴏᴡɴʟᴏᴀᴅ •", url=download)], [ InlineKeyboardButton('❌ ᴄʟᴏꜱᴇ', callback_data='close_data') ]]
        await message.reply('✔️ ʟɪɴᴋꜱ ɢᴇɴᴇʀᴀᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ:', reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e:
        logger.error(f"Link cmd error: {e}", exc_info=True); await message.reply('❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ɢᴇɴᴇʀᴀᴛɪɴɢ ʟɪɴᴋꜱ.')

@Client.on_message(filters.command('index_channels') & filters.user(ADMINS))
async def channels_info_cmd(bot, message):
    ids = INDEX_CHANNELS; text = '**ɪɴᴅᴇхᴇᴅ ᴄʜᴀɴɴᴇʟꜱ:**\n\n'
    if not ids: return await message.reply("⚠️ ɴᴏ ᴄʜᴀɴɴᴇʟꜱ ᴀʀᴇ ᴄᴏɴꜰɪɢᴜʀᴇᴅ ꜰᴏʀ ɪɴᴅᴇхɪɴɢ.")
    for id_ in ids:
        try: chat = await bot.get_chat(id_); text += f' • {chat.title} (`{id_}`)\n'
        except Exception as e: logger.warning(f"Could not get chat info for {id_}: {e}"); text += f' • ᴜɴᴋɴᴏᴡɴ ᴄʜᴀɴɴᴇʟ (`{id_}`) - ᴇʀʀᴏʀ: {e}\n'
    await message.reply(text + f'\n**ᴛᴏᴛᴀʟ ɪɴᴅᴇх ᴄʜᴀNɴᴇʟꜱ:** {len(ids)}')

@Client.on_message(filters.command('stats') & filters.user(ADMINS))
async def stats_cmd(bot, message):
    loop = asyncio.get_running_loop()
    sts_msg = await message.reply("ɢᴀᴛʜᴇʀɪɴɢ ʙᴏᴛ ꜱᴛᴀᴛɪꜱᴛɪᴄꜱ...")
    async def get_stat_safe(func, *args):
        try:
            call_func = partial(func, *args) if args else func
            return await loop.run_in_executor(None, call_func)
        except Exception as e:
            logger.error(f"Stat collection error ({func.__name__ if hasattr(func, '__name__') else 'unknown'}): {e}")
            return "ᴇʀʀ" # Return error string
            
    # Fetch stats concurrently
    total_files_task = get_stat_safe(get_total_files_count)
    users_task = get_stat_safe(db.total_users_count)
    chats_task = get_stat_safe(db.total_chat_count)
    data_db_size_task = get_stat_safe(db.get_data_db_size)
    all_files_db_stats_task = get_stat_safe(db.get_all_files_db_stats)

    total_files, users, chats, used_data_db_size_raw, all_files_db_stats = await asyncio.gather(
        total_files_task,
        users_task,
        chats_task,
        data_db_size_task,
        all_files_db_stats_task
    )
    
    # Format sizes
    used_data_db_size = get_size(used_data_db_size_raw) if isinstance(used_data_db_size_raw, (int, float)) else used_data_db_size_raw

    # Format files DB stats string
    db_stats_str = ""
    if isinstance(all_files_db_stats, list):
        for stat in all_files_db_stats:
            if stat.get('error'):
                db_stats_str += f"│ 🗂️ {stat['name']}: <code>Error</code>\n"
            else:
                # Use the 'coll_count' already retrieved from db.get_all_files_db_stats
                db_file_count = stat.get('coll_count', 'N/A') 
                db_stats_str += f"│ 🗂️ {stat['name']} ({db_file_count} ꜰɪʟᴇꜱ): <code>{get_size(stat['size'])}</code>\n"
    else:
        db_stats_str = "│ 🗂️ ꜰɪʟᴇ ᴅʙ ꜱᴛᴀᴛꜱ: <code>ᴇʀʀ</code>\n"

    # Get active DB index
    stg = await loop.run_in_executor(None, db.get_bot_sttgs) # Get settings
    current_db_index = stg.get('CURRENT_DB_INDEX', 0) # Get the index

    # Get uptime
    uptime = get_readable_time(time_now() - temp.START_TIME)
    
    # Edit message with results (Assuming font applied in Script.py)
    await sts_msg.edit(script.STATUS_TXT.format(
        users, 
        chats, 
        used_data_db_size, 
        total_files,
        current_db_index + 1, # Pass the active DB index (1-based)
        db_stats_str.rstrip('\n'), 
        uptime
    ))

async def get_grp_stg(group_id):
    """Generates the settings inline keyboard for a group."""
    settings = await get_settings(group_id)
    # Define buttons using f-strings for status and callback data (Using font cautiously for button text)
    btn = [
        [InlineKeyboardButton('ɪᴍᴅʙ ᴛᴇᴍᴘʟᴀᴛᴇ', callback_data=f'imdb_setgs#{group_id}')],
        [InlineKeyboardButton('ꜱʜᴏʀᴛʟɪɴᴋ ꜱᴇᴛᴛɪɴɢꜱ', callback_data=f'shortlink_setgs#{group_id}')],
        [InlineKeyboardButton('ꜰɪʟᴇ ᴄᴀᴘᴛɪᴏɴ', callback_data=f'caption_setgs#{group_id}')],
        [InlineKeyboardButton('ᴡᴇʟᴄᴏᴍᴇ ᴍᴇꜱꜱᴀɢᴇ', callback_data=f'welcome_setgs#{group_id}')],
        [InlineKeyboardButton('ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ (ꜰᴏʀ ꜱʜᴏʀᴛʟɪɴᴋ/ᴠᴇʀɪꜰʏ)', callback_data=f'tutorial_setgs#{group_id}')],
        # Boolean toggles
        [InlineKeyboardButton(f'ᴘᴏꜱᴛᴇʀ {"✔️ ᴇɴᴀʙʟᴇᴅ" if settings.get("imdb", IMDB) else "❌ ᴅɪꜱᴀʙʟᴇᴅ"}', callback_data=f'bool_setgs#imdb#{settings.get("imdb", IMDB)}#{group_id}')],
        [InlineKeyboardButton(f'ꜱᴘᴇʟʟ ᴄʜᴇᴄᴋ {"✔️ ᴇɴᴀʙʟᴇᴅ" if settings.get("spell_check", SPELL_CHECK) else "❌ ᴅɪꜱᴀʙʟᴇᴅ"}', callback_data=f'bool_setgs#spell_check#{settings.get("spell_check", SPELL_CHECK)}#{group_id}')],
        [InlineKeyboardButton(f'ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ {"✔️ ᴇɴᴀʙʟᴇᴅ" if settings.get("auto_delete", AUTO_DELETE) else "❌ ᴅɪꜱᴀʙʟᴇᴅ"}', callback_data=f'bool_setgs#auto_delete#{settings.get("auto_delete", AUTO_DELETE)}#{group_id}')],
        [InlineKeyboardButton(f'ᴡᴇʟᴄᴏᴍᴇ ᴍꜱɢ {"✔️ ᴇɴᴀʙʟᴇᴅ" if settings.get("welcome", WELCOME) else "❌ ᴅɪꜱᴀʙʟᴇᴅ"}', callback_data=f'bool_setgs#welcome#{settings.get("welcome", WELCOME)}#{group_id}')],
        [InlineKeyboardButton(f'ꜱʜᴏʀᴛʟɪɴᴋ (ꜰɪʟᴇ ᴀᴄᴄᴇꜱꜱ) {"✔️ ᴇɴᴀʙʟᴇᴅ" if settings.get("shortlink", SHORTLINK) else "❌ ᴅɪꜱᴀʙʟᴇᴅ"}', callback_data=f'bool_setgs#shortlink#{settings.get("shortlink", SHORTLINK)}#{group_id}')],
        [InlineKeyboardButton(f'ʀᴇꜱᴜʟᴛ ᴘᴀɢᴇ {"🔗 ʟɪɴᴋ ᴍᴏᴅᴇ" if settings.get("links", LINK_MODE) else "🔘 ʙᴜᴛᴛᴏɴ ᴍᴏᴅᴇ"}', callback_data=f'bool_setgs#links#{settings.get("links", LINK_MODE)}#{group_id}')]
    ]
    return btn

@Client.on_message(filters.command('settings'))
async def settings_cmd(client, message):
    group_id = message.chat.id
    user_id = message.from_user.id
    # Handle /settings in groups
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await is_check_admin(client, group_id, user_id): return await message.reply('❌ ᴏɴʟʏ ᴀᴅᴍɪɴꜱ ᴄᴀɴ ᴍᴀɴᴀɢᴇ ɢʀᴏᴜᴘ ꜱᴇᴛᴛɪɴɢꜱ.')
        btn = [[ InlineKeyboardButton("🔧 ᴏᴘᴇɴ ꜱᴇᴛᴛɪɴɢꜱ ʜᴇʀᴇ", callback_data='open_group_settings') ], [ InlineKeyboardButton("🔒 ᴏᴘᴇɴ ꜱᴇᴛᴛɪɴɢꜱ ɪɴ ᴘᴍ", callback_data='open_pm_settings') ]]
        await message.reply('ᴄʜᴏᴏꜱᴇ ᴡʜᴇʀᴇ ᴛᴏ ᴏᴘᴇɴ ᴛʜᴇ ꜱᴇᴛᴛɪɴɢꜱ ᴍᴇɴᴜ:', reply_markup=InlineKeyboardMarkup(btn))
    # Handle /settings in PM
    elif message.chat.type == enums.ChatType.PRIVATE:
        loop = asyncio.get_running_loop()
        cons = await loop.run_in_executor(None, db.get_connections, user_id)
        if not cons: return await message.reply("ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ ᴀɴʏ ɢʀᴏᴜᴘꜱ ʏᴇᴛ! ᴜꜱᴇ /ᴄᴏɴɴᴇᴄᴛ ɪɴ ᴀ ɢʀᴏᴜᴘ ᴡʜᴇʀᴇ ʏᴏᴜ ᴀʀᴇ ᴀɴ ᴀᴅᴍɪɴ.")
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
        if not buttons: return await message.reply("ʏᴏᴜ ᴀʀᴇ ɴᴏ ʟᴏɴɢᴇʀ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴀɴʏ ᴄᴏɴɴᴇᴄᴛᴇᴅ ɢʀᴏᴜᴘꜱ. ᴜꜱᴇ /ᴄᴏɴɴᴇᴄᴛ ɪɴ ᴀ ɢʀᴏᴜᴘ.")
        await message.reply('ꜱᴇʟᴇᴄᴛ ᴛʜᴇ ɢʀᴏᴜᴘ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴍᴀɴᴀɢᴇ ꜱᴇᴛᴛɪɴɢꜱ ꜰᴏʀ:', reply_markup=InlineKeyboardMarkup(buttons))

@Client.on_message(filters.command('connect'))
async def connect_cmd(client, message):
    loop = asyncio.get_running_loop()
    user_id = message.from_user.id
    # Handle /connect in groups
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        group_id = message.chat.id
        if not await is_check_admin(client, group_id, user_id): return await message.reply("❌ ᴏɴʟʏ ᴀᴅᴍɪɴꜱ ᴄᴀɴ ᴄᴏɴɴᴇᴄᴛ ᴛʜɪꜱ ɢʀᴏᴜᴘ ᴛᴏ ᴛʜᴇɪʀ ᴘᴍ.")
        await loop.run_in_executor(None, db.add_connect, group_id, user_id)
        await message.reply('✔️ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛʜɪꜱ ɢʀᴏᴜᴘ ᴛᴏ ʏᴏᴜʀ ᴘᴍ ꜰᴏʀ ꜱᴇᴛᴛɪɴɢꜱ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ.')
    # Handle /connect in PM (with group ID)
    elif message.chat.type == enums.ChatType.PRIVATE:
        if len(message.command) > 1:
            try: grp_id = int(message.command[1])
            except ValueError: return await message.reply("⚠️ ɪɴᴠᴀʟɪᴅ ɢʀᴏᴜᴘ ɪᴅ ᴘʀᴏᴠɪᴅᴇᴅ. ᴘʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ᴠᴀʟɪᴅ ɴᴜᴍᴇʀɪᴄᴀʟ ɪᴅ.")
            try:
                 # Check admin status in the target group
                 if not await is_check_admin(client, grp_id, user_id): return await message.reply('❌ ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.')
                 chat = await client.get_chat(grp_id)
                 await loop.run_in_executor(None, db.add_connect, grp_id, user_id)
                 await message.reply(f'✔️ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴄᴏɴɴᴇᴄᴛᴇᴅ ᴛᴏ ɢʀᴏᴜᴘ: {chat.title}.')
            except Exception as e:
                logger.error(f"Connect cmd error get chat {grp_id}: {e}")
                await message.reply("❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴄᴏɴɴᴇᴄᴛ. ᴇɴꜱᴜʀᴇ ᴛʜᴇ ɪᴅ ɪꜱ ᴄᴏʀʀᴇᴄᴛ ᴀɴᴅ ɪ ᴀᴍ ɪɴ ᴛʜᴇ ɢʀᴏᴜᴘ.")
        else: await message.reply('ᴜꜱᴀɢᴇ: /ᴄᴏɴɴᴇᴄᴛ <ɢʀᴏᴜᴘ_ɪᴅ>')

@Client.on_message(filters.command('delete') & filters.user(ADMINS))
async def delete_cmd(bot, message):
    try: query = message.text.split(" ", 1)[1]
    except IndexError: return await message.reply("ᴜꜱᴀɢᴇ: /ᴅᴇʟᴇᴛᴇ <ꜱᴇᴀʀᴄʜ ǫᴜᴇʀʏ>\n\nᴛʜɪꜱ ᴡɪʟʟ ᴅᴇʟᴇᴛᴇ ᴀʟʟ ꜰɪʟᴇꜱ ᴍᴀᴛᴄʜɪɴɢ ᴛʜᴇ ǫᴜᴇʀʏ ꜰʀᴏᴍ ᴛʜᴇ ᴅᴀᴛᴀʙᴀꜱᴇ.")
    btn = [[ InlineKeyboardButton("⚠️ ʏᴇꜱ, ᴅᴇʟᴇᴛᴇ", callback_data=f"delete_{query}") ], [ InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="close_data") ]]
    await message.reply(f"❓ ᴀʀᴇ ʏᴏᴜ ꜱᴜʀᴇ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴅᴇʟᴇᴛᴇ ᴀʟʟ ꜰɪʟᴇꜱ ᴍᴀᴛᴄʜɪɴɢ `{query}`? ᴛʜɪꜱ ᴀᴄᴛɪᴏɴ ᴄᴀɴɴᴏᴛ ʙᴇ ᴜɴᴅᴏɴᴇ.", reply_markup=InlineKeyboardMarkup(btn))

@Client.on_message(filters.command('img_2_link') & filters.user(ADMINS))
async def img_2_link_cmd(bot, message):
    r = message.reply_to_message
    if not r or not r.photo: return await message.reply('⚠️ ᴘʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ᴛᴏ ɢᴇᴛ ɪᴛꜱ ᴅɪʀᴇᴄᴛ ʟɪɴᴋ.')
    txt = await message.reply("⏳ ᴜᴘʟᴏᴀᴅɪɴɢ...")
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

    if not link: return await txt.edit("❌ ᴜᴘʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ!")
    await txt.edit(f"<b>✔️ ʟɪɴᴋ ɢᴇɴᴇʀᴀᴛᴇᴅ:\n`{link}`</b>", disable_web_page_preview=True);

@Client.on_message(filters.command('ping') & filters.user(ADMINS))
async def ping_cmd(client, message):
    start = monotonic(); msg = await message.reply("👀 ᴘɪɴɢɪɴɢ..."); end = monotonic()
    await msg.edit(f'<b>ᴘᴏɴɢ!\n⏱️ {round((end - start) * 1000)} ᴍꜱ</b>')


topdown_lock = asyncio.Lock() # Lock to prevent running /topdown twice

@Client.on_message(filters.command('topdown') & filters.user(ADMINS))
async def topdown_cmd(bot, message):
    """Moves 10% of files from other DBs to the active DB."""
    global topdown_lock
    if topdown_lock.locked():
        return await message.reply("⚠️ ᴀ ᴛᴏᴘᴅᴏᴡɴ ᴘʀᴏᴄᴇꜱꜱ ɪꜱ ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɢ.")
    
    async with topdown_lock:
        sts_msg = await message.reply("⏳ ɪɴɪᴛɪᴀᴛɪɴɢ ᴛᴏᴘ-ᴅᴏᴡɴ ᴅᴀᴛᴀ ʙᴀʟᴀɴᴄᴇ...\nᴛʜɪꜱ ᴡɪʟʟ ᴛᴀᴋᴇ ᴀ ʟᴏɴɢ ᴛɪᴍᴇ.\n\nꜰɪɴᴅɪɴɢ ᴀᴄᴛɪᴠᴇ ᴅᴀᴛᴀʙᴀꜱᴇ...")
        loop = asyncio.get_running_loop()
        start = time_now()
        
        try:
            active_coll, active_index = await get_active_collection_with_index(data_db)
            if active_coll is None:
                return await sts_msg.edit("❌ ᴀʟʟ ᴅᴀᴛᴀʙᴀꜱᴇꜱ ᴀʀᴇ ꜰᴜʟʟ. ᴄᴀɴɴᴏᴛ ᴘᴇʀꜰᴏʀᴍ ᴛᴏᴘᴅᴏᴡɴ.")
            
            active_db_name = f"DB #{active_index + 1}"
            await sts_msg.edit(f"🎯 ᴀᴄᴛɪᴠᴇ ᴅᴀᴛᴀʙᴀꜱᴇ ꜰᴏᴜɴᴅ: **{active_db_name}**.\n\nꜱᴛᴀʀᴛɪɴɢ ᴛʀᴀɴꜱꜰᴇʀ...")
            
            source_collections = [(coll, i) for i, coll in enumerate(file_db_collections) if i != active_index]
            
            if not source_collections:
                return await sts_msg.edit("ℹ️ ᴏɴʟʏ ᴏɴᴇ ᴅᴀᴛᴀʙᴀꜱᴇ ᴄᴏɴꜰɪɢᴜʀᴇᴅ. ɴᴏᴛʜɪɴɢ ᴛᴏ ᴛᴏᴘᴅᴏᴡɴ.")

            total_moved = 0
            total_failed_duplicates = 0
            total_failed_other = 0

            for source_coll, source_index in source_collections:
                source_db_name = f"DB #{source_index + 1}"
                await sts_msg.edit(f"⏳ ᴘʀᴏᴄᴇꜱꜱɪɴɢ {source_db_name}...\n~ ᴛᴏᴛᴀʟ ᴍᴏᴠᴇᴅ: {total_moved}\n~ ᴛᴏᴛᴀʟ ꜰᴀɪʟᴇᴅ: {total_failed_duplicates + total_failed_other}")
                
                try:
                    coll_count = await loop.run_in_executor(None, partial(source_coll.count_documents, {}))
                    limit = int(coll_count * 0.10) # 10%
                    
                    if limit == 0:
                        logger.info(f"Skipping {source_db_name}, 10% is 0.")
                        continue
                    
                    # Fetch the full documents to move
                    docs_to_move = await loop.run_in_executor(None, list, source_coll.find().limit(limit))
                    if not docs_to_move:
                        logger.info(f"No documents found to move from {source_db_name}.")
                        continue
                    
                    ids_to_move = [doc['_id'] for doc in docs_to_move]
                    inserted_ids = []
                    
                    try:
                        # Insert into active DB, skip duplicates (ordered=False)
                        result = await loop.run_in_executor(None, partial(active_coll.insert_many, docs_to_move, ordered=False))
                        inserted_ids = result.inserted_ids
                        total_moved += len(inserted_ids)
                        
                    except BulkWriteError as bwe:
                        # Some docs were inserted, some failed (likely duplicates)
                        inserted_ids = [d['_id'] for d in bwe.details.get('inserted_ops', [])]
                        total_moved += len(inserted_ids)
                        
                        dupe_errors = sum(1 for e in bwe.details.get('writeErrors', []) if e.get('code') == 11000)
                        total_failed_duplicates += dupe_errors
                        total_failed_other += len(bwe.details.get('writeErrors', [])) - dupe_errors
                        
                    except Exception as insert_e:
                        logger.error(f"Error inserting batch from {source_db_name} to {active_db_name}: {insert_e}")
                        total_failed_other += len(docs_to_move)
                        continue # Skip deletion if insert failed badly

                    # Delete *only* the successfully inserted documents from the source DB
                    if inserted_ids:
                        try:
                            await loop.run_in_executor(None, partial(source_coll.delete_many, {'_id': {'$in': inserted_ids}}))
                            logger.info(f"Moved {len(inserted_ids)} docs from {source_db_name} to {active_db_name}.")
                        except Exception as del_e:
                            logger.error(f"CRITICAL: Failed to delete moved docs from {source_db_name}! {del_e}")
                            await sts_msg.edit(f"❌ ᴄʀɪᴛɪᴄᴀʟ ᴇʀʀᴏʀ!\nꜰᴀɪʟᴇᴅ ᴛᴏ ᴅᴇʟᴇᴛᴇ {len(inserted_ids)} ᴅᴏᴄꜱ ꜰʀᴏᴍ {source_db_name} ᴀꜰᴛᴇʀ ᴛʀᴀɴꜱꜰᴇʀ.\n\nᴘʟᴇᴀꜱᴇ ᴄʜᴇᴄᴋ ʟᴏɢꜱ. /cleanmultdb ɪꜱ ʀᴇᴄᴏᴍᴍᴇɴᴅᴇᴅ.")
                            return # Stop immediately
                    
                except Exception as e:
                    logger.error(f"Error processing {source_db_name} for topdown: {e}", exc_info=True)
                    total_failed_other += limit # Estimate failure
            
            elapsed = get_readable_time(time_now() - start)
            await sts_msg.edit(
                f"✔️ ᴛᴏᴘ-ᴅᴏᴡɴ ᴄᴏᴍᴘʟᴇᴛᴇ!\n\n"
                f"⏱️ ᴛᴏᴏᴋ: <code>{elapsed}</code>\n"
                f"🎯 ᴛᴀʀɢᴇᴛ ᴅʙ: {active_db_name}\n\n"
                f"~ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ᴍᴏᴠᴇᴅ: <code>{total_moved}</code> ꜰɪʟᴇꜱ\n"
                f"~ ꜰᴀɪʟᴇᴅ (ᴅᴜᴘʟɪᴄᴀᴛᴇꜱ): <code>{total_failed_duplicates}</code>\n"
                f"~ ꜰᴀɪʟᴇᴅ (ᴏᴛʜᴇʀ ᴇʀʀᴏʀꜱ): <code>{total_failed_other}</code>"
            )
            
        except Exception as e:
            logger.error(f"Fatal /topdown error: {e}", exc_info=True)
            await sts_msg.edit(f"❌ ꜰᴀᴛᴀʟ ᴇʀʀᴏʀ ᴅᴜʀɪɴɢ ᴛᴏᴘᴅᴏᴡɴ: {e}")
        finally:
            if topdown_lock.locked():
                topdown_lock.release()


@Client.on_message(filters.command('cleanmultdb') & filters.user(ADMINS))
async def clean_multi_db_duplicates(bot, message):
    if len(file_db_collections) < 2:
        return await message.reply("⚠️ ᴏɴʟʏ ᴏɴᴇ ᴅᴀᴛᴀʙᴀꜱᴇ ɪꜱ ᴄᴏɴꜰɪɢᴜʀᴇᴅ. ɴᴏ ᴅᴜᴘʟɪᴄᴀᴛᴇꜱ ᴛᴏ ᴄʟᴇᴀɴ.")
    
    sts_msg = await message.reply("🧹 ꜱᴛᴀʀᴛɪɴɢ ᴍᴇᴍᴏʀʏ-ᴇꜰꜰɪᴄɪᴇɴᴛ ᴅᴜᴘʟɪᴄᴀᴛᴇ ᴄʟᴇᴀɴᴜᴘ...\nᴛʜɪꜱ ᴍɪɢʜᴛ ᴛᴀᴋᴇ ᴀ ᴠᴇʀʏ ʟᴏɴɢ ᴛɪᴍᴇ.")
    loop = asyncio.get_running_loop()
    total_removed = 0
    total_checked = 0
    total_errors = 0
    start = time_now()
    last_update_time = time_now()
    
    BATCH_SIZE = 5000 # Process 5000 IDs at a time

    # This MUST be a 'def', not 'async def', to run in the executor
    def get_batch_ids_sync(cursor, batch_size):
        # This function runs in the executor
        ids = []
        try:
            for _ in range(batch_size):
                # cursor.next() is a blocking call
                ids.append(cursor.next()['_id']) # Only get the _id
        except StopIteration:
            pass # End of cursor
        except Exception as e:
            logger.error(f"Error fetching batch from cursor: {e}")
        return ids

    try:
        # Iterate through each DB, using it as a "master"
        for i, master_collection in enumerate(file_db_collections):
            master_db_name = f"DB #{i+1}"
            logger.info(f"Using {master_db_name} as master, checking subsequent DBs...")
            
            # Get the cursor for the master collection (sync)
            try:
                master_cursor = await loop.run_in_executor(None, partial(master_collection.find, {}, {'_id': 1}))
            except Exception as e:
                logger.error(f"Failed to create cursor for {master_db_name}: {e}")
                total_errors += 1
                continue # Skip this master DB if cursor fails

            while True:
                # 1. Get a batch of IDs from the master DB
                batch_ids = await loop.run_in_executor(None, partial(get_batch_ids_sync, master_cursor, BATCH_SIZE))
                if not batch_ids:
                    break # No more documents in this master collection

                total_checked += len(batch_ids)
                temp_id_set = set(batch_ids) # Small, in-memory set
                
                # 2. Iterate through all *subsequent* databases to clean them
                for j, slave_collection in enumerate(file_db_collections):
                    if i >= j:
                        continue # Don't check against self or previous DBs

                    slave_db_name = f"DB #{j+1}"
                    try:
                        # 3. Delete from slave DB
                        del_res = await loop.run_in_executor(None, partial(slave_collection.delete_many, {'_id': {'$in': list(temp_id_set)}}))
                        deleted_now = del_res.deleted_count if del_res else 0
                        if deleted_now > 0:
                            total_removed += deleted_now
                            logger.info(f"Removed {deleted_now} duplicates from {slave_db_name} (found in {master_db_name}). Total removed: {total_removed}")
                    except Exception as del_e:
                        logger.error(f"Error removing batch from {slave_db_name}: {del_e}")
                        total_errors += len(temp_id_set) # Estimate error count
                
                # Update status message periodically
                current_time = time_now()
                if current_time - last_update_time > 15: # Update every 15 seconds
                     elapsed = get_readable_time(current_time - start)
                     status_text = (
                         f"🧹 ᴄʟᴇᴀɴɪɴɢ ᴅᴜᴘʟɪᴄᴀᴛᴇꜱ...\n"
                         f"~ ᴍᴀꜱᴛᴇʀ ᴅʙ: <code>{master_db_name}</code>\n"
                         f"~ ᴄʜᴇᴄᴋᴇᴅ (ᴛᴏᴛᴀʟ): <code>{total_checked}</code>\n"
                         f"~ ʀᴇᴍᴏᴠᴇᴅ (ᴛᴏᴛᴀʟ): <code>{total_removed}</code>\n"
                         f"~ ᴇʀʀᴏʀꜱ: <code>{total_errors}</code>\n"
                         f"~ ᴇʟᴀᴘꜱᴇᴅ: <code>{elapsed}</code>"
                     )
                     try: await sts_msg.edit_text(status_text)
                     except FloodWait as e: await asyncio.sleep(e.value)
                     except MessageNotModified: pass
                     except Exception as edit_e: logger.warning(f"Cleanup status edit error: {edit_e}")
                     last_update_time = current_time
            
            # Clean up cursor for this master collection
            try:
                await loop.run_in_executor(None, master_cursor.close)
            except Exception as e_close:
                 logger.warning(f"Error closing cursor for {master_db_name}: {e_close}")
            
            logger.info(f"Finished using {master_db_name} as master.")

        # After iterating all DBs
        elapsed = get_readable_time(time_now() - start)
        await sts_msg.edit_text(
            f"✔️ ᴄʀᴏꜱꜱ-ᴅʙ ᴄʟᴇᴀɴᴜᴘ ᴄᴏᴍᴘʟᴇᴛᴇ!\n\n"
            f"⏱️ ᴛᴏᴏᴋ: <code>{elapsed}</code>\n\n"
            f"~ ᴛᴏᴛᴀʟ ᴄʜᴇᴄᴋᴇᴅ (ᴀᴘᴘʀᴏx): <code>{total_checked}</code>\n"
            f"~ ᴛᴏᴛᴀʟ ᴅᴜᴘʟɪᴄᴀᴛᴇꜱ ʀᴇᴍᴏᴠᴇᴅ: <code>{total_removed}</code>\n"
            f"~ ᴇʀʀᴏʀꜱ (ᴇꜱᴛɪᴍᴀᴛᴇ): <code>{total_errors}</code>"
        )

    except Exception as e:
        logger.error(f"/cleanmultdb error: {e}", exc_info=True)
        await sts_msg.edit(f"❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴅᴜʀɪɴɢ ᴄʟᴇᴀɴᴜᴘ: {e}")
    finally:
        if topdown_lock.locked():
            topdown_lock.release()


@Client.on_message(filters.command('set_fsub') & filters.user(ADMINS))
async def set_fsub_cmd(bot, message):
    try: _, ids_text = message.text.split(' ', 1)
    except ValueError: return await message.reply('ᴜꜱᴀɢᴇ: /ꜱᴇᴛ_ꜰꜱᴜʙ -100xxx -100xxx ... (ꜱᴇᴘᴀʀᴀᴛᴇ ɪᴅꜱ ᴡɪᴛʜ ꜱᴘᴀᴄᴇꜱ)')
    title = ""; valid_ids = []
    for id_str in ids_text.split():
        try:
            chat_id = int(id_str)
            chat = await bot.get_chat(chat_id)
            title += f' • {chat.title} (`{chat_id}`)\n'; valid_ids.append(str(chat_id))
        except ValueError: return await message.reply(f'⚠️ ɪɴᴠᴀʟɪᴅ ɪᴅ: `{id_str}`. ɪᴅꜱ ᴍᴜꜱᴛ ʙᴇ ɪɴᴛᴇɢᴇʀꜱ.')
        except Exception as e: return await message.reply(f'❌ ᴇʀʀᴏʀ ɢᴇᴛᴛɪɴɢ ᴄʜᴀᴛ ɪɴꜰᴏ ꜰᴏʀ `{id_str}`: {e}')
    if not valid_ids: return await message.reply('⚠️ ɴᴏ ᴠᴀʟɪᴅ ᴄʜᴀɴɴᴇʟ ɪᴅꜱ ᴘʀᴏᴠɪᴅᴇᴅ.')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'FORCE_SUB_CHANNELS', " ".join(valid_ids))
    await message.reply(f'✔️ ꜰᴏʀᴄᴇ ꜱᴜʙꜱᴄʀɪʙᴇ ᴄʜᴀɴɴᴇʟꜱ ᴜᴘᴅᴀᴛᴇᴅ:\n{title}')

@Client.on_message(filters.command('set_req_fsub') & filters.user(ADMINS))
async def set_req_fsub_cmd(bot, message):
    try: _, id_str = message.text.split(' ', 1)
    except ValueError: return await message.reply('ᴜꜱᴀɢᴇ: /ꜱᴇᴛ_ʀᴇǫ_ꜰꜱᴜʙ <ᴄʜᴀɴɴᴇʟ_ɪᴅ>')
    try:
        chat_id = int(id_str)
        chat = await bot.get_chat(chat_id)
    except ValueError: return await message.reply(f'⚠️ ɪɴᴠᴀʟɪᴅ ɪᴅ: `{id_str}`. ɪᴅ ᴍᴜꜱᴛ ʙᴇ ᴀɴ ɪɴᴛᴇɢᴇʀ.')
    except Exception as e: return await message.reply(f'❌ ᴇʀʀᴏʀ ɢᴇᴛᴛɪɴɢ ᴄʜᴀᴛ ɪɴꜰᴏ ꜰᴏʀ `{id_str}`: {e}')
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'REQUEST_FORCE_SUB_CHANNELS', str(chat_id))
    await message.reply(f'✔️ ʀᴇǫᴜᴇꜱᴛ ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ ꜱᴇᴛ ᴛᴏ: {chat.title} (`{chat_id}`)')

@Client.on_message(filters.command('off_auto_filter') & filters.user(ADMINS))
async def off_auto_filter_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', False); await message.reply('✔️ ɢʟᴏʙᴀʟ ᴀᴜᴛᴏ ꜰɪʟᴛᴇʀ ʜᴀꜱ ʙᴇᴇɴ **ᴅɪꜱᴀʙʟᴇᴅ**.')

@Client.on_message(filters.command('on_auto_filter') & filters.user(ADMINS))
async def on_auto_filter_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'AUTO_FILTER', True); await message.reply('✔️ ɢʟᴏʙᴀʟ ᴀᴜᴛᴏ ꜰɪʟᴛᴇʀ ʜᴀꜱ ʙᴇᴇɴ **ᴇɴᴀʙʟᴇᴅ**.')

@Client.on_message(filters.command('off_pm_search') & filters.user(ADMINS))
async def off_pm_search_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', False); await message.reply('✔️ ᴘᴍ ꜰɪʟᴇ ꜱᴇᴀʀᴄʜ ʜᴀꜱ ʙᴇᴇɴ **ᴅɪꜱᴀʙʟᴇᴅ**.')

@Client.on_message(filters.command('on_pm_search') & filters.user(ADMINS))
async def on_pm_search_cmd(bot, message):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_bot_sttgs, 'PM_SEARCH', True); await message.reply('✔️ ᴘᴍ ꜰɪʟᴇ ꜱᴇᴀʀᴄʜ ʜᴀꜱ ʙᴇᴇɴ **ᴇɴᴀʙʟᴇᴅ**.')