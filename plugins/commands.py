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
# Removed db_count_documents, second_db_count_documents import if now fetched via db methods
from database.ia_filterdb import get_file_details, delete_files, db_count_documents, second_db_count_documents
from database.users_chats_db import db
from datetime import datetime, timedelta # Keep timedelta
# Removed IS_PREMIUM imports
from info import (URL, BIN_CHANNEL, SECOND_FILES_DATABASE_URL, INDEX_CHANNELS, ADMINS,
                  IS_VERIFY, VERIFY_TUTORIAL, VERIFY_EXPIRE, SHORTLINK_API, SHORTLINK_URL,
                  DELETE_TIME, SUPPORT_LINK, UPDATES_LINK, LOG_CHANNEL, PICS, IS_STREAM,
                  PM_FILE_DELETE_TIME, BOT_ID) # Added BOT_ID
from utils import (get_settings, get_size, is_subscribed, is_check_admin, get_shortlink,
                   get_verify_status, update_verify_status, save_group_settings, temp,
                   get_readable_time, get_wish, get_seconds, upload_image) # Removed is_premium
# Import collections for cleanup command
from database.ia_filterdb import collection as primary_collection, second_collection
from hydrogram.errors import MessageNotModified, FloodWait # Import specific errors
import logging # Add logging

logger = logging.getLogger(__name__)

# --- Start Command ---
@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    # Group Join Handling
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await client.loop.run_in_executor(None, db.grp.find_one, {'id': message.chat.id}): # Check if group exists (run sync in executor)
            try:
                total = await client.get_chat_members_count(message.chat.id)
                username = f'@{message.chat.username}' if message.chat.username else 'ᴘʀɪᴠᴀᴛᴇ'
                await client.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(message.chat.title, message.chat.id, username, total))
                await client.loop.run_in_executor(None, db.add_chat, message.chat.id, message.chat.title) # Run sync in executor
            except Exception as e:
                logger.error(f"Error logging new group {message.chat.id}: {e}")
        # Simple reply in group
        wish = get_wish()
        user = message.from_user.mention if message.from_user else "ᴅᴇᴀʀ"
        btn = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK),
                 InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
        await message.reply(text=f"<b>ʜᴇʏ {user}, <i>{wish}</i>\nʜᴏᴡ ᴄᴀɴ ɪ ʜᴇʟᴘ ʏᴏᴜ?</b>", reply_markup=InlineKeyboardMarkup(btn))
        return

    # New User Handling in PM
    user_id = message.from_user.id
    if not await client.loop.run_in_executor(None, db.is_user_exist, user_id): # Run sync in executor
        try:
             await client.loop.run_in_executor(None, db.add_user, user_id, message.from_user.first_name) # Run sync in executor
             await client.send_message(LOG_CHANNEL, script.NEW_USER_TXT.format(message.from_user.mention, user_id))
        except Exception as e:
             logger.error(f"Error adding new user {user_id}: {e}")

    # Verify Status Check (remains, no premium bypass)
    verify_status = await get_verify_status(user_id)
    if verify_status['is_verified'] and isinstance(verify_status['expire_time'], datetime) and datetime.now(pytz.utc) > verify_status['expire_time'].replace(tzinfo=pytz.utc): # Check for valid datetime and compare TZ aware
        logger.info(f"Verification expired for user {user_id}")
        await update_verify_status(user_id, is_verified=False) # Expire verification

    # --- Start Parameter Handling ---
    if len(message.command) == 1 or message.command[1] == 'start':
        # Default start message
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

    # Settings parameter handling
    if mc.startswith('settings'):
        try:
             _, group_id_str = mc.split("_", 1)
             group_id = int(group_id_str)
        except (ValueError, IndexError):
             return await message.reply("Invalid settings link.")

        if not await is_check_admin(client, group_id, user_id):
            return await message.reply("ʏᴏᴜ ᴀʀᴇ ɴᴏᴛ ᴀɴ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴀᴛ ɢʀᴏᴜᴘ.")
        try:
             btn = await get_grp_stg(group_id) # Ensure this is async
             chat = await client.get_chat(group_id)
             await message.reply(f"⚙️ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        except Exception as e:
             logger.error(f"Error opening settings via PM link for group {group_id}: {e}")
             await message.reply("Could not fetch settings for that group.")
        return

    # Inline Fsub parameter handling
    if mc == 'inline_fsub':
        btn = await is_subscribed(client, message) # is_subscribed needs the message object
        if btn:
            await message.reply(f"❗ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ(s) ʙᴇʟᴏᴡ ᴛᴏ ᴜsᴇ ɪɴʟɪɴᴇ sᴇᴀʀᴄʜ.",
                reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML )
        else:
             await message.reply("✅ ʏᴏᴜ ᴀʀᴇ ᴀʟʀᴇᴀᴅʏ sᴜʙsᴄʀɪʙᴇᴅ. ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜsᴇ ɪɴʟɪɴᴇ sᴇᴀʀᴄʜ.")
        return

    # Verify parameter handling
    if mc.startswith('verify_'):
        try:
             _, token = mc.split("_", 1)
        except ValueError:
             return await message.reply("Invalid verification link.")

        verify_status = await get_verify_status(user_id)
        if verify_status['verify_token'] != token:
            return await message.reply("❌ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ᴛᴏᴋᴇɴ ɪs ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ.")

        expiry_time = datetime.now(pytz.utc) + timedelta(seconds=VERIFY_EXPIRE)
        await update_verify_status(user_id, is_verified=True, expire_time=expiry_time, verify_token="") # Clear token
        link_to_get = verify_status.get("link", "") # Get stored link if any

        reply_markup = None
        if link_to_get:
            btn = [[ InlineKeyboardButton("📌 ɢᴇᴛ ғɪʟᴇ", url=f'https://t.me/{temp.U_NAME}?start={link_to_get}') ]]
            reply_markup = InlineKeyboardMarkup(btn)

        await message.reply(f"✅ ᴠᴇʀɪғɪᴇᴅ sᴜᴄᴄᴇssғᴜʟʟʏ!\n\nʏᴏᴜ ᴄᴀɴ ᴜsᴇ ᴍᴇ ᴜɴᴛɪʟ: {expiry_time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
                           reply_markup=reply_markup, protect_content=True)
        return

    # --- File Request Handling (Triggered by start parameter like file_groupid_fileid) ---
    # FSub Check
    btn_fsub = await is_subscribed(client, message) # Use message object
    if btn_fsub:
        btn_fsub.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
        await message.reply_photo(
            photo=random.choice(PICS),
            caption=f"👋 ʜᴇʟʟᴏ {message.from_user.mention},\n\nᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴍʏ ᴄʜᴀɴɴᴇʟ(s) ᴀɴᴅ ᴛʀʏ ᴀɢᴀɪɴ. 👇",
            reply_markup=InlineKeyboardMarkup(btn_fsub),
            parse_mode=enums.ParseMode.HTML
        )
        return

    # Verification Check (No premium bypass)
    verify_status = await get_verify_status(user_id)
    # Check if verified and if expiry time is valid and in the past
    is_expired = isinstance(verify_status['expire_time'], datetime) and datetime.now(pytz.utc) > verify_status['expire_time'].replace(tzinfo=pytz.utc)

    if IS_VERIFY and (not verify_status['is_verified'] or is_expired):
        if is_expired: await update_verify_status(user_id, is_verified=False) # Mark as not verified if expired
        token = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        # Store the original file request parameter (`mc`) in the link field
        await update_verify_status(user_id, verify_token=token, link=mc)
        try:
             settings = await get_settings(int(mc.split("_")[1])) # Get settings from group ID in param
             short_url, short_api = settings.get('url', SHORTLINK_URL), settings.get('api', SHORTLINK_API)
             tutorial = settings.get('tutorial', VERIFY_TUTORIAL)
        except (IndexError, ValueError, TypeError): # Handle cases where group ID is invalid or not in param
             short_url, short_api, tutorial = SHORTLINK_URL, SHORTLINK_API, VERIFY_TUTORIAL

        verify_link = f'https://t.me/{temp.U_NAME}?start=verify_{token}'
        try:
             short_link = await get_shortlink(short_url, short_api, verify_link)
        except Exception as e:
             logger.error(f"Error creating shortlink for verify: {e}")
             short_link = verify_link # Fallback to direct link

        btn_verify = [[ InlineKeyboardButton("🧿 ᴠᴇʀɪғʏ", url=short_link) ],
                      [ InlineKeyboardButton('❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ', url=tutorial) ]]
        await message.reply("🔐 ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ!\n\nᴘʟᴇᴀsᴇ ᴄᴏᴍᴘʟᴇᴛᴇ ᴛʜᴇ ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ғɪʟᴇ.",
                           reply_markup=InlineKeyboardMarkup(btn_verify), protect_content=True)
        return

    # --- Process File Request (User is Subscribed and Verified/Verification Off) ---
    try:
        if mc.startswith('all'):
            _, grp_id, key = mc.split("_", 2)
            grp_id = int(grp_id)
            files = temp.FILES.get(key)
            if not files: return await message.reply('❌ ʟɪɴᴋ ᴇxᴘɪʀᴇᴅ ᴏʀ ɪɴᴠᴀʟɪᴅ.')

            settings = await get_settings(grp_id)
            sent_messages = []
            total_files_msg = await message.reply(f"<b><i>🗂️ sᴇɴᴅɪɴɢ <code>{len(files)}</code> ғɪʟᴇs...</i></b>")

            for file_doc in files:
                file_id = file_doc['_id'] # Use the file ID from the cached list
                caption_text = file_doc.get('caption', '') # Use cached caption

                CAPTION = settings.get('caption', script.FILE_CAPTION) # Get group specific or default caption
                try:
                    f_caption = CAPTION.format(
                        file_name = file_doc.get('file_name', 'N/A'),
                        file_size = get_size(file_doc.get('file_size', 0)),
                        file_caption = caption_text if caption_text else ""
                    )
                except KeyError as e:
                     logger.warning(f"Caption format error in group {grp_id}: {e}. Using filename.")
                     f_caption = file_doc.get('file_name', 'N/A')
                except Exception as e:
                     logger.error(f"Error formatting caption: {e}")
                     f_caption = file_doc.get('file_name', 'N/A')


                stream_btn = []
                if IS_STREAM:
                     stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]]

                other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK),
                                InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]

                reply_markup = InlineKeyboardMarkup(stream_btn + other_btns)

                try:
                    msg_sent = await client.send_cached_media(
                        chat_id=user_id,
                        file_id=file_id,
                        caption=f_caption[:1024], # Enforce caption limit
                        protect_content=settings.get('file_secure', PROTECT_CONTENT),
                        reply_markup=reply_markup
                    )
                    sent_messages.append(msg_sent.id)
                    await asyncio.sleep(0.5) # Small delay between sends
                except FloodWait as e:
                     logger.warning(f"FloodWait sending file {file_id} to {user_id}: sleeping {e.value}s")
                     await asyncio.sleep(e.value)
                     # Retry sending the same file
                     try:
                          msg_sent = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup)
                          sent_messages.append(msg_sent.id)
                     except Exception as retry_e:
                          logger.error(f"Failed to send file {file_id} to {user_id} after retry: {retry_e}")
                except Exception as e:
                     logger.error(f"Error sending file {file_id} to {user_id}: {e}")

            # Auto-delete logic
            pm_delete_time = PM_FILE_DELETE_TIME
            time_readable = get_readable_time(pm_delete_time)
            info_msg = await message.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜᴇsᴇ ғɪʟᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_readable}</b> ᴛᴏ ᴀᴠᴏɪᴅ ᴄᴏᴘʏʀɪɢʜᴛ ɪssᴜᴇs. sᴀᴠᴇ ᴛʜᴇᴍ ᴇʟsᴇᴡʜᴇʀᴇ.")

            await asyncio.sleep(pm_delete_time)

            try:
                 await client.delete_messages(chat_id=user_id, message_ids=sent_messages + [total_files_msg.id])
                 logger.info(f"Auto-deleted {len(sent_messages)} files for user {user_id} (Batch Key: {key})")
            except Exception as e:
                 logger.error(f"Error auto-deleting batch messages for user {user_id}: {e}")

            btns_after_del = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ғɪʟᴇs ᴀɢᴀɪɴ', callback_data=f"get_del_send_all_files#{grp_id}#{key}") ]]
            try:
                await info_msg.edit("❗️ ᴛʜᴇ ғɪʟᴇs ʜᴀᴠᴇ ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(btns_after_del))
            except: pass # Ignore if info message already deleted

            return # End processing for 'all'

        # --- Single File Request ---
        elif mc.startswith(('file_', 'shortlink_')):
            if mc.startswith('file_'): type_, grp_id, file_id = mc.split("_", 2)
            else: type_, grp_id, file_id = mc.split("_", 2) # Handling shortlink trigger

            grp_id = int(grp_id)
            settings = await get_settings(grp_id)
            # Fetch details from DB
            files_ = await get_file_details(file_id) # Ensure this is async or wrapped
            if not files_: return await message.reply('❌ ɴᴏ sᴜᴄʜ ғɪʟᴇ ᴇxɪsᴛs.')
            # file_details function returns a list, get the first item
            file_doc = files_[0] if isinstance(files_, list) and files_ else None
            if not file_doc: return await message.reply('❌ ᴇʀʀᴏʀ ɢᴇᴛᴛɪɴɢ ғɪʟᴇ ᴅᴇᴛᴀɪʟs.')

            # Shortlink check (group setting based)
            if type_ != 'shortlink' and settings.get('shortlink', False): # Default to False
                short_url = settings.get('url', SHORTLINK_URL)
                short_api = settings.get('api', SHORTLINK_API)
                tutorial = settings.get('tutorial', TUTORIAL)
                original_link = f"https://t.me/{temp.U_NAME}?start=shortlink_{grp_id}_{file_id}"
                try:
                     short_link = await get_shortlink(short_url, short_api, original_link)
                except Exception as e:
                     logger.error(f"Error creating shortlink for file {file_id}: {e}")
                     short_link = original_link # Fallback

                btn_short = [[ InlineKeyboardButton("♻️ ɢᴇᴛ ғɪʟᴇ", url=short_link) ],
                             [ InlineKeyboardButton("❓ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ", url=tutorial) ]]
                file_name_display = file_doc.get('file_name', 'ғɪʟᴇ')
                file_size_display = get_size(file_doc.get('file_size', 0))
                await message.reply(f"[{file_size_display}] {file_name_display}\n\nʏᴏᴜʀ ғɪʟᴇ ɪs ʀᴇᴀᴅʏ. ᴘʟᴇᴀsᴇ ᴜsᴇ ᴛʜɪs ʟɪɴᴋ ᴛᴏ ɢᴇᴛ ɪᴛ. 👇",
                                   reply_markup=InlineKeyboardMarkup(btn_short), protect_content=True)
                return

            # --- Direct File Sending ---
            CAPTION = settings.get('caption', script.FILE_CAPTION)
            caption_text = file_doc.get('caption', '')
            try:
                f_caption = CAPTION.format(
                    file_name = file_doc.get('file_name', 'N/A'),
                    file_size = get_size(file_doc.get('file_size', 0)),
                    file_caption= caption_text if caption_text else ""
                )
            except KeyError as e:
                 logger.warning(f"Caption format error in group {grp_id}: {e}. Using filename.")
                 f_caption = file_doc.get('file_name', 'N/A')
            except Exception as e:
                 logger.error(f"Error formatting caption: {e}")
                 f_caption = file_doc.get('file_name', 'N/A')

            stream_btn = []; other_btns = []
            if IS_STREAM: stream_btn = [[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ & ᴅᴏᴡɴʟᴏᴀᴅ", callback_data=f"stream#{file_id}") ]]
            other_btns = [[ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
            reply_markup = InlineKeyboardMarkup(stream_btn + other_btns)

            try:
                vp = await client.send_cached_media(
                    chat_id=user_id,
                    file_id=file_id,
                    caption=f_caption[:1024],
                    protect_content=settings.get('file_secure', PROTECT_CONTENT),
                    reply_markup=reply_markup
                )
            except FloodWait as e:
                 logger.warning(f"FloodWait sending file {file_id} to {user_id}: sleeping {e.value}s")
                 await asyncio.sleep(e.value)
                 # Retry
                 try:
                      vp = await client.send_cached_media(chat_id=user_id, file_id=file_id, caption=f_caption[:1024], protect_content=settings.get('file_secure', PROTECT_CONTENT), reply_markup=reply_markup)
                 except Exception as retry_e:
                      logger.error(f"Failed send file {file_id} to {user_id} after retry: {retry_e}")
                      await message.reply("❌ sᴏʀʀʏ, ᴄᴏᴜʟᴅ ɴᴏᴛ sᴇɴᴅ ᴛʜᴇ ғɪʟᴇ ᴀᴛ ᴛʜɪs ᴍᴏᴍᴇɴᴛ.")
                      return
            except Exception as e:
                 logger.error(f"Error sending file {file_id} to {user_id}: {e}")
                 await message.reply("❌ sᴏʀʀʏ, ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ sᴇɴᴅɪɴɢ ᴛʜᴇ ғɪʟᴇ.")
                 return

            # Auto-delete logic for single file
            pm_delete_time = PM_FILE_DELETE_TIME
            time_readable = get_readable_time(pm_delete_time)
            msg = await vp.reply(f"⚠️ ɴᴏᴛᴇ: ᴛʜɪs ғɪʟᴇ ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ɪɴ <b>{time_readable}</b>.", quote=True)
            await asyncio.sleep(pm_delete_time)

            btns_after_del = [[ InlineKeyboardButton('🔄 ɢᴇᴛ ғɪʟᴇ ᴀɢᴀɪɴ', callback_data=f"get_del_file#{grp_id}#{file_id}") ]]
            try: await msg.delete(); logger.debug(f"Deleted timer msg for file {file_id}")
            except: pass
            try: await vp.delete(); logger.info(f"Auto-deleted file {file_id} for user {user_id}")
            except Exception as e: logger.error(f"Error auto-deleting file message {vp.id}: {e}")

            # Send the "File is gone" message only if the original message still exists (avoid error after manual deletion)
            try:
                 await message.reply("❗️ ᴛʜᴇ ғɪʟᴇ ʜᴀs ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ.", reply_markup=InlineKeyboardMarkup(btns_after_del))
            except Exception as final_reply_e:
                 logger.warning(f"Could not send 'file gone' message to user {user_id}: {final_reply_e}")
            return # End processing for single file

        else:
             await message.reply("❓ ɪɴᴠᴀʟɪᴅ sᴛᴀʀᴛ ᴘᴀʀᴀᴍᴇᴛᴇʀ.")

    except Exception as e:
        logger.error(f"Error processing start parameter '{mc}': {e}", exc_info=True)
        await message.reply("❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇssɪɴɢ ʏᴏᴜʀ ʀᴇǫᴜᴇsᴛ.")


# --- Link Command ---
@Client.on_message(filters.command('link'))
async def link(bot, message):
    # This command remains, used for streaming links (no premium check needed)
    msg = message.reply_to_message
    if not msg: return await message.reply('⚠️ ᴘʟᴇᴀsᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴍᴇᴅɪᴀ ғɪʟᴇ.')

    media = getattr(msg, msg.media.value, None) if msg.media else None
    if not media or not hasattr(media, 'file_id'):
        return await message.reply('⚠️ ᴜɴsᴜᴘᴘᴏʀᴛᴇᴅ ғɪʟᴇ ᴛʏᴘᴇ.')

    try:
        # Check if IS_STREAM is enabled globally
        if not IS_STREAM:
             return await message.reply('🖥️ sᴛʀᴇᴀᴍɪɴɢ ғᴇᴀᴛᴜʀᴇ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴅɪsᴀʙʟᴇᴅ.')

        # Send to BIN_CHANNEL to get a message ID for the link
        try:
             stream_msg = await bot.send_cached_media(chat_id=BIN_CHANNEL, file_id=media.file_id)
        except Exception as cache_err:
             logger.error(f"Error sending to BIN_CHANNEL {BIN_CHANNEL}: {cache_err}")
             return await message.reply("❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ɢᴇɴᴇʀᴀᴛᴇ ʟɪɴᴋ. ʙɪɴ ᴄʜᴀɴɴᴇʟ ᴇʀʀᴏʀ.")

        # Generate links using the message ID from BIN_CHANNEL
        watch_url = f"{URL}watch/{stream_msg.id}"
        download_url = f"{URL}download/{stream_msg.id}"
        btn=[[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ ᴏɴʟɪɴᴇ", url=watch_url),
               InlineKeyboardButton("📥 ғᴀsᴛ ᴅᴏᴡɴʟᴏᴀᴅ", url=download_url)],
             [ InlineKeyboardButton('❌ ᴄʟᴏsᴇ', callback_data='close_data') ]]
        await message.reply('✅ ʜᴇʀᴇ ᴀʀᴇ ʏᴏᴜʀ ʟɪɴᴋs:', reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e:
        logger.error(f"Error generating stream/download links: {e}", exc_info=True)
        await message.reply('❌ sᴏʀʀʏ, ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ.')

# --- Other Commands (channels_info, stats, get_grp_stg, settings, connect, delete, img_2_link, ping) remain largely the same ---
# Ensure 'stats' uses the fixed version with run_in_executor and correct formatting

# --- Add /cleanmultdb Command ---
@Client.on_message(filters.command('cleanmultdb') & filters.user(ADMINS))
async def clean_multi_db_duplicates(bot, message):
    if not SECOND_FILES_DATABASE_URL or second_collection is None:
        return await message.reply("⚠️ sᴇᴄᴏɴᴅᴀʀʏ ᴅᴀᴛᴀʙᴀsᴇ ɪs ɴᴏᴛ ᴄᴏɴғɪɢᴜʀᴇᴅ ᴏʀ ᴄᴏɴɴᴇᴄᴛᴇᴅ.")

    sts_msg = await message.reply("🧹 sᴛᴀʀᴛɪɴɢ ᴄʀᴏss-ᴅʙ ᴅᴜᴘʟɪᴄᴀᴛᴇ ᴄʟᴇᴀɴᴜᴘ...\nᴛʜɪs ᴍᴀʏ ᴛᴀᴋᴇ sᴏᴍᴇ ᴛɪᴍᴇ.")
    loop = asyncio.get_event_loop()
    removed_count = 0; checked_count = 0; error_count = 0
    start_time = time_now()

    try:
        # Fetch primary IDs (run sync in executor)
        logger.info("Fetching primary DB IDs for cleanup...")
        primary_ids_cursor = primary_collection.find({}, {'_id': 1})
        # Use lambda to iterate cursor within executor
        primary_ids = await loop.run_in_executor(None, lambda: {doc['_id'] for doc in primary_ids_cursor})
        primary_count = len(primary_ids)
        logger.info(f"Found {primary_count} IDs in primary DB.")

        if primary_count == 0:
             await sts_msg.edit("🧹 ᴘʀɪᴍᴀʀʏ ᴅᴀᴛᴀʙᴀsᴇ ɪs ᴇᴍᴘᴛʏ. ɴᴏᴛʜɪɴɢ ᴛᴏ ᴄʟᴇᴀɴ.")
             return

        # Iterate secondary DB (run sync in executor)
        logger.info("Iterating through secondary DB...")
        secondary_docs_cursor = second_collection.find({}, {'_id': 1})

        BATCH_SIZE = 1000 # Process in batches
        processed_in_batch = 0

        # Create generator in executor for efficient iteration
        def secondary_iterator():
            for doc in secondary_docs_cursor:
                yield doc

        doc_generator = await loop.run_in_executor(None, secondary_iterator)

        ids_to_remove_batch = []

        for doc in doc_generator:
            checked_count += 1
            processed_in_batch += 1

            if doc['_id'] in primary_ids:
                ids_to_remove_batch.append(doc['_id'])

            # Process removal in batches
            if len(ids_to_remove_batch) >= BATCH_SIZE:
                if ids_to_remove_batch:
                    try:
                        delete_result = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove_batch}}))
                        deleted_now = delete_result.deleted_count
                        removed_count += deleted_now
                        logger.info(f"Removed {deleted_now} duplicates from secondary (Batch).")
                    except Exception as del_e:
                         logger.error(f"Error removing batch: {del_e}")
                         error_count += len(ids_to_remove_batch)
                ids_to_remove_batch = [] # Reset batch

            # Update status periodically
            if checked_count % (BATCH_SIZE * 10) == 0: # Update less frequently
                 elapsed = get_readable_time(time_now() - start_time)
                 try:
                     await sts_msg.edit_text(f"🧹 ᴄʟᴇᴀɴɪɴɢ...\n"
                                             f"~ ᴄʜᴇᴄᴋᴇᴅ: <code>{checked_count}</code> (sᴇᴄ)\n"
                                             f"~ ʀᴇᴍᴏᴠᴇᴅ: <code>{removed_count}</code>\n"
                                             f"~ ᴇʀʀᴏʀs: <code>{error_count}</code>\n"
                                             f"~ ᴇʟᴀᴘsᴇᴅ: <code>{elapsed}</code>")
                 except FloodWait as e: await asyncio.sleep(e.value)
                 except MessageNotModified: pass

        # Process any remaining items in the last batch
        if ids_to_remove_batch:
            try:
                delete_result = await loop.run_in_executor(None, partial(second_collection.delete_many, {'_id': {'$in': ids_to_remove_batch}}))
                deleted_now = delete_result.deleted_count
                removed_count += deleted_now
                logger.info(f"Removed {deleted_now} duplicates from secondary (Final Batch).")
            except Exception as del_e:
                 logger.error(f"Error removing final batch: {del_e}")
                 error_count += len(ids_to_remove_batch)

        elapsed = get_readable_time(time_now() - start_time)
        await sts_msg.edit_text(f"✅ ᴄʟᴇᴀɴᴜᴘ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!\n\n"
                                f"~ ᴛᴏᴏᴋ: <code>{elapsed}</code>\n"
                                f"~ ᴄʜᴇᴄᴋᴇᴅ (sᴇᴄ): <code>{checked_count}</code>\n"
                                f"~ ʀᴇᴍᴏᴠᴇᴅ: <code>{removed_count}</code>\n"
                                f"~ ᴇʀʀᴏʀs: <code>{error_count}</code>")

    except Exception as e:
        logger.error(f"Error during /cleanmultdb: {e}", exc_info=True)
        await sts_msg.edit_text(f"❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ: {e}")


# --- [Ensure get_grp_stg, settings, connect functions are present and correct] ---
# Make sure get_grp_stg is async if it needs to be
async def get_grp_stg(group_id):
    settings = await get_settings(group_id) # Ensure await is used
    # Rebuild button list using current settings values
    btn = [[# Edit IMDb template
            InlineKeyboardButton('ɪᴍᴅʙ ᴛᴇᴍᴘʟᴀᴛᴇ', callback_data=f'imdb_setgs#{group_id}')],
           [# Edit Shortlink
            InlineKeyboardButton('sʜᴏʀᴛʟɪɴᴋ', callback_data=f'shortlink_setgs#{group_id}')],
           [# Edit File Caption
            InlineKeyboardButton('ғɪʟᴇ ᴄᴀᴘᴛɪᴏɴ', callback_data=f'caption_setgs#{group_id}')],
           [# Edit Welcome Message
            InlineKeyboardButton('ᴡᴇʟᴄᴏᴍᴇ ᴍᴇssᴀɢᴇ', callback_data=f'welcome_setgs#{group_id}')],
           [# Edit Tutorial Link
            InlineKeyboardButton('ᴛᴜᴛᴏʀɪᴀʟ ʟɪɴᴋ', callback_data=f'tutorial_setgs#{group_id}')],
           [# Toggle IMDb Poster
            InlineKeyboardButton(f'ᴘᴏsᴛᴇʀ {"✅" if settings.get("imdb", IMDB) else "❌"}', callback_data=f'bool_setgs#imdb#{settings.get("imdb", IMDB)}#{group_id}')],
           [# Toggle Spelling Check
            InlineKeyboardButton(f'sᴘᴇʟʟ ᴄʜᴇᴄᴋ {"✅" if settings.get("spell_check", SPELL_CHECK) else "❌"}', callback_data=f'bool_setgs#spell_check#{settings.get("spell_check", SPELL_CHECK)}#{group_id}')],
           [# Toggle Auto Delete
            InlineKeyboardButton(f'ᴀᴜᴛᴏ ᴅᴇʟᴇᴛᴇ {"✅" if settings.get("auto_delete", AUTO_DELETE) else "❌"}', callback_data=f'bool_setgs#auto_delete#{settings.get("auto_delete", AUTO_DELETE)}#{group_id}')],
           [# Toggle Welcome Message Enable
            InlineKeyboardButton(f'ᴡᴇʟᴄᴏᴍᴇ {"✅" if settings.get("welcome", WELCOME) else "❌"}', callback_data=f'bool_setgs#welcome#{settings.get("welcome", WELCOME)}#{group_id}')],
           [# Toggle Shortlink Enable
            InlineKeyboardButton(f'sʜᴏʀᴛʟɪɴᴋ {"✅" if settings.get("shortlink", SHORTLINK) else "❌"}', callback_data=f'bool_setgs#shortlink#{settings.get("shortlink", SHORTLINK)}#{group_id}')],
           [# Toggle Result Page Style (Link/Button)
            InlineKeyboardButton(f'ʀᴇsᴜʟᴛ ᴘᴀɢᴇ {"ʟɪɴᴋ" if settings.get("links", LINK_MODE) else "ʙᴜᴛᴛᴏɴ"}', callback_data=f'bool_setgs#links#{settings.get("links", LINK_MODE)}#{group_id}')]
          ]
    return btn


# ... (rest of the file, e.g., settings, connect, delete, img_2_link, ping) ...
