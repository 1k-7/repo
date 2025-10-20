import asyncio
import re
from time import time as time_now
import math, os
# import qrcode # Removed: No longer needed without premium plan activation
import random
from hydrogram.errors import ListenerTimeout, MessageNotModified, FloodWait
from hydrogram.errors.exceptions.bad_request_400 import MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty
from Script import script # Use updated Script.py texts
from datetime import datetime, timedelta, timezone # Added timezone
import pytz # Added pytz
# Removed premium related imports from info
from info import (PICS, TUTORIAL, ADMINS, URL, MAX_BTN, BIN_CHANNEL,
                  DELETE_TIME, FILMS_LINK, LOG_CHANNEL, SUPPORT_GROUP, SUPPORT_LINK,
                  UPDATES_LINK, LANGUAGES, QUALITY, IS_STREAM, BOT_ID) # Keep necessary imports
from hydrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto, Message # Added Message type hint
from hydrogram import Client, filters, enums
# Removed is_premium import
from utils import (get_size, is_subscribed, is_check_admin, get_wish,
                   get_shortlink, get_readable_time, get_poster, temp,
                   get_settings, save_group_settings) # is_premium removed
from database.users_chats_db import db
from database.ia_filterdb import get_search_results,delete_files # Removed db counts if only used in stats
from plugins.commands import get_grp_stg # Keep settings related import
import logging # Added logging

logger = logging.getLogger(__name__)

BUTTONS = {}
CAP = {}

@Client.on_message(filters.private & filters.text & filters.incoming)
async def pm_search(client, message: Message): # Added type hint
    if message.text.startswith("/") or not message.text: return # Ignore commands and empty
    stg = db.get_bot_sttgs() # Sync call
    if not stg.get('PM_SEARCH', True): # Default to True
        return await message.reply_text('🔒 ᴘᴍ sᴇᴀʀᴄʜ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴅɪsᴀʙʟᴇᴅ.')

    if not stg.get('AUTO_FILTER', True): # Default to True
        return await message.reply_text('⚙️ ᴀᴜᴛᴏ ғɪʟᴛᴇʀ ᴡᴀs ᴅɪsᴀʙʟᴇᴅ ɢʟᴏʙᴀʟʟʏ.')

    s = await message.reply(f"<b><i>⏳ `{message.text}` sᴇᴀʀᴄʜɪɴɢ...</i></b>", quote=True)
    await auto_filter(client, message, s)


@Client.on_message(filters.group & filters.text & filters.incoming)
async def group_search(client, message: Message): # Added type hint
    if not message.text or message.text.startswith("/"): return
    user_id = message.from_user.id if message and message.from_user else 0
    if not user_id: return # Ignore anonymous admins

    stg = db.get_bot_sttgs()
    if stg.get('AUTO_FILTER', True): # Default to True
        # Support Group handling
        if message.chat.id == SUPPORT_GROUP:
            files, offset, total = await get_search_results(message.text) # Use async search
            if files:
                btn = [[ InlineKeyboardButton("➡️ ɢᴇᴛ ғɪʟᴇs ʜᴇʀᴇ", url=FILMS_LINK) ]]
                await message.reply_text(f'♢ {total} ʀᴇsᴜʟᴛs ғᴏᴜɴᴅ.\n♢ ᴘʟᴇᴀsᴇ sᴇᴀʀᴄʜ ɪɴ ᴛʜᴇ ᴍᴀɪɴ ᴄʜᴀᴛ.', reply_markup=InlineKeyboardMarkup(btn))
            return

        # Admin tag handling
        elif '@admin' in message.text.lower() or '@admins' in message.text.lower():
            if await is_check_admin(client, message.chat.id, user_id): return
            admins = [member.user.id async for member in client.get_chat_members(chat_id=message.chat.id, filter=enums.ChatMembersFilter.ADMINISTRATORS) if not member.user.is_bot]
            owner = next((member async for member in client.get_chat_members(chat_id=message.chat.id, filter=enums.ChatMembersFilter.ADMINISTRATORS) if member.status == enums.ChatMemberStatus.OWNER), None)
            if owner and owner.user.id not in admins : admins.append(owner.user.id)
            if not admins: return await message.reply("Couldn't find admins.")

            mention_target = message.reply_to_message or message
            mention_text = f"#ᴀᴛᴛᴇɴᴛɪᴏɴ\n♢ ᴜsᴇʀ: {message.from_user.mention}\n♢ ɢʀᴏᴜᴘ: {message.chat.title}\n\n♢ <a href={mention_target.link}>ʀᴇᴘᴏʀᴛᴇᴅ ᴍᴇssᴀɢᴇ</a>"
            reported_admin_ids = set()

            for admin_id in admins:
                if admin_id in reported_admin_ids: continue
                try:
                    await mention_target.forward(admin_id)
                    await client.send_message(admin_id, mention_text, disable_web_page_preview=True)
                    reported_admin_ids.add(admin_id)
                    await asyncio.sleep(0.3)
                except Exception as e: logger.error(f"Notify admin {admin_id} error: {e}")

            if reported_admin_ids: await message.reply_text('✅ Report sent!') # No need to tag admins in group
            else: await message.reply_text('❌ Could not notify admins.')
            return

        # Link/Spam handling
        elif re.findall(r'https?://\S+|www\.\S+|t\.me/\S+', message.text):
            if await is_check_admin(client, message.chat.id, user_id): return
            try: await message.delete()
            except Exception as e: logger.warning(f"Delete link msg error {message.chat.id}: {e}")
            return # Just delete

        # Request handling
        elif '#request' in message.text.lower():
            if user_id in ADMINS: return
            if not LOG_CHANNEL: return await message.reply("Request feature disabled.")
            try:
                request_text = re.sub(r'#request', '', message.text, flags=re.IGNORECASE).strip()
                if not request_text: return await message.reply("Specify request after #request.")
                log_msg = f"#ʀᴇǫᴜᴇsᴛ\n♢ ᴜsᴇʀ: {message.from_user.mention} (`{user_id}`)\n♢ ɢʀᴏᴜᴘ: {message.chat.title} (`{message.chat.id}`)\n\n♢ ʀᴇǫᴜᴇsᴛ: {request_text}"
                await client.send_message(LOG_CHANNEL, log_msg)
                await message.reply_text("✅ ʀᴇǫᴜᴇsᴛ sᴇɴᴛ!")
            except Exception as e: logger.error(f"Request log error: {e}"); await message.reply_text("❌ ғᴀɪʟᴇᴅ sᴇɴᴅ.")
            return

        # Normal search
        else:
            if len(message.text) < 2: return # Ignore very short messages
            s = await message.reply(f"<b><i>⏳ `{message.text}` sᴇᴀʀᴄʜɪɴɢ...</i></b>")
            await auto_filter(client, message, s)
    else: # Auto Filter is OFF globally
        pass # Do nothing

@Client.on_callback_query(filters.regex(r"^next"))
async def next_page(bot, query: CallbackQuery): # Added type hint
    ident, req, key, offset_str = query.data.split("_")
    try: req_user_id = int(req); offset = int(offset_str)
    except (ValueError, TypeError): return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != 0 and query.from_user.id != req_user_id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    search = BUTTONS.get(key)
    cap = CAP.get(key)
    if not search or not cap:
        await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)
        try: await query.message.edit_text("❌ sᴇᴀʀᴄʜ ᴇxᴘɪʀᴇᴅ.")
        except: pass
        return

    # Use async search
    files, n_offset, total = await get_search_results(search, offset=offset) # Removed is_premium argument if present
    try: n_offset = int(n_offset) if n_offset else "" # Use empty string for last page offset
    except: n_offset = ""

    if not files:
        await query.answer("No more files.", show_alert=False)
        # Optionally edit message to remove 'Next' button or disable it
        # Example: Modify the existing buttons to remove or change the next button
        # current_markup = query.message.reply_markup
        # new_buttons = ... (rebuild buttons without 'Next')
        # if current_markup and new_buttons != current_markup.inline_keyboard:
        #      try: await query.message.edit_reply_markup(InlineKeyboardMarkup(new_buttons))
        #      except MessageNotModified: pass
        return

    temp.FILES[key] = files # Update cache
    settings = await get_settings(query.message.chat.id) # await settings
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɪɴ {get_readable_time(DELETE_TIME)}.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    # Build results (Link or Button mode)
    if settings.get('links', False):
        for file_num, file in enumerate(files, start=offset+1):
            file_id_str = file.get('_id', 'N/A')
            file_name = file.get('file_name', 'N/A')
            file_size = file.get('file_size', 0)
            files_link += f"<b>\n\n{file_num}. <a href=https://t.me/{temp.U_NAME}?start=file_{query.message.chat.id}_{file_id_str}>[{get_size(file_size)}] {file_name}</a></b>"
    else:
        for file in files:
            file_id_str = file.get('_id', 'N/A')
            file_name = file.get('file_name', 'N/A')[:60] # Limit button text length
            file_size = file.get('file_size', 0)
            btn.append([InlineKeyboardButton(text=f"[{get_size(file_size)}] {file_name}", callback_data=f"file#{file_id_str}")])

    # Standard filter/update buttons
    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"), # Reset offset for filters
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0")
    ])

    # Pagination Logic
    current_page = math.ceil(offset / MAX_BTN) + 1
    total_pages = math.ceil(total / MAX_BTN)
    page_label = f"ᴘɢ {current_page}/{total_pages}"

    pagination_row = []
    if offset > 0:
        prev_offset = max(0, offset - MAX_BTN)
        pagination_row.append(InlineKeyboardButton("« ʙᴀᴄᴋ", callback_data=f"next_{req}_{key}_{prev_offset}"))

    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons")) # Non-clickable

    if n_offset != "": # Check if there IS a next page offset
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"next_{req}_{key}_{n_offset}"))

    if pagination_row: btn.append(pagination_row)

    # Edit the message
    try:
        await query.message.edit_text(cap + files_link + del_msg, reply_markup=InlineKeyboardMarkup(btn), disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except FloodWait as e: logger.warning(f"FloodWait next_page: sleep {e.value}"); await asyncio.sleep(e.value); await query.answer("Retrying...", show_alert=False)
    except Exception as e: logger.error(f"Error edit next_page: {e}"); await query.answer("Error.", show_alert=True)

# Keep languages_ callback
@Client.on_callback_query(filters.regex(r"^languages"))
async def languages_(client: Client, query: CallbackQuery):
    _, key, req, offset_str = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str) # Keep original offset
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    search = BUTTONS.get(key)
    if not search: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    btn = []; lang_iter = iter(LANGUAGES)
    for lang1 in lang_iter:
         try:
              lang2 = next(lang_iter)
              btn.append([ InlineKeyboardButton(lang1.title(), callback_data=f"lang_search#{lang1}#{key}#{original_offset}#{req}"),
                           InlineKeyboardButton(lang2.title(), callback_data=f"lang_search#{lang2}#{key}#{original_offset}#{req}") ])
         except StopIteration: btn.append([InlineKeyboardButton(lang1.title(), callback_data=f"lang_search#{lang1}#{key}#{original_offset}#{req}")]); break

    btn.append([InlineKeyboardButton("⪻ ʙᴀᴄᴋ", callback_data=f"next_{req}_{key}_{original_offset}")])
    try: await query.message.edit_text("<b>👇 sᴇʟᴇᴄᴛ ʟᴀɴɢᴜᴀɢᴇ:</b>", reply_markup=InlineKeyboardMarkup(btn))
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"languages_ error: {e}"); await query.answer("Error.", show_alert=True)

# Keep quality callback
@Client.on_callback_query(filters.regex(r"^quality"))
async def quality(client: Client, query: CallbackQuery):
    _, key, req, offset_str = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    search = BUTTONS.get(key)
    if not search: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    btn = []; qual_iter = iter(QUALITY)
    for q1 in qual_iter:
         try:
              q2 = next(qual_iter)
              btn.append([ InlineKeyboardButton(q1.upper(), callback_data=f"qual_search#{q1}#{key}#{original_offset}#{req}"),
                           InlineKeyboardButton(q2.upper(), callback_data=f"qual_search#{q2}#{key}#{original_offset}#{req}") ])
         except StopIteration: btn.append([InlineKeyboardButton(q1.upper(), callback_data=f"qual_search#{q1}#{key}#{original_offset}#{req}")]); break

    btn.append([InlineKeyboardButton("⪻ ʙᴀᴄᴋ", callback_data=f"next_{req}_{key}_{original_offset}")])
    try: await query.message.edit_text("<b>👇 sᴇʟᴇᴄᴛ ǫᴜᴀʟɪᴛʏ:</b>", reply_markup=InlineKeyboardMarkup(btn))
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"quality error: {e}"); await query.answer("Error.", show_alert=True)

# Keep lang_search (already modified)
@Client.on_callback_query(filters.regex(r"^lang_search"))
async def filter_languages_cb_handler(client: Client, query: CallbackQuery):
    _, lang, key, offset_str, req = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    current_offset = 0 # Start filtered list from page 0
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=lang, offset=current_offset)
    if not files: return await query.answer(f"ɴᴏ '{lang.title()}' ғɪʟᴇs ғᴏᴜɴᴅ.", show_alert=True)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=1): files_link += f"<b>\n\n{file_num}. <a href=https://t.me/{temp.U_NAME}?start=file_{query.message.chat.id}_{file['_id']}>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name'][:60]}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#{original_offset}"), # Back to lang selection
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0")
    ])

    current_page = 1; total_pages = math.ceil(total_filtered / MAX_BTN); page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []
    if next_filtered_offset != "":
        pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"lang_next#{req}#{key}#{lang}#{next_filtered_offset}#{original_offset}"))
    elif total_filtered > 0: pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if pagination_row: btn.append(pagination_row)

    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit lang_search: {e}"); await query.answer("Error.", show_alert=True)

# Keep qual_search (already modified)
@Client.on_callback_query(filters.regex(r"^qual_search"))
async def quality_search(client: Client, query: CallbackQuery):
    _, qual, key, offset_str, req = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    current_offset = 0
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=qual, offset=current_offset) # Use lang param for quality
    if not files: return await query.answer(f"ɴᴏ '{qual.upper()}' ғɪʟᴇs ғᴏᴜɴᴅ.", show_alert=True)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=1): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name'][:60]}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#{original_offset}")
    ])

    current_page = 1; total_pages = math.ceil(total_filtered / MAX_BTN); page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []
    if next_filtered_offset != "":
        pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"qual_next#{req}#{key}#{qual}#{next_filtered_offset}#{original_offset}"))
    elif total_filtered > 0: pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if pagination_row: btn.append(pagination_row)

    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit qual_search: {e}"); await query.answer("Error.", show_alert=True)

# Keep lang_next (already modified)
@Client.on_callback_query(filters.regex(r"^lang_next"))
async def lang_next_page(bot, query: CallbackQuery):
    _, req, key, lang, current_filtered_offset_str, original_offset_str = query.data.split("#")
    try: req_user_id = int(req); current_filtered_offset = int(current_filtered_offset_str); original_offset = int(original_offset_str)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=lang, offset=current_filtered_offset)
    if not files: return await query.answer("No more files.", show_alert=False)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []; start_num = current_filtered_offset + 1

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=start_num): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name'][:60]}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#{original_offset}"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0")
    ])

    current_page = math.ceil(current_filtered_offset / MAX_BTN) + 1; total_pages = math.ceil(total_filtered / MAX_BTN); page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []

    prev_filtered_offset = max(0, current_filtered_offset - MAX_BTN)
    pagination_row.append(InlineKeyboardButton("« ʙᴀᴄᴋ", callback_data=f"lang_next#{req}#{key}#{lang}#{prev_filtered_offset}#{original_offset}"))
    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if next_filtered_offset != "": pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"lang_next#{req}#{key}#{lang}#{next_filtered_offset}#{original_offset}"))

    btn.append(pagination_row)
    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit lang_next: {e}"); await query.answer("Error.", show_alert=True)

# Keep qual_next (already modified)
@Client.on_callback_query(filters.regex(r"^qual_next"))
async def quality_next_page(bot, query: CallbackQuery):
    _, req, key, qual, current_filtered_offset_str, original_offset_str = query.data.split("#")
    try: req_user_id = int(req); current_filtered_offset = int(current_filtered_offset_str); original_offset = int(original_offset_str)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=qual, offset=current_filtered_offset) # Use lang for quality
    if not files: return await query.answer("No more files.", show_alert=False)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []; start_num = current_filtered_offset + 1

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=start_num): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name'][:60]}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#{original_offset}")
    ])

    current_page = math.ceil(current_filtered_offset / MAX_BTN) + 1; total_pages = math.ceil(total_filtered / MAX_BTN); page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []

    prev_filtered_offset = max(0, current_filtered_offset - MAX_BTN)
    pagination_row.append(InlineKeyboardButton("« ʙᴀᴄᴋ", callback_data=f"qual_next#{req}#{key}#{qual}#{prev_filtered_offset}#{original_offset}"))
    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if next_filtered_offset != "": pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"qual_next#{req}#{key}#{qual}#{next_filtered_offset}#{original_offset}"))

    btn.append(pagination_row)
    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit qual_next: {e}"); await query.answer("Error.", show_alert=True)


# Keep spolling callback (no premium logic)
@Client.on_callback_query(filters.regex(r"^spolling"))
async def advantage_spoll_choker(bot, query: CallbackQuery):
    _, id, user = query.data.split('#')
    try: req_user_id = int(user)
    except ValueError: return await query.answer("Invalid data.", show_alert=True)

    if req_user_id != 0 and query.from_user.id != req_user_id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    await query.answer("Searching suggestion...")
    try:
        movie = await get_poster(id, id=True) # Use refined get_poster
        if not movie: raise ValueError("IMDb lookup failed")
        search = movie.get('title', "N/A"); year = movie.get('year')
        if search == "N/A": raise ValueError("Could not get title")
        # Clean title slightly for search
        search_cleaned = re.sub(r"[:()]", "", search).strip()
        search_query = f"{search_cleaned} {year}" if year else search_cleaned # Add year if available
    except Exception as e:
        logger.error(f"Error getting poster spell check ID {id}: {e}")
        await query.message.edit(f"❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ғᴇᴛᴄʜ ᴅᴇᴛᴀɪʟs."); return

    s = await query.message.edit(f"<b><i>✅ ᴏᴋᴀʏ, sᴇᴀʀᴄʜɪɴɢ ғᴏʀ: `{search_query}`</i></b>")
    files, offset, total_results = await get_search_results(search_query, offset=0) # Use async
    if files:
        k = (search_query, files, offset, total_results) # Package results
        await auto_filter(bot, query, s, spoll=k) # Pass query, status msg, results
    else:
        k = await s.edit(script.NOT_FILE_TXT.format(query.from_user.mention, search_query))
        try: await bot.send_message(LOG_CHANNEL, f"#ɴᴏ_ʀᴇsᴜʟᴛ_ᴀғᴛᴇʀ_sᴘᴇʟʟ\n\n♢ ʀᴇǫ: {query.from_user.mention}\n♢ ᴏʀɪɢ: `{BUTTONS.get(query.message.reply_markup.inline_keyboard[0][0].callback_data.split('#')[1], 'Unknown')}`\n♢ sᴜɢɢ: `{search_query}`")
        except: pass
        await asyncio.sleep(60)
        try: await k.delete()
        except: pass


# Keep main cb_handler (already modified to remove premium)
@Client.on_callback_query()
async def cb_handler(client: Client, query: CallbackQuery):
    data = query.data # Cache data

    # Close button
    if data == "close_data":
        try: user = query.message.reply_to_message.from_user.id
        except: user = query.from_user.id
        if int(user) != 0 and query.from_user.id != int(user): return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)
        await query.answer("Closing."); await query.message.delete()
        try: await query.message.reply_to_message.delete() # Delete original trigger message too
        except: pass
        return # Important to return after handling

    # File button click
    elif data.startswith("file"):
        ident, file_id = data.split("#")
        try: user = query.message.reply_to_message.from_user.id
        except: user = query.from_user.id # Fallback if no reply_to_message
        if int(user) != 0 and query.from_user.id != int(user): return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)
        # Just provide the start link in the answer
        await query.answer(url=f"https://t.me/{temp.U_NAME}?start=file_{query.message.chat.id}_{file_id}")
        return

    # Get deleted file again
    elif data.startswith("get_del_file"):
        ident, group_id, file_id = data.split("#")
        # No premium check needed
        await query.answer(url=f"https://t.me/{temp.U_NAME}?start=file_{group_id}_{file_id}")
        try: await query.message.delete()
        except: pass
        return

    # Get deleted file batch again
    elif data.startswith("get_del_send_all_files"):
        ident, group_id, key = data.split("#")
        # No premium check needed
        await query.answer(url=f"https://t.me/{temp.U_NAME}?start=all_{group_id}_{key}")
        try: await query.message.delete()
        except: pass
        return

    # Stream button click
    elif data.startswith("stream"):
        file_id = data.split('#', 1)[1]
        # No premium check needed
        try:
            msg = await client.send_cached_media(chat_id=BIN_CHANNEL, file_id=file_id)
            watch = f"{URL}watch/{msg.id}"
            download = f"{URL}download/{msg.id}"
            btn=[[ InlineKeyboardButton("🖥️ ᴡᴀᴛᴄʜ", url=watch), InlineKeyboardButton("📥 ᴅᴏᴡɴʟᴏᴀᴅ", url=download)],
                 [ InlineKeyboardButton('❌ ᴄʟᴏsᴇ', callback_data='close_data')]]
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(btn))
            await query.answer("Links generated!")
        except Exception as e: logger.error(f"Stream callback error: {e}"); await query.answer("Error generating links.", show_alert=True)
        return

    # FSub check retry
    elif data.startswith("checksub"):
        ident, mc = data.split("#")
        btn = await is_subscribed(client, query) # Pass query object
        if btn:
            await query.answer("❗ᴘʟᴇᴀsᴇ ᴊᴏɪɴ ᴀʟʟ ᴄʜᴀɴɴᴇʟs ғɪʀsᴛ.", show_alert=True)
            btn.append([InlineKeyboardButton("🔁 ᴛʀʏ ᴀɢᴀɪɴ", callback_data=f"checksub#{mc}")])
            try: await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(btn))
            except MessageNotModified: pass
        else: # User is subscribed
            await query.answer("✅ sᴜʙsᴄʀɪʙᴇᴅ! ʀᴇᴅɪʀᴇᴄᴛɪɴɢ...", show_alert=False)
            await query.answer(url=f"https://t.me/{temp.U_NAME}?start={mc}") # Redirect via answer URL
            try: await query.message.delete() # Delete the fsub message
            except: pass
        return

    # Non-clickable page button
    elif data == "buttons": await query.answer()

    # Instructions alert
    elif data == "instructions":
        await query.answer(
            "♢ ᴍᴏᴠɪᴇ: `Movie Name` ᴏʀ `Movie Name Year`\n"
            "♢ sᴇʀɪᴇs: `Series Name S01E01`\n\n"
            "ᴅᴏɴ'ᴛ ᴜsᴇ sʏᴍʙᴏʟs.", show_alert=True)

    # Removed activate_trial, activate_plan callbacks

    # --- Start Menu Callbacks ---
    elif data == "start":
        buttons = [[ InlineKeyboardButton("➕ ᴀᴅᴅ ᴍᴇ", url=f'http://t.me/{temp.U_NAME}?startgroup=start') ],
                   [ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK), InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ],
                   [ InlineKeyboardButton('❔ ʜᴇʟᴘ', callback_data='help'),
                     InlineKeyboardButton('🔍 ɪɴʟɪɴᴇ', switch_inline_query_current_chat=''),
                     InlineKeyboardButton('ℹ️ ᴀʙᴏᴜᴛ', callback_data='about') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.START_TXT.format(query.from_user.mention, get_wish())), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"Start CB Error: {e}")
        return

    elif data == "about":
        buttons = [[ InlineKeyboardButton('📊 sᴛᴀᴛᴜs', callback_data='stats'), InlineKeyboardButton('👨‍💻 sᴏᴜʀᴄᴇ', callback_data='source') ],
                   [ InlineKeyboardButton('🧑‍💻 ᴏᴡɴᴇʀ', callback_data='owner') ],
                   [ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='start') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.MY_ABOUT_TXT), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"About CB Error: {e}")
        return

    elif data == "stats":
        if query.from_user.id not in ADMINS: return await query.answer("ᴀᴅᴍɪɴs ᴏɴʟʏ!", show_alert=True)
        # Use synchronous db calls here for simplicity in callback (can wrap in executor if slow)
        try:
             files = db_count_documents(); users = db.total_users_count(); chats = db.total_chat_count()
             # prm = db.get_premium_count() # Removed
             used_files_db = get_size(db.get_files_db_size()); used_data_db = get_size(db.get_data_db_size())
             secnd_files = second_db_count_documents() if second_collection else '-'
             secnd_files_db_used = get_size(db.get_second_files_db_size()) if second_collection else '-'
             uptime = get_readable_time(time_now() - temp.START_TIME)
             stats_text = script.STATUS_TXT.format(users, chats, used_data_db, (files + (secnd_files if isinstance(secnd_files, int) else 0)), files, used_files_db, secnd_files, secnd_files_db_used, uptime) # Pass total files too
             buttons = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='about') ]]
             await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=stats_text), reply_markup=InlineKeyboardMarkup(buttons))
        except Exception as e: logger.error(f"Stats CB Error: {e}"); await query.answer("Error fetching stats.", show_alert=True)
        return

    elif data == "owner":
        buttons = [[InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='about')]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.MY_OWNER_TXT), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"Owner CB Error: {e}")
        return

    elif data == "help":
        buttons = [[ InlineKeyboardButton('📚 ᴜsᴇʀ ᴄᴍᴅs', callback_data='user_command'), InlineKeyboardButton('⚙️ ᴀᴅᴍɪɴ ᴄᴍᴅs', callback_data='admin_command') ],
                   [ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='start') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.HELP_TXT.format(query.from_user.mention)), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"Help CB Error: {e}")
        return

    elif data == "user_command":
        buttons = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='help') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.USER_COMMAND_TXT), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"User CMD CB Error: {e}")
        return

    elif data == "admin_command":
        if query.from_user.id not in ADMINS: return await query.answer("ᴀᴅᴍɪɴs ᴏɴʟʏ!", show_alert=True)
        buttons = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='help') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.ADMIN_COMMAND_TXT), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"Admin CMD CB Error: {e}")
        return

    elif data == "source":
        buttons = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data='about') ]]
        try: await query.edit_message_media(InputMediaPhoto(random.choice(PICS), caption=script.SOURCE_TXT), reply_markup=InlineKeyboardMarkup(buttons))
        except MessageNotModified: await query.answer()
        except Exception as e: logger.error(f"Source CB Error: {e}")
        return

    # --- Settings Callbacks ---
    elif data.startswith("bool_setgs"):
        ident, set_type, status, grp_id_str = data.split("#")
        try: grp_id = int(grp_id_str)
        except ValueError: return await query.answer("Invalid group ID.", show_alert=True)
        userid = query.from_user.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)
        new_status = not (status == "True") # Toggle boolean
        await save_group_settings(grp_id, set_type, new_status) # await save
        btn = await get_grp_stg(grp_id) # await get
        try: await query.message.edit_reply_markup(InlineKeyboardMarkup(btn)) # Edit markup
        except MessageNotModified: pass
        await query.answer(f"{set_type.replace('_',' ').title()} set to {new_status}")
        return

    elif data.startswith(("imdb_setgs", "welcome_setgs", "tutorial_setgs", "shortlink_setgs", "caption_setgs")):
        setting_type = data.split("_")[0] # imdb, welcome, etc.
        _, grp_id_str = data.split("#")
        try: grp_id = int(grp_id_str)
        except ValueError: return await query.answer("Invalid group ID.", show_alert=True)
        userid = query.from_user.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)

        settings = await get_settings(grp_id) # await get
        current_val_key = {'imdb': 'template', 'welcome': 'welcome_text', 'tutorial': 'tutorial', 'shortlink': 'url', 'caption': 'caption'}.get(setting_type)
        current_val = settings.get(current_val_key, "N/A")
        if setting_type == 'shortlink': current_val = f"{settings.get('url', 'N/A')} - {settings.get('api', 'N/A')}"

        btn = [[ InlineKeyboardButton(f'sᴇᴛ ɴᴇᴡ {setting_type.title()}', callback_data=f'set_{setting_type}#{grp_id}') ],
               [ InlineKeyboardButton(f'ᴜsᴇ ᴅᴇғᴀᴜʟᴛ {setting_type.title()}', callback_data=f'default_{setting_type}#{grp_id}') ],
               [ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data=f'back_setgs#{grp_id}') ]]

        await query.message.edit(f'⚙️ {setting_type.replace("_"," ").title()} sᴇᴛᴛɪɴɢs:\n\nᴄᴜʀʀᴇɴᴛ:\n`{current_val}`', reply_markup=InlineKeyboardMarkup(btn), disable_web_page_preview=True)
        return

    elif data.startswith(("set_imdb", "set_welcome", "set_tutorial", "set_shortlink", "set_caption")):
        setting_type = data.split("_")[1].split("#")[0] # imdb, welcome, etc.
        _, grp_id_str = data.split("#")
        try: grp_id = int(grp_id_str)
        except ValueError: return await query.answer("Invalid group ID.", show_alert=True)
        userid = query.from_user.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)

        value_key = {'imdb': 'template', 'welcome': 'welcome_text', 'tutorial': 'tutorial', 'shortlink': 'url', 'caption': 'caption'}.get(setting_type)
        api_key = 'api' if setting_type == 'shortlink' else None # Special case for shortlink

        prompt_text = f"➡️ sᴇɴᴅ ᴛʜᴇ ɴᴇᴡ {setting_type.replace('_',' ').title()}"
        if setting_type == 'shortlink': prompt_text += " URL."
        elif setting_type in ['imdb', 'welcome', 'caption']: prompt_text += " (ᴜsᴇ ғᴏʀᴍᴀᴛᴛɪɴɢ ᴋᴇʏs ʟɪᴋᴇ `{key}`)."

        try:
            ask_msg = await query.message.edit(prompt_text)
            response1 = await client.listen(chat_id=query.message.chat.id, user_id=userid, timeout=300) # 5 min timeout
            if not response1 or not response1.text: raise asyncio.TimeoutError
            new_value1 = response1.text.strip()
            await response1.delete() # Delete user's response
        except asyncio.TimeoutError: return await ask_msg.edit("⏰ ᴛɪᴍᴇᴏᴜᴛ.")
        except Exception as e: logger.error(f"Listen error set_{setting_type}: {e}"); return await ask_msg.edit("An error occurred.")

        new_value2 = None
        if api_key: # Ask for API key if it's shortlink
             try:
                  await ask_msg.edit("➡️ ɴᴏᴡ sᴇɴᴅ ᴛʜᴇ API ᴋᴇʏ.")
                  response2 = await client.listen(chat_id=query.message.chat.id, user_id=userid, timeout=300)
                  if not response2 or not response2.text: raise asyncio.TimeoutError
                  new_value2 = response2.text.strip()
                  await response2.delete()
             except asyncio.TimeoutError: return await ask_msg.edit("⏰ ᴛɪᴍᴇᴏᴜᴛ.")
             except Exception as e: logger.error(f"Listen API error set_{setting_type}: {e}"); return await ask_msg.edit("An error occurred.")

        # Save the setting(s)
        await save_group_settings(grp_id, value_key, new_value1) # await save
        if api_key and new_value2: await save_group_settings(grp_id, api_key, new_value2) # await save

        back_button = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data=f'{setting_type}_setgs#{grp_id}') ]]
        success_text = f"✅ sᴜᴄᴄᴇssғᴜʟʟʏ ᴜᴘᴅᴀᴛᴇᴅ {setting_type.replace('_',' ').title()}!"
        if setting_type == 'shortlink': success_text += f"\n\nURL: `{new_value1}`\nAPI: `{new_value2}`"
        else: success_text += f"\n\nɴᴇᴡ ᴠᴀʟᴜᴇ:\n`{new_value1}`"

        await ask_msg.edit(success_text, reply_markup=InlineKeyboardMarkup(back_button))
        return

    elif data.startswith(("default_imdb", "default_welcome", "default_tutorial", "default_shortlink", "default_caption")):
        setting_type = data.split("_")[1].split("#")[0]
        _, grp_id_str = data.split("#")
        try: grp_id = int(grp_id_str)
        except ValueError: return await query.answer("Invalid group ID.", show_alert=True)
        userid = query.from_user.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)

        default_map = {'imdb': ('template', script.IMDB_TEMPLATE), 'welcome': ('welcome_text', script.WELCOME_TEXT),
                       'tutorial': ('tutorial', TUTORIAL), 'shortlink': [('url', SHORTLINK_URL), ('api', SHORTLINK_API)],
                       'caption': ('caption', script.FILE_CAPTION)}

        setting_info = default_map.get(setting_type)
        if not setting_info: return await query.answer("Invalid setting type.", show_alert=True)

        if isinstance(setting_info, list): # Handle shortlink case
             for key, default_value in setting_info: await save_group_settings(grp_id, key, default_value) # await save
        else: # Handle single value case
             key, default_value = setting_info
             await save_group_settings(grp_id, key, default_value) # await save

        back_button = [[ InlineKeyboardButton('« ʙᴀᴄᴋ', callback_data=f'{setting_type}_setgs#{grp_id}') ]]
        await query.message.edit(f"✅ sᴜᴄᴄᴇssғᴜʟʟʏ ʀᴇsᴇᴛ {setting_type.replace('_',' ').title()} ᴛᴏ ᴅᴇғᴀᴜʟᴛ.", reply_markup=InlineKeyboardMarkup(back_button))
        return

    elif data.startswith("back_setgs"):
        _, grp_id_str = data.split("#")
        try: grp_id = int(grp_id_str)
        except ValueError: return await query.answer("Invalid group ID.", show_alert=True)
        userid = query.from_user.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)

        btn = await get_grp_stg(grp_id) # await get
        chat = await client.get_chat(grp_id)
        await query.message.edit(text=f"⚙️ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        return

    elif data == "open_group_settings":
        userid = query.from_user.id
        grp_id = query.message.chat.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)
        btn = await get_grp_stg(grp_id)
        await query.message.edit(text=f"⚙️ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{query.message.chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
        return

    elif data == "open_pm_settings":
        userid = query.from_user.id
        grp_id = query.message.chat.id
        if not await is_check_admin(client, grp_id, userid): return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)
        btn = await get_grp_stg(grp_id)
        pm_button = [[ InlineKeyboardButton('ɢᴏ ᴛᴏ ᴘᴍ ➔', url=f"https://t.me/{temp.U_NAME}?start=settings_{grp_id}") ]]
        try:
            # Try sending the message first
            await client.send_message(userid, f"⚙️ ᴄʜᴀɴɢᴇ sᴇᴛᴛɪɴɢs ғᴏʀ <b>'{query.message.chat.title}'</b>:", reply_markup=InlineKeyboardMarkup(btn))
            await query.message.edit("✅ sᴇᴛᴛɪɴɢs ᴍᴇɴᴜ sᴇɴᴛ ᴛᴏ ʏᴏᴜʀ ᴘᴍ.", reply_markup=InlineKeyboardMarkup(pm_button))
        except Exception as e: # If sending fails (e.g., user blocked bot)
            logger.warning(f"Failed to send settings to PM for user {userid}: {e}")
            await query.answer(url=f"https://t.me/{temp.U_NAME}?start=settings_{grp_id}") # Use answer URL as fallback
            await query.message.edit("⚠️ ᴄᴏᴜʟᴅ ɴᴏᴛ sᴇɴᴅ ᴛᴏ ᴘᴍ. ᴄʟɪᴄᴋ ᴛʜᴇ ʙᴜᴛᴛᴏɴ ᴛᴏ ᴏᴘᴇɴ.", reply_markup=InlineKeyboardMarkup(pm_button))
        return

    elif data.startswith("delete"):
        if query.from_user.id not in ADMINS: return await query.answer("ᴀᴅᴍɪɴs ᴏɴʟʏ.", show_alert=True)
        _, query_text = data.split("_", 1)
        await query.message.edit('⏳ ᴅᴇʟᴇᴛɪɴɢ...')
        deleted_count = await delete_files(query_text) # await async delete
        await query.message.edit(f'✅ ᴅᴇʟᴇᴛᴇᴅ {deleted_count} ғɪʟᴇs ғᴏʀ ǫᴜᴇʀʏ `{query_text}`.')
        return

    elif data.startswith("send_all"):
        ident, key, req = data.split("#")
        try: req_user_id = int(req)
        except ValueError: return await query.answer("Invalid data.", show_alert=True)
        if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)
        files = temp.FILES.get(key)
        if not files: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)
        # Just provide the start link
        await query.answer(url=f"https://t.me/{temp.U_NAME}?start=all_{query.message.chat.id}_{key}")
        return

    # --- Moderation Callbacks ---
    elif data in ["unmute_all_members", "unban_all_members", "kick_muted_members", "kick_deleted_accounts_members"]:
        if not await is_check_admin(client, query.message.chat.id, query.from_user.id):
             return await query.answer("ɴᴏᴛ ᴀᴅᴍɪɴ.", show_alert=True)

        action = data.split("_")[0] # unmute, unban, kick
        target = data.split("_")[1] # muted, deleted, all (for unban/unmute)
        filter_type = None
        success_count = 0
        error_count = 0
        start_time = time_now()

        await query.message.edit(f"⏳ sᴛᴀʀᴛɪɴɢ `{action} {target}`...")

        try:
            if target == "muted": filter_type = enums.ChatMembersFilter.RESTRICTED
            elif target == "all" and action in ["unmute", "unban"]: filter_type = enums.ChatMembersFilter.RESTRICTED if action == "unmute" else enums.ChatMembersFilter.BANNED
            elif target == "deleted": filter_type = enums.ChatMembersFilter.SEARCH # Need search to check is_deleted

            async for member in client.get_chat_members(query.message.chat.id, filter=filter_type):
                 user = member.user
                 if not user: continue
                 # Skip bots and self
                 if user.is_bot or user.id == temp.ME: continue
                 # Skip admins unless explicitly targeted (which these actions don't)
                 if member.status in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]: continue

                 action_taken = False
                 try:
                      if action == "unmute" and target == "all" and member.status == enums.ChatMemberStatus.RESTRICTED:
                           await client.unban_chat_member(query.message.chat.id, user.id) # Unmuting might require unban_chat_member
                           action_taken = True
                      elif action == "unban" and target == "all" and member.status == enums.ChatMemberStatus.BANNED:
                           await client.unban_chat_member(query.message.chat.id, user.id)
                           action_taken = True
                      elif action == "kick" and target == "muted" and member.status == enums.ChatMemberStatus.RESTRICTED:
                           await client.ban_chat_member(query.message.chat.id, user.id, until_date=datetime.now(timezone.utc) + timedelta(seconds=35)) # Temp ban = kick
                           action_taken = True
                      elif action == "kick" and target == "deleted" and user.is_deleted:
                           await client.ban_chat_member(query.message.chat.id, user.id, until_date=datetime.now(timezone.utc) + timedelta(seconds=35))
                           action_taken = True

                      if action_taken: success_count += 1; await asyncio.sleep(0.1) # Small delay

                 except FloodWait as e: logger.warning(f"FloodWait during {action}: sleep {e.value}"); await asyncio.sleep(e.value); error_count += 1 # Count as error and continue
                 except Exception as e: logger.error(f"Error {action} user {user.id}: {e}"); error_count += 1

        except Exception as outer_e:
            logger.error(f"Error during {action} {target} loop: {outer_e}", exc_info=True)
            await query.message.edit(f"❌ ᴇʀʀᴏʀ ᴅᴜʀɪɴɢ ᴏᴘᴇʀᴀᴛɪᴏɴ:\n`{outer_e}`")
            return

        elapsed = get_readable_time(time_now() - start_time)
        final_msg = f"✅ ᴏᴘᴇʀᴀᴛɪᴏɴ `{action} {target}` ᴄᴏᴍᴘʟᴇᴛᴇᴅ ɪɴ {elapsed}.\n\n♢ sᴜᴄᴄᴇss: <code>{success_count}</code>\n♢ ᴇʀʀᴏʀs: <code>{error_count}</code>"
        if success_count == 0 and error_count == 0: final_msg = f"🤷‍♂️ ɴᴏ ᴜsᴇʀs ғᴏᴜɴᴅ ᴛᴏ `{action}`."

        # Edit original message first
        try: await query.message.edit(final_msg)
        except: # Fallback reply if edit fails (e.g., message too old)
             await query.message.reply(final_msg)
             try: await query.message.delete() # Try deleting original prompt
             except: pass
        return

    # Fallback for unhandled callbacks
    # else:
    #     await query.answer("Invalid callback data or action.", show_alert=True)


# Keep auto_filter function (already modified)
async def auto_filter(client, msg, s, spoll=False):
    if not spoll:
        message = msg; settings = await get_settings(message.chat.id)
        search = re.sub(r"\s+", " ", re.sub(r"[-:\"';!]", " ", message.text)).strip()
        if not search: return await s.edit("Please provide text to search.") # Handle empty search
        # Use async search
        files, offset, total_results = await get_search_results(search, offset=0)
        if not files:
            if settings.get("spell_check", True): await advantage_spell_chok(message, s) # await spell check
            else: await s.edit(script.NOT_FILE_TXT.format(message.from_user.mention, search)) # Use formatted text
            return
    else: # Called from spell check callback
        settings = await get_settings(msg.message.chat.id) # msg is CallbackQuery
        message = msg.message.reply_to_message # Original user message
        search, files, offset, total_results = spoll # Unpack results

    req = message.from_user.id if message and message.from_user else 0
    key = f"{message.chat.id}-{message.id}" # Unique key per trigger message
    temp.FILES[key] = files; BUTTONS[key] = search # Cache files and search term

    files_link = ""; btn = []
    if settings.get('links', False):
        for file_num, file in enumerate(files, start=1): files_link += f"<b>\n\n{file_num}. <a href=https://t.me/{temp.U_NAME}?start=file_{message.chat.id}_{file['_id']}>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name'][:60]}", callback_data=f'file#{file["_id"]}')])

    # Standard buttons
    btn.insert(0, [InlineKeyboardButton("✨ ᴜᴘᴅᴀᴛᴇs", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0")
    ])

    # Pagination buttons if needed
    if offset != "": # offset is the next page offset from get_search_results
        current_page = 1; total_pages = math.ceil(total_results / MAX_BTN)
        page_label = f"ᴘɢ {current_page}/{total_pages}"
        pagination_row = [ InlineKeyboardButton(page_label, callback_data="buttons"),
                           InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"next_{req}_{key}_{offset}") ]
        btn.append(pagination_row)

    # IMDb and Caption
    imdb = await get_poster(search, file=(files[0])['file_name']) if settings.get("imdb", True) else None
    TEMPLATE = settings.get('template', script.IMDB_TEMPLATE) # Use group or default template
    if imdb:
        # Use updated template format keys from Script.py and get_poster
        try:
             cap = TEMPLATE.format( query=search, **imdb, message=message ) # Pass message for mention
        except Exception as format_e:
             logger.error(f"IMDb template format error: {format_e}"); cap = f"🎬 {imdb.get('title', search)}" # Fallback caption
    else:
        cap = f"<b>👋 ʜᴇʏ {message.from_user.mention},\n\n🔎 ʀᴇsᴜʟᴛs ғᴏʀ: {search}</b>"
    CAP[key] = cap # Cache the final caption

    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɪɴ {get_readable_time(DELETE_TIME)}.</b>" if settings.get("auto_delete", False) else ''
    final_caption = cap[:1024] + files_link + del_msg # Combine and limit caption

    # Send response with appropriate media
    reply_markup = InlineKeyboardMarkup(btn)
    try:
        if imdb and imdb.get('poster'):
             # Try sending poster
             try:
                  await s.delete() # Delete "searching..." message
                  k = await message.reply_photo(photo=imdb['poster'], caption=final_caption, reply_markup=reply_markup, parse_mode=enums.ParseMode.HTML, quote=True)
             except (MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty):
                  logger.warning(f"IMDb poster failed for {search}, using text.")
                  # Fallback to text if poster fails
                  k = await message.reply_text(final_caption, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True)
             except FloodWait as e: logger.warning(f"FloodWait sending photo: sleep {e.value}"); await asyncio.sleep(e.value); k = await message.reply_text(final_caption, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True) # Fallback retry as text
             except Exception as e_photo: logger.error(f"Error sending photo reply: {e_photo}"); k = await message.reply_text(final_caption, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True) # Fallback to text
        else:
             # Send as text if no poster
             await s.delete() # Delete "searching..." message
             k = await message.reply_text(final_caption, reply_markup=reply_markup, disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML, quote=True)

        # Auto-delete logic
        if settings.get("auto_delete", False):
            await asyncio.sleep(DELETE_TIME)
            try: await k.delete()
            except: pass
            try: await message.delete() # Delete user's trigger message
            except: pass
    except FloodWait as e_main:
         logger.warning(f"FloodWait editing/sending main reply: sleep {e_main.value}"); await asyncio.sleep(e_main.value)
         # Optionally retry sending as text here if needed
    except Exception as e_main:
         logger.error(f"Error sending final filter message: {e_main}", exc_info=True)
         try: await s.edit("❌ ᴀɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ɢᴇɴᴇʀᴀᴛɪɴɢ ʀᴇsᴜʟᴛs.") # Edit status message on error
         except: pass # Ignore if editing status fails


# Keep advantage_spell_chok (already modified)
async def advantage_spell_chok(message, s):
    search = message.text
    google_search_url = f"https://www.google.com/search?q={re.sub(r' ', '+', search)}"
    btn = [[ InlineKeyboardButton("❓ ɪɴsᴛʀᴜᴄᴛɪᴏɴs", callback_data='instructions'),
             InlineKeyboardButton("🌍 sᴇᴀʀᴄʜ ɢᴏᴏɢʟᴇ", url=google_search_url) ]]
    try: movies = await get_poster(search, bulk=True) # Use async get_poster
    except Exception as e: logger.error(f"Spell check poster error: {e}"); movies = None

    if not movies:
        n = await s.edit(text=script.NOT_FILE_TXT.format(message.from_user.mention, search), reply_markup=InlineKeyboardMarkup(btn))
        try: await message._client.send_message(LOG_CHANNEL, f"#ɴᴏ_ʀᴇsᴜʟᴛ\n\n♢ ʀᴇǫ: {message.from_user.mention}\n♢ ǫᴜᴇʀʏ: `{search}`")
        except Exception as log_e: logger.error(f"Log NO_RESULT error: {log_e}")
        # Auto-delete handled by caller (auto_filter)
        return

    # Filter unique movies and limit suggestions
    seen_ids = set(); unique_movies = []
    for movie in movies:
        if movie.movieID not in seen_ids: unique_movies.append(movie); seen_ids.add(movie.movieID)
        if len(unique_movies) >= 7: break

    user = message.from_user.id if message.from_user else 0
    buttons = []
    for movie in unique_movies:
        title = movie.get('title', 'Unknown')[:50] # Limit title length for button
        year = f"({movie.get('year')})" if movie.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"{title} {year}".strip(), callback_data=f"spolling#{movie.movieID}#{user}")])

    buttons.append([InlineKeyboardButton("🚫 ᴄʟᴏsᴇ", callback_data="close_data")])

    await s.edit(text=f"👋 ʜᴇʟʟᴏ {message.from_user.mention},\n\nɪ ᴄᴏᴜʟᴅɴ'ᴛ ғɪɴᴅ `<b>{search}</b>`.\nᴅɪᴅ ʏᴏᴜ ᴍᴇᴀɴ ᴏɴᴇ ᴏғ ᴛʜᴇsᴇ? 👇",
               reply_markup=InlineKeyboardMarkup(buttons))
    # Auto-delete handled by caller (auto_filter)
