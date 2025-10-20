import asyncio
import re
from time import time as time_now
import math, os
import random # Keep random if used for PICS
from hydrogram.errors import ListenerTimeout, MessageNotModified, FloodWait
from hydrogram.errors.exceptions.bad_request_400 import MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty
from Script import script
from datetime import datetime, timedelta
# Removed premium related imports
from info import (PICS, TUTORIAL, ADMINS, URL, MAX_BTN, BIN_CHANNEL,
                  DELETE_TIME, FILMS_LINK, LOG_CHANNEL, SUPPORT_GROUP, SUPPORT_LINK,
                  UPDATES_LINK, LANGUAGES, QUALITY, IS_STREAM) # Removed IS_PREMIUM, others
from hydrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaPhoto
from hydrogram import Client, filters, enums
# Removed is_premium import from utils
from utils import (get_size, is_subscribed, is_check_admin, get_wish,
                   get_shortlink, get_readable_time, get_poster, temp,
                   get_settings, save_group_settings) # Removed is_premium
from database.users_chats_db import db
from database.ia_filterdb import get_search_results,delete_files # Removed counts if only used in stats
from plugins.commands import get_grp_stg
import logging # Added logging

logger = logging.getLogger(__name__)

BUTTONS = {}
CAP = {}

# Keep pm_search function (already modified to remove premium checks)
@Client.on_message(filters.private & filters.text & filters.incoming)
async def pm_search(client, message):
    if message.text.startswith("/") or not message.text: return # Ignore commands and empty messages
    stg = db.get_bot_sttgs() # Fetch bot settings synchronously
    if not stg.get('PM_SEARCH', True): # Default to True if not set
        return await message.reply_text('🔒 ᴘᴍ sᴇᴀʀᴄʜ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴅɪsᴀʙʟᴇᴅ.')

    if not stg.get('AUTO_FILTER', True): # Default to True
        return await message.reply_text('⚙️ ᴀᴜᴛᴏ ғɪʟᴛᴇʀ ᴡᴀs ᴅɪsᴀʙʟᴇᴅ ɢʟᴏʙᴀʟʟʏ.')

    s = await message.reply(f"<b><i>⏳ `{message.text}` sᴇᴀʀᴄʜɪɴɢ...</i></b>", quote=True)
    await auto_filter(client, message, s)


# Keep group_search function (already modified)
@Client.on_message(filters.group & filters.text & filters.incoming)
async def group_search(client, message):
    # Basic checks
    if not message.text or message.text.startswith("/"): return
    user_id = message.from_user.id if message and message.from_user else 0
    if not user_id: return # Ignore anonymous admins for searches

    stg = db.get_bot_sttgs()
    if stg.get('AUTO_FILTER', True): # Default to True
        # Support Group handling
        if message.chat.id == SUPPORT_GROUP:
            files, offset, total = await get_search_results(message.text) # Use async search
            if files:
                btn = [[ InlineKeyboardButton("➡️ ɢᴇᴛ ғɪʟᴇs ʜᴇʀᴇ", url=FILMS_LINK) ]]
                await message.reply_text(f'♢ ᴛᴏᴛᴀʟ {total} ʀᴇsᴜʟᴛs ғᴏᴜɴᴅ.\n♢ ᴘʟᴇᴀsᴇ sᴇᴀʀᴄʜ ɪɴ ᴛʜᴇ ᴍᴀɪɴ ᴄʜᴀᴛ.', reply_markup=InlineKeyboardMarkup(btn))
            return # Don't process further in support group

        # Admin tag handling
        elif '@admin' in message.text.lower() or '@admins' in message.text.lower():
            if await is_check_admin(client, message.chat.id, user_id): return # Admin tagged themselves
            admins = [member.user.id async for member in client.get_chat_members(chat_id=message.chat.id, filter=enums.ChatMembersFilter.ADMINISTRATORS) if not member.user.is_bot]
            owner = next((member async for member in client.get_chat_members(chat_id=message.chat.id, filter=enums.ChatMembersFilter.ADMINISTRATORS) if member.status == enums.ChatMemberStatus.OWNER), None)
            if owner and owner.user.id not in admins : admins.append(owner.user.id) # Ensure owner is included
            if not admins: return await message.reply("Couldn't find any admins to notify.")

            mention_target = message.reply_to_message or message
            mention_text = f"#ᴀᴛᴛᴇɴᴛɪᴏɴ\n♢ ᴜsᴇʀ: {message.from_user.mention}\n♢ ɢʀᴏᴜᴘ: {message.chat.title}\n\n♢ <a href={mention_target.link}>ʀᴇᴘᴏʀᴛᴇᴅ ᴍᴇssᴀɢᴇ</a>"
            reported_admin_ids = set() # Track who was notified

            for admin_id in admins:
                if admin_id in reported_admin_ids: continue # Avoid duplicate notifications
                try:
                    await mention_target.forward(admin_id)
                    await client.send_message(admin_id, mention_text, disable_web_page_preview=True)
                    reported_admin_ids.add(admin_id)
                    await asyncio.sleep(0.3) # Small delay
                except Exception as e: logger.error(f"Failed to notify admin {admin_id}: {e}")

            # Notify user confirmation (without tagging admins in group)
            if reported_admin_ids: await message.reply_text('✅ Report sent to admins!')
            else: await message.reply_text('❌ Could not notify any admins.')
            return

        # Link/Spam handling
        elif re.findall(r'https?://\S+|www\.\S+|t\.me/\S+', message.text): # Removed @ check, handled by admin tag
            if await is_check_admin(client, message.chat.id, user_id): return # Admins can post links
            try: await message.delete()
            except Exception as e: logger.warning(f"Failed to delete link message in {message.chat.id}: {e}")
            return # Just delete, no warning needed usually

        # Request handling
        elif '#request' in message.text.lower():
            if user_id in ADMINS: return # Admins don't need to use #request
            if not LOG_CHANNEL: return await message.reply("Request feature disabled.")
            try:
                request_text = re.sub(r'#request', '', message.text, flags=re.IGNORECASE).strip()
                if not request_text: return await message.reply("Please specify what you want to request after #request.")
                log_msg = f"#ʀᴇǫᴜᴇsᴛ\n♢ ᴜsᴇʀ: {message.from_user.mention} (`{user_id}`)\n♢ ɢʀᴏᴜᴘ: {message.chat.title} (`{message.chat.id}`)\n\n♢ ʀᴇǫᴜᴇsᴛ: {request_text}"
                await client.send_message(LOG_CHANNEL, log_msg)
                await message.reply_text("✅ ʀᴇǫᴜᴇsᴛ sᴇɴᴛ!")
            except Exception as e: logger.error(f"Failed to send request log: {e}"); await message.reply_text("❌ ғᴀɪʟᴇᴅ ᴛᴏ sᴇɴᴅ ʀᴇǫᴜᴇsᴛ.")
            return

        # Normal search
        else:
            # Prevent short messages if desired (optional)
            # if len(message.text) < 3: return
            s = await message.reply(f"<b><i>⏳ `{message.text}` sᴇᴀʀᴄʜɪɴɢ...</i></b>")
            await auto_filter(client, message, s)
    else: # Auto Filter is OFF globally
        pass # Do nothing

# --- Callback Handlers (Removed premium checks) ---

# Keep next_page (already modified)
@Client.on_callback_query(filters.regex(r"^next"))
async def next_page(bot, query):
    ident, req, key, offset = query.data.split("_")
    try: req_user_id = int(req)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != 0 and query.from_user.id != req_user_id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)
    try: offset = int(offset)
    except: offset = 0

    search = BUTTONS.get(key)
    cap = CAP.get(key)
    if not search or not cap:
        await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ. sᴇɴᴅ ᴀ ɴᴇᴡ ᴏɴᴇ.", show_alert=True)
        # Optionally edit the message to indicate expiry
        try: await query.message.edit_text("❌ ᴛʜɪs sᴇᴀʀᴄʜ ʜᴀs ᴇxᴘɪʀᴇᴅ. ᴘʟᴇᴀsᴇ sᴇᴀʀᴄʜ ᴀɢᴀɪɴ.")
        except: pass
        return

    # Fetch results using async function
    files, n_offset, total = await get_search_results(search, offset=offset)
    try: n_offset = int(n_offset) if n_offset else "" # Keep empty string if last page
    except: n_offset = ""

    if not files:
         # This case should ideally not happen if the initial search found results
         # But handle it just in case offset is wrong or cache mismatch
         await query.answer("No more files found on this page.", show_alert=False)
         # Optionally edit message if appropriate
         try:
              if offset == 0: # If it was the first page click but no files
                  await query.message.edit_text("sᴏʀʀʏ, ɴᴏ ғɪʟᴇs ғᴏᴜɴᴅ ғᴏʀ ᴛʜᴀᴛ ǫᴜᴇʀʏ ᴀɴʏᴍᴏʀᴇ.")
              else: # If it was a later page click
                  # Maybe disable the 'Next' button if possible, or just inform user
                  pass # query.answer is usually sufficient
         except: pass
         return

    temp.FILES[key] = files # Update file cache for this page view
    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ɪɴ {get_readable_time(DELETE_TIME)}.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    if settings.get('links', False): # Link mode
        for file_num, file in enumerate(files, start=offset+1):
            file_id_str = file.get('_id', 'N/A')
            file_name = file.get('file_name', 'N/A')
            file_size = file.get('file_size', 0)
            files_link += f"<b>\n\n{file_num}. <a href=https://t.me/{temp.U_NAME}?start=file_{query.message.chat.id}_{file_id_str}>[{get_size(file_size)}] {file_name}</a></b>"
    else: # Button mode
        for file in files:
             file_id_str = file.get('_id', 'N/A')
             file_name = file.get('file_name', 'N/A')
             file_size = file.get('file_size', 0)
             btn.append([InlineKeyboardButton(text=f"[{get_size(file_size)}] {file_name}", callback_data=f"file#{file_id_str}")])

    # Standard filter/update buttons
    btn.insert(0, [InlineKeyboardButton("• ᴜᴘᴅᴀᴛᴇs •", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"),
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

    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))

    if n_offset != "":
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"next_{req}_{key}_{n_offset}"))

    if pagination_row: btn.append(pagination_row)

    # Edit the message
    try:
        # Use the cached caption `cap`
        await query.message.edit_text(cap + files_link + del_msg, reply_markup=InlineKeyboardMarkup(btn), disable_web_page_preview=True, parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer() # Ignore if message is identical
    except FloodWait as e: logger.warning(f"FloodWait editing next_page: sleep {e.value}"); await asyncio.sleep(e.value); await query.answer("Retrying...", show_alert=False) # Optionally retry or just alert
    except Exception as e: logger.error(f"Error editing message in next_page: {e}"); await query.answer("An error occurred.", show_alert=True)


# Keep languages_ callback (no premium logic needed)
@Client.on_callback_query(filters.regex(r"^languages"))
async def languages_(client: Client, query: CallbackQuery):
    _, key, req, offset = query.data.split("#")
    try: req_user_id = int(req)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    search = BUTTONS.get(key) # Use original search query stored
    if not search: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    btn = []
    # Create buttons in pairs
    lang_iter = iter(LANGUAGES)
    for lang1 in lang_iter:
         try:
              lang2 = next(lang_iter)
              btn.append([
                   InlineKeyboardButton(text=lang1.title(), callback_data=f"lang_search#{lang1}#{key}#{offset}#{req}"),
                   InlineKeyboardButton(text=lang2.title(), callback_data=f"lang_search#{lang2}#{key}#{offset}#{req}")
              ])
         except StopIteration:
              # Handle odd number of languages
              btn.append([InlineKeyboardButton(text=lang1.title(), callback_data=f"lang_search#{lang1}#{key}#{offset}#{req}")])
              break

    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ʀᴇsᴜʟᴛs", callback_data=f"next_{req}_{key}_{offset}")]) # Go back to original offset
    try:
         await query.message.edit_text("<b>👇 sᴇʟᴇᴄᴛ ʏᴏᴜʀ ᴘʀᴇғᴇʀʀᴇᴅ ʟᴀɴɢᴜᴀɢᴇ:</b>", disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn))
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error in languages_ callback: {e}"); await query.answer("Error showing languages.", show_alert=True)


# Keep quality callback (no premium logic needed)
@Client.on_callback_query(filters.regex(r"^quality"))
async def quality(client: Client, query: CallbackQuery):
    _, key, req, offset = query.data.split("#")
    try: req_user_id = int(req)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    search = BUTTONS.get(key)
    if not search: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    btn = []
    qual_iter = iter(QUALITY)
    for q1 in qual_iter:
         try:
              q2 = next(qual_iter)
              btn.append([
                   InlineKeyboardButton(text=q1.upper(), callback_data=f"qual_search#{q1}#{key}#{offset}#{req}"),
                   InlineKeyboardButton(text=q2.upper(), callback_data=f"qual_search#{q2}#{key}#{offset}#{req}")
              ])
         except StopIteration:
              btn.append([InlineKeyboardButton(text=q1.upper(), callback_data=f"qual_search#{q1}#{key}#{offset}#{req}")])
              break

    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ʀᴇsᴜʟᴛs", callback_data=f"next_{req}_{key}_{offset}")])
    try:
        await query.message.edit_text("<b>👇 sᴇʟᴇᴄᴛ ʏᴏᴜʀ ᴘʀᴇғᴇʀʀᴇᴅ ǫᴜᴀʟɪᴛʏ:</b>", disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn))
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error in quality callback: {e}"); await query.answer("Error showing qualities.", show_alert=True)


# Keep lang_search (already modified)
@Client.on_callback_query(filters.regex(r"^lang_search"))
async def filter_languages_cb_handler(client: Client, query: CallbackQuery):
    _, lang, key, offset_str, req = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str) # Keep original offset for back button
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    # Use the original search query stored in BUTTONS, not CAP
    original_search = BUTTONS.get(key)
    cap = CAP.get(key) # Keep the original caption
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    # Search with language filter, start from offset 0 for the filtered list
    current_offset = 0 # Offset for the *filtered* results
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=lang, offset=current_offset)
    if not files: return await query.answer(f"ɴᴏ '{lang.title()}' ғɪʟᴇs ғᴏᴜɴᴅ.", show_alert=True)

    # Store filtered files temporarily if needed for pagination (optional, can refetch)
    # temp.FILES[f"{key}_{lang}"] = files # Example key for filtered results

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=1): # Start numbering from 1 for this page
            files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name']}", callback_data=f"file#{file['_id']}")])

    # Standard Buttons (adjust filter buttons)
    btn.insert(0, [InlineKeyboardButton("• ᴜᴘᴅᴀᴛᴇs •", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#{original_offset}"), # Back to lang selection
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0") # Start quality filter from page 0
    ])

    # Pagination for Filtered Results
    current_page = 1 # Always start at page 1 for filtered view
    total_pages = math.ceil(total_filtered / MAX_BTN)
    page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []
    # No "Back" on page 1 of filtered results

    if next_filtered_offset != "": # If more filtered pages exist
        pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
        # Pass necessary info for the next filtered page
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"lang_next#{req}#{key}#{lang}#{next_filtered_offset}#{original_offset}"))
    elif total_filtered > 0: # Show label if results exist but only one page
        pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if pagination_row: btn.append(pagination_row)

    # Back to original, unfiltered results
    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit lang_search: {e}"); await query.answer("Error.", show_alert=True)


# Keep qual_search (modify similarly to lang_search)
@Client.on_callback_query(filters.regex(r"^qual_search"))
async def quality_search(client: Client, query: CallbackQuery):
    _, qual, key, offset_str, req = query.data.split("#")
    try: req_user_id = int(req); original_offset = int(offset_str)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    # Search with quality filter (acts like language filter here), start from offset 0
    current_offset = 0
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=qual, offset=current_offset) # Use lang param for quality term
    if not files: return await query.answer(f"ɴᴏ '{qual.upper()}' ғɪʟᴇs ғᴏᴜɴᴅ.", show_alert=True)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=1): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name']}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("• ᴜᴘᴅᴀᴛᴇs •", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"), # Start lang filter from page 0
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#{original_offset}") # Back to quality selection
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


# Keep lang_next (adjust logic for filtered pagination)
@Client.on_callback_query(filters.regex(r"^lang_next"))
async def lang_next_page(bot, query):
    _, req, key, lang, current_filtered_offset_str, original_offset_str = query.data.split("#")
    try:
        req_user_id = int(req)
        current_filtered_offset = int(current_filtered_offset_str)
        original_offset = int(original_offset_str) # Keep track of original page offset
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    # Fetch next page of filtered results
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=lang, offset=current_filtered_offset)
    if not files: return await query.answer("No more files found.", show_alert=False) # Should not happen if next button exists, but safety

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []

    start_num = current_filtered_offset + 1 # Correct numbering for the current page
    if settings.get('links', False):
        for file_num, file in enumerate(files, start=start_num): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name']}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("• ᴜᴘᴅᴀᴛᴇs •", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#{original_offset}"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#0")
    ])

    # Pagination for Filtered Results
    current_page = math.ceil(current_filtered_offset / MAX_BTN) + 1
    total_pages = math.ceil(total_filtered / MAX_BTN)
    page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []

    prev_filtered_offset = max(0, current_filtered_offset - MAX_BTN)
    pagination_row.append(InlineKeyboardButton("« ʙᴀᴄᴋ", callback_data=f"lang_next#{req}#{key}#{lang}#{prev_filtered_offset}#{original_offset}"))

    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))

    if next_filtered_offset != "":
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"lang_next#{req}#{key}#{lang}#{next_filtered_offset}#{original_offset}"))

    btn.append(pagination_row)
    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit lang_next: {e}"); await query.answer("Error.", show_alert=True)


# Keep qual_next (modify similarly to lang_next)
@Client.on_callback_query(filters.regex(r"^qual_next"))
async def quality_next_page(bot, query):
    _, req, key, qual, current_filtered_offset_str, original_offset_str = query.data.split("#")
    try:
        req_user_id = int(req)
        current_filtered_offset = int(current_filtered_offset_str)
        original_offset = int(original_offset_str)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != query.from_user.id: return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    original_search = BUTTONS.get(key); cap = CAP.get(key)
    if not original_search or not cap: return await query.answer("ʀᴇǫᴜᴇsᴛ ᴇxᴘɪʀᴇᴅ.", show_alert=True)

    # Fetch next page of quality-filtered results
    files, next_filtered_offset, total_filtered = await get_search_results(original_search, lang=qual, offset=current_filtered_offset) # Use lang for quality
    if not files: return await query.answer("No more files found.", show_alert=False)

    settings = await get_settings(query.message.chat.id)
    del_msg = f"\n\n<b>⚠️ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛᴇ ᴇɴᴀʙʟᴇᴅ.</b>" if settings.get("auto_delete", False) else ''
    files_link = ''; btn = []
    start_num = current_filtered_offset + 1

    if settings.get('links', False):
        for file_num, file in enumerate(files, start=start_num): files_link += f"<b>\n\n{file_num}. <a href=...>[{get_size(file['file_size'])}] {file['file_name']}</a></b>"
    else:
        for file in files: btn.append([InlineKeyboardButton(f"[{get_size(file['file_size'])}] {file['file_name']}", callback_data=f"file#{file['_id']}")])

    btn.insert(0, [InlineKeyboardButton("• ᴜᴘᴅᴀᴛᴇs •", url=UPDATES_LINK)])
    btn.insert(1, [
        InlineKeyboardButton("⫸ ʟᴀɴɢᴜᴀɢᴇ", callback_data=f"languages#{key}#{req}#0"),
        InlineKeyboardButton("⫸ ǫᴜᴀʟɪᴛʏ", callback_data=f"quality#{key}#{req}#{original_offset}")
    ])

    current_page = math.ceil(current_filtered_offset / MAX_BTN) + 1
    total_pages = math.ceil(total_filtered / MAX_BTN)
    page_label = f"ᴘɢ {current_page}/{total_pages}"
    pagination_row = []

    prev_filtered_offset = max(0, current_filtered_offset - MAX_BTN)
    pagination_row.append(InlineKeyboardButton("« ʙᴀᴄᴋ", callback_data=f"qual_next#{req}#{key}#{qual}#{prev_filtered_offset}#{original_offset}"))
    pagination_row.append(InlineKeyboardButton(page_label, callback_data="buttons"))
    if next_filtered_offset != "":
        pagination_row.append(InlineKeyboardButton("ɴᴇxᴛ »", callback_data=f"qual_next#{req}#{key}#{qual}#{next_filtered_offset}#{original_offset}"))

    btn.append(pagination_row)
    btn.append([InlineKeyboardButton(text="⪻ ʙᴀᴄᴋ ᴛᴏ ᴀʟʟ", callback_data=f"next_{req}_{key}_{original_offset}")])

    try: await query.message.edit_text(cap + files_link + del_msg, disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(btn), parse_mode=enums.ParseMode.HTML)
    except MessageNotModified: await query.answer()
    except Exception as e: logger.error(f"Error edit qual_next: {e}"); await query.answer("Error.", show_alert=True)


# Keep spolling callback (no premium logic)
@Client.on_callback_query(filters.regex(r"^spolling"))
async def advantage_spoll_choker(bot, query):
    _, id, user = query.data.split('#')
    try: req_user_id = int(user)
    except ValueError: return await query.answer("Invalid request data.", show_alert=True)

    if req_user_id != 0 and query.from_user.id != req_user_id:
        return await query.answer(f"ʜᴇʟʟᴏ {query.from_user.first_name},\nᴛʜɪs ɪs ɴᴏᴛ ғᴏʀ ʏᴏᴜ!", show_alert=True)

    await query.answer("Searching for the correct title...")
    try:
        movie = await get_poster(id, id=True) # Use refined get_poster
        if not movie: raise ValueError("IMDb lookup failed")
        search = movie.get('title', "N/A")
        if search == "N/A": raise ValueError("Could not get title from IMDb")
        # Clean title slightly for search
        search_cleaned = re.sub(r"[:()]", "", search).strip() # Basic cleaning
    except Exception as e:
        logger.error(f"Error getting poster for spell check ID {id}: {e}")
        await query.message.edit(f"❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ғᴇᴛᴄʜ ᴅᴇᴛᴀɪʟs ғᴏʀ ᴛʜᴀᴛ sᴜɢɢᴇsᴛɪᴏɴ.")
        return

    s = await query.message.edit(f"<b><i>✅ ᴏᴋᴀʏ, sᴇᴀʀᴄʜɪɴɢ ғᴏʀ: `{search_cleaned}`</i></b>")
    # Use async search, start from offset 0
    files, offset, total_results = await get_search_results(search_cleaned, offset=0)
    if files:
        k = (search_cleaned, files, offset, total_results) # Package results for auto_filter
        await auto_filter(bot, query, s, spoll=k) # Pass query, status msg, and results
    else:
        # No results even after spell check
        k = await s.edit(script.NOT_FILE_TXT.format(query.from_user.mention, search_cleaned))
        # Log no result after spell check correction
        try: await bot.send_message(LOG_CHANNEL, f"#ɴᴏ_ʀᴇsᴜʟᴛ_ᴀғᴛᴇʀ_sᴘᴇʟʟ\n\n♢ ʀᴇǫᴜᴇsᴛᴇʀ: {query.from_user.mention}\n♢ ᴏʀɪɢɪɴᴀʟ: `{BUTTONS.get(query.message.reply_markup.inline_keyboard[0][0].callback_data.split('#')[1], 'Unknown')}`\n♢ sᴜɢɢᴇsᴛᴇᴅ: `{search_cleaned}`")
        except: pass
        await asyncio.sleep(60) # Keep message longer
        try: await k.delete()
        except: pass


# Keep main cb_handler (already modified to remove premium)
# Keep auto_filter function (already modified)
# Keep advantage_spell_chok function (modified slightly for clarity)
async def advantage_spell_chok(message, s):
    search = message.text
    # Google Search button
    google_search_url = f"https://www.google.com/search?q={re.sub(r' ', '+', search)}"
    btn = [[ InlineKeyboardButton("❓ ɪɴsᴛʀᴜᴄᴛɪᴏɴs", callback_data='instructions'),
             InlineKeyboardButton("🌍 sᴇᴀʀᴄʜ ɢᴏᴏɢʟᴇ", url=google_search_url) ]]
    try:
        # Use async get_poster
        movies = await get_poster(search, bulk=True)
    except Exception as e:
         logger.error(f"Error getting posters for spell check '{search}': {e}")
         movies = None # Handle error case

    if not movies:
        n = await s.edit(text=script.NOT_FILE_TXT.format(message.from_user.mention, search), reply_markup=InlineKeyboardMarkup(btn))
        # Log no results (even without suggestions)
        try: await message._client.send_message(LOG_CHANNEL, f"#ɴᴏ_ʀᴇsᴜʟᴛ\n\n♢ ʀᴇǫᴜᴇsᴛᴇʀ: {message.from_user.mention}\n♢ ǫᴜᴇʀʏ: `{search}`")
        except Exception as log_e: logger.error(f"Error logging NO_RESULT: {log_e}")
        # Auto-delete handled by main auto_filter logic potentially
        return # Important to return here

    # Filter out potential duplicates based on ID, keep order somewhat
    seen_ids = set()
    unique_movies = []
    for movie in movies:
        if movie.movieID not in seen_ids:
            unique_movies.append(movie)
            seen_ids.add(movie.movieID)
        if len(unique_movies) >= 7: break # Limit suggestions

    user = message.from_user.id if message.from_user else 0
    buttons = []
    for movie in unique_movies:
        title = movie.get('title', 'Unknown Title')
        year = f"({movie.get('year')})" if movie.get('year') else ""
        buttons.append([InlineKeyboardButton(text=f"{title} {year}".strip(), callback_data=f"spolling#{movie.movieID}#{user}")])

    buttons.append([InlineKeyboardButton("🚫 ᴄʟᴏsᴇ", callback_data="close_data")])

    await s.edit(text=f"👋 ʜᴇʟʟᴏ {message.from_user.mention},\n\nɪ ᴄᴏᴜʟᴅɴ'ᴛ ғɪɴᴅ ᴇxᴀᴄᴛʟʏ `<b>{search}</b>`.\nᴅɪᴅ ʏᴏᴜ ᴍᴇᴀɴ ᴏɴᴇ ᴏғ ᴛʜᴇsᴇ? 👇",
               reply_markup=InlineKeyboardMarkup(buttons))
    # Auto-delete might be handled by main filter logic or remove manual sleep here
