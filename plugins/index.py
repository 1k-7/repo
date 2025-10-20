import re
import time
import asyncio
from math import ceil
from hydrogram import Client, filters, enums
from hydrogram.errors import FloodWait, MessageNotModified
from hydrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from info import ADMINS, INDEX_EXTENSIONS
from database.ia_filterdb import save_file # Assuming save_file is now async
from utils import temp, get_readable_time
import logging

logger = logging.getLogger(__name__)
lock = asyncio.Lock()

# Global cache for status
index_status_cache = {"text": "Initializing...", "last_update": 0}

@Client.on_callback_query(filters.regex(r'^index'))
async def index_files_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    # Only admins can start/cancel indexing
    if user_id not in ADMINS:
        return await query.answer("ᴏɴʟʏ ᴀᴅᴍɪɴs ᴄᴀɴ ᴍᴀɴᴀɢᴇ ɪɴᴅᴇxɪɴɢ.", show_alert=True)

    try:
        data_parts = query.data.split("#")
        if len(data_parts) != 5: raise ValueError("Incorrect callback data format")
        _, ident, chat, lst_msg_id_str, skip_str = data_parts
        lst_msg_id = int(lst_msg_id_str)
        skip = int(skip_str)
        # Handle chat ID (int or username string)
        try: chat_id_int = int(chat)
        except ValueError: chat_id_int = chat # Keep as string
    except (ValueError, IndexError) as e:
        logger.error(f"Error splitting index callback data '{query.data}': {e}")
        return await query.answer("Invalid callback data.", show_alert=True)

    if ident == 'yes':
        if lock.locked():
             return await query.answer("⏳ ᴀɴᴏᴛʜᴇʀ ɪɴᴅᴇxɪɴɢ ɪs ᴀʟʀᴇᴀᴅʏ ɪɴ ᴘʀᴏɢʀᴇss.", show_alert=True)
        msg = query.message
        await msg.edit("⏳ sᴛᴀʀᴛɪɴɢ ɪɴᴅᴇxɪɴɢ...")
        # Start indexing in background, don't await here directly if it's long
        asyncio.create_task(index_files_to_db(lst_msg_id, chat_id_int, msg, bot, skip))
        await query.answer("Indexing started in background.", show_alert=False)
    elif ident == 'cancel':
        if not temp.CANCEL:
             temp.CANCEL = True
             logger.warning(f"User {user_id} requested indexing cancellation.")
             await query.message.edit("❗️ ᴛʀʏɪɴɢ ᴛᴏ ᴄᴀɴᴄᴇʟ ɪɴᴅᴇxɪɴɢ...")
             await query.answer("Cancellation request sent.", show_alert=False)
        else:
             await query.answer("Cancellation already requested.", show_alert=False)

@Client.on_message(filters.command('index') & filters.private & filters.user(ADMINS))
async def send_for_index(bot, message):
    if lock.locked():
        return await message.reply('⏳ ᴀɴᴏᴛʜᴇʀ ɪɴᴅᴇxɪɴɢ ᴘʀᴏᴄᴇss ɪs ʀᴜɴɴɪɴɢ. ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ.')

    ask_msg = None
    response_msg = None
    try:
        ask_msg = await message.reply("➡️ ғᴏʀᴡᴀʀᴅ ᴛʜᴇ ʟᴀsᴛ ᴍᴇssᴀɢᴇ ғʀᴏᴍ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ (ᴡɪᴛʜ ǫᴜᴏᴛᴇs)\nᴏʀ sᴇɴᴅ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ's ʟᴀsᴛ ᴍᴇssᴀɢᴇ ʟɪɴᴋ.")
        response_msg = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id, timeout=120)
    except asyncio.TimeoutError:
        if ask_msg: await ask_msg.edit("⏰ ᴛɪᴍᴇᴏᴜᴛ. ɪɴᴅᴇxɪɴɢ ᴄᴀɴᴄᴇʟʟᴇᴅ.")
        return
    except Exception as e:
         if ask_msg: await ask_msg.edit(f"An error occurred: {e}")
         return
    finally:
         # Simplified finally block
         if ask_msg:
             try: await ask_msg.delete()
             except: pass

    if not response_msg: # Check if response_msg was captured
        logger.warning("No response received for index command.")
        return

    # Process the response message
    chat_id = None
    last_msg_id = None
    if response_msg.forward_from_chat and response_msg.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = response_msg.forward_from_message_id
        chat_id = response_msg.forward_from_chat.username or response_msg.forward_from_chat.id
        logger.info(f"Received forward: chat_id={chat_id}, last_msg_id={last_msg_id}")
    elif response_msg.text and response_msg.text.startswith(("https://t.me/", "http://t.me/")):
        try:
            msg_link = response_msg.text.strip()
            # Regex to handle public (username)/private (c/ID) links
            match = re.match(r"https?://t\.me/(?:c/)?(\w+)/(\d+)", msg_link)
            if match:
                 channel_part = match.group(1)
                 last_msg_id = int(match.group(2))
                 # Try converting to int for private channels, keep as username for public
                 try: chat_id = int(f"-100{channel_part}")
                 except ValueError: chat_id = channel_part
                 logger.info(f"Received link: chat_id={chat_id}, last_msg_id={last_msg_id}")
            else:
                 await response_msg.reply('⚠️ ɪɴᴠᴀʟɪᴅ ᴍᴇssᴀɢᴇ ʟɪɴᴋ ғᴏʀᴍᴀᴛ.')
                 return
        except (ValueError, IndexError, Exception) as e:
            await response_msg.reply(f'⚠️ ɪɴᴠᴀʟɪᴅ ᴍᴇssᴀɢᴇ ʟɪɴᴋ ({e}).')
            return
    else:
        await response_msg.reply('❌ ᴛʜɪs ɪs ɴᴏᴛ ᴀ ᴠᴀʟɪᴅ ғᴏʀᴡᴀʀᴅᴇᴅ ᴍᴇssᴀɢᴇ ᴏʀ ᴄʜᴀɴɴᴇʟ ᴍᴇssᴀɢᴇ ʟɪɴᴋ.')
        return

    # Verify chat access and type
    try:
        chat = await bot.get_chat(chat_id)
        if chat.type != enums.ChatType.CHANNEL:
            return await response_msg.reply("❌ ɪ ᴄᴀɴ ᴏɴʟʏ ɪɴᴅᴇx ᴄʜᴀɴɴᴇʟs.")
    except Exception as e:
        logger.error(f"Error getting chat {chat_id}: {e}")
        return await response_msg.reply(f'❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴀᴄᴄᴇss ᴛʜᴇ ᴄʜᴀɴɴᴇʟ/ʟɪɴᴋ.\nᴇʀʀᴏʀ: {e}')

    # Ask for skip number
    skip_ask_msg = None
    skip_response = None
    try:
        skip_ask_msg = await response_msg.reply("🔢 ᴇɴᴛᴇʀ ᴛʜᴇ ɴᴜᴍʙᴇʀ ᴏғ ᴍᴇssᴀɢᴇs ᴛᴏ sᴋɪᴘ ғʀᴏᴍ ᴛʜᴇ sᴛᴀʀᴛ (ᴇ.ɢ., `0` ᴛᴏ sᴋɪᴘ ɴᴏɴᴇ).")
        skip_response = await bot.listen(chat_id=message.chat.id, user_id=message.from_user.id, timeout=60)
    except asyncio.TimeoutError:
         if skip_ask_msg: await skip_ask_msg.edit("⏰ ᴛɪᴍᴇᴏᴜᴛ. ɪɴᴅᴇxɪɴɢ ᴄᴀɴᴄᴇʟʟᴇᴅ.")
         return
    except Exception as e:
         if skip_ask_msg: await skip_ask_msg.edit(f"An error occurred: {e}")
         return
    finally:
         # Simplified finally block
         if skip_ask_msg:
             try: await skip_ask_msg.delete()
             except: pass

    if not skip_response: # Check if skip_response was captured
        logger.warning("No response received for skip number.")
        return

    try:
        skip = int(skip_response.text.strip())
        if skip < 0: raise ValueError("Skip number cannot be negative")
    except ValueError:
        await skip_response.reply("❌ ɪɴᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ. ᴘʟᴇᴀsᴇ ᴇɴᴛᴇʀ ᴀ ᴘᴏsɪᴛɪᴠᴇ ɪɴᴛᴇɢᴇʀ.")
        return

    # Confirmation
    buttons = [[ InlineKeyboardButton('✅ ʏᴇs', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}') ],
               [ InlineKeyboardButton('❌ ɴᴏ', callback_data='close_data') ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    await skip_response.reply(f'❓ ᴅᴏ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ɪɴᴅᴇx `{chat.title}`?\n\nᴛᴏᴛᴀʟ ᴍᴇssᴀɢᴇs: ~<code>{last_msg_id}</code>\nsᴋɪᴘᴘɪɴɢ: <code>{skip}</code>', reply_markup=reply_markup)


# --- Helper for Progress Bar ---
def get_progress_bar(percent, length=10):
    filled = int(length * percent / 100)
    unfilled = length - filled
    return '█' * filled + '▒' * unfilled

# --- Modified index_files_to_db Function ---
async def index_files_to_db(lst_msg_id, chat, msg, bot, skip):
    total_files = 0; duplicate = 0; errors = 0; deleted = 0; no_media = 0; unsupported = 0
    BATCH_SIZE = 200; start_time = time.time(); last_edit_time = 0; EDIT_INTERVAL = 20 # Edit interval in seconds
    global index_status_cache # Use the global cache

    # Ensure lock is acquired before proceeding
    if lock.locked():
         logger.warning("index_files_to_db called while lock was already held. This shouldn't happen.")
         try: await msg.edit("⚠️ Indexing lock is already held. Please wait for the current process.")
         except: pass
         return

    async with lock: # Acquire lock
        try:
            current = skip; temp.CANCEL = False
            total_messages_in_channel = lst_msg_id
            total_fetch = total_messages_in_channel - current
            if total_fetch <= 0:
                 final_text = "🚫 ɴᴏ ɴᴇᴡ ᴍᴇssᴀɢᴇs ᴛᴏ ɪɴᴅᴇx."; await msg.edit(final_text); index_status_cache = {"text": final_text, "last_update": time.time()}; return

            batches = ceil(total_fetch / BATCH_SIZE) if BATCH_SIZE > 0 else 0
            batch_times = []; status_callback_data = f'index_status#{chat}#{lst_msg_id}#{skip}'

            initial_edit = (f"⏳ ɪɴᴅᴇxɪɴɢ sᴛᴀʀᴛɪɴɢ...\n~ sᴋɪᴘᴘɪɴɢ ғɪʀsᴛ {current} ᴍᴇssᴀɢᴇs.")
            index_status_cache = {"text": initial_edit, "last_update": time.time()}

            try:
                await msg.edit(
                    initial_edit,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('📊 sᴛᴀᴛᴜs', callback_data=status_callback_data)],
                        [InlineKeyboardButton('❌ ᴄᴀɴᴄᴇʟ', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
                last_edit_time = time.time()
            except FloodWait as e: await asyncio.sleep(e.value); last_edit_time = time.time()
            except Exception as initial_edit_e: logger.error(f"Error editing initial index msg: {initial_edit_e}")

            # --- Batch Processing Loop ---
            for batch_num in range(batches):
                if temp.CANCEL: logger.warning("Cancellation requested, breaking index loop."); break
                batch_start_time = time.time()
                start_id = current + 1; end_id = min(current + BATCH_SIZE, lst_msg_id)
                message_ids = list(range(start_id, end_id + 1))
                if not message_ids: continue

                messages_in_batch = []
                try: # Fetch messages
                    messages_in_batch = await bot.get_messages(chat, message_ids)
                    messages_in_batch = [m for m in messages_in_batch if m is not None]
                except FloodWait as e:
                     logger.warning(f"FloodWait on get_messages: sleep {e.value}s"); await asyncio.sleep(e.value);
                     try: messages_in_batch = await bot.get_messages(chat, message_ids); messages_in_batch = [m for m in messages_in_batch if m is not None] # Retry
                     except Exception as retry_e: errors += len(message_ids); current = end_id; logger.error(f"Retry fetch failed: {retry_e}"); continue
                # **** THIS IS THE CORRECTED LINE ****
                except Exception as e: errors += len(message_ids); current = end_id; logger.error(f"Fetch batch error: {e}"); continue
                # **** END CORRECTION ****

                batch_processed_count = len(message_ids); batch_found_count = len(messages_in_batch)
                deleted += (batch_processed_count - batch_found_count)
                save_tasks = []

                # Process messages in batch
                for message in messages_in_batch:
                     if message.empty: continue
                     if not message.media: no_media += 1; continue
                     if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.AUDIO, enums.MessageMediaType.DOCUMENT]: unsupported += 1; continue
                     media = getattr(message, message.media.value, None)
                     if not media or not hasattr(media, 'file_name') or not media.file_name: unsupported += 1; continue
                     file_name_lower = media.file_name.lower()
                     if not any(file_name_lower.endswith("." + ext.lstrip('.')) for ext in INDEX_EXTENSIONS): unsupported += 1; continue
                     media.caption = message.caption
                     save_tasks.append(save_file(media)) # save_file is async

                # Save files concurrently
                if save_tasks:
                    results = await asyncio.gather(*save_tasks, return_exceptions=True)
                    for result in results:
                         if isinstance(result, Exception): errors += 1; logger.error(f"Save error: {result}")
                         elif result == 'suc': total_files += 1
                         elif result == 'dup': duplicate += 1
                         elif result == 'err': errors += 1
                current = end_id # Update position

                # --- Update Status Cache ---
                batch_time = time.time() - batch_start_time; batch_times.append(batch_time)
                elapsed = time.time() - start_time; progress = current - skip
                percentage = min((progress / total_fetch) * 100, 100.0) if total_fetch > 0 else 100.0
                avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0.1
                remaining_messages = lst_msg_id - current
                remaining_batches = ceil(remaining_messages / BATCH_SIZE) if BATCH_SIZE > 0 else 0
                eta = remaining_batches * avg_batch_time
                progress_bar_str = get_progress_bar(int(percentage))
                status_text_for_cache = (
                     f"📊 ɪɴᴅᴇxɪɴɢ ᴘʀᴏɢʀᴇss\n\n"
                     f"▷ ʙᴀᴛᴄʜ: {batch_num + 1}/{batches}\n"
                     f"▷ {progress_bar_str} {percentage:.1f}%\n"
                     f"▷ ғᴇᴛᴄʜᴇᴅ: {progress}/{total_fetch}\n"
                     f"▷ sᴀᴠᴇᴅ: {total_files} | ᴅᴜᴘ: {duplicate}\n"
                     f"▷ ᴅᴇʟ: {deleted} | ᴜɴsᴜᴘ: {no_media + unsupported}\n"
                     f"▷ ᴇʀʀᴏʀs: {errors}\n"
                     f"▷ ᴇʟᴀᴘ: {get_readable_time(elapsed)}\n"
                     f"▷ ᴇᴛᴀ: {get_readable_time(eta)}"
                 )
                index_status_cache = {"text": status_text_for_cache, "last_update": time.time()}

                # --- Edit message less frequently ---
                current_time = time.time()
                if current_time - last_edit_time > EDIT_INTERVAL or batch_num == batches - 1:
                    edit_text = (f"⏳ ɪɴᴅᴇxɪɴɢ... {progress_bar_str} {percentage:.1f}%\n"
                                f"~ {progress}/{total_fetch} ᴘʀᴏᴄᴇssᴇᴅ.")
                    try:
                        await msg.edit_text(text=edit_text, reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton('📊 sᴛᴀᴛᴜs', callback_data=status_callback_data)],
                            [InlineKeyboardButton('❌ ᴄᴀɴᴄᴇʟ', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
                        last_edit_time = current_time
                    except FloodWait as e: await asyncio.sleep(e.value); last_edit_time = time.time()
                    except MessageNotModified: pass
                    except Exception as e_edit: logger.error(f"Edit error: {e_edit}")

            # --- Final Status ---
            elapsed = time.time() - start_time
            final_status_msg = "✅ ɪɴᴅᴇxɪɴɢ ᴄᴏᴍᴘʟᴇᴛᴇᴅ!" if not temp.CANCEL else "🛑 ɪɴᴅᴇxɪɴɢ ᴄᴀɴᴄᴇʟʟᴇᴅ!"
            final_text_summary = (f"{final_status_msg}\nᴛᴏᴏᴋ {get_readable_time(elapsed)}\n\n"
                                  f"▷ ᴘʀᴏᴄᴇssᴇᴅ: {current - skip}\n"
                                  f"▷ sᴀᴠᴇᴅ: {total_files}\n"
                                  f"▷ ᴅᴜᴘʟɪᴄᴀᴛᴇs: {duplicate}\n"
                                  f"▷ ᴅᴇʟ/ᴜɴᴀᴠᴀɪʟ: {deleted}\n"
                                  f"▷ ɴᴏɴ/ᴜɴsᴜᴘ: {no_media + unsupported}\n"
                                  f"▷ ᴇʀʀᴏʀs: {errors}")
            index_status_cache = {"text": final_text_summary, "last_update": time.time()}
            await msg.edit(final_text_summary, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('ᴄʟᴏsᴇ', callback_data='close_data')]]))

        except Exception as e:
            logger.exception(f"Fatal indexing error: {e}") # Log full traceback
            final_error_text = f'❌ ɪɴᴅᴇxɪɴɢ ᴇʀʀᴏʀ: {e}'
            index_status_cache = {"text": final_error_text, "last_update": time.time()}
            try: await msg.edit(final_error_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('ᴄʟᴏsᴇ', callback_data='close_data')]]))
            except: pass # Ignore if msg edit fails on error
        finally:
            temp.CANCEL = False # Always reset cancel flag
            logger.info("Indexing process finished or cancelled.")
            # Lock is automatically released by 'async with'

# --- Status Button Callback ---
@Client.on_callback_query(filters.regex(r"^index_status"))
async def index_status_alert(bot, query: CallbackQuery):
    global index_status_cache
    last_updated_ago = time.time() - index_status_cache.get("last_update", time.time()) # Avoid error if no update yet
    status_text = index_status_cache.get("text", "sᴛᴀᴛᴜs ɴᴏᴛ ʀᴇᴀᴅʏ ʏᴇᴛ.")
    update_info = f"\n\n(ᴜᴘᴅᴀᴛᴇᴅ: {int(last_updated_ago)}s ᴀɢᴏ)"

    try:
        # Split status text if too long for alert
        max_alert_len = 190 # Approx limit for alerts
        display_text = status_text
        if len(status_text + update_info) > max_alert_len:
             display_text = status_text[:max_alert_len - len(update_info) - 3] + "..." # Truncate main text

        await query.answer(text=display_text + update_info, show_alert=True, cache_time=2)
    except MessageTooLong: await query.answer("Status too long for alert.", show_alert=True) # Fallback
    except Exception as e: logger.error(f"Index status alert error: {e}"); await query.answer("Error fetching status.", show_alert=True)
