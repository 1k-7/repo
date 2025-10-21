import re
import time
import asyncio
from math import ceil
from hydrogram import Client, filters, enums
from hydrogram.errors import FloodWait, MessageNotModified, MessageTooLong # Added MessageTooLong
from hydrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from info import ADMINS, INDEX_EXTENSIONS
from database.ia_filterdb import save_file # Assuming save_file is now async
from utils import temp, get_readable_time
import logging

logger = logging.getLogger(__name__)
lock = asyncio.Lock()

# Global cache for status
index_status_cache = {"text": "Initializing...", "last_update": 0}

# More specific regex to only catch index#yes or index#cancel
@Client.on_callback_query(filters.regex(r'^index#(yes|cancel)'))
async def index_files_callback(bot, query: CallbackQuery):
    user_id = query.from_user.id
    # Only admins can start/cancel indexing
    if user_id not in ADMINS:
        return await query.answer("ᴏɴʟʏ ᴀᴅᴍɪɴs ᴄᴀɴ ᴍᴀɴᴀɢᴇ ɪɴᴅᴇxɪɴɢ.", show_alert=True)

    try:
        # Expected format: index#{ident}#{chat}#{lst_msg_id_str}#{skip_str}
        # Example: index#yes#-100123456#5000#0
        parts = query.data.split("#")
        if len(parts) != 5: raise ValueError("Incorrect callback data format for index control")
        _, ident, chat, lst_msg_id_str, skip_str = parts # Use parts instead of data_parts
        lst_msg_id = int(lst_msg_id_str)
        skip = int(skip_str)
        # Handle chat ID (int or username string)
        try: chat_id_int = int(chat)
        except ValueError: chat_id_int = chat # Keep as string if it's not purely numeric (username)
    except (ValueError, IndexError) as e:
        logger.error(f"Error splitting index control callback data '{query.data}': {e}")
        return await query.answer("Invalid index control callback data.", show_alert=True)

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
         if ask_msg:
             try: await ask_msg.delete()
             except: pass

    if not response_msg:
        logger.warning("No response received for index command.")
        return

    chat_id = None
    last_msg_id = None
    if response_msg.forward_from_chat and response_msg.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = response_msg.forward_from_message_id
        chat_id = response_msg.forward_from_chat.username or response_msg.forward_from_chat.id
        logger.info(f"Received forward: chat_id={chat_id}, last_msg_id={last_msg_id}")
    elif response_msg.text and response_msg.text.startswith(("https://t.me/", "http://t.me/")):
        try:
            msg_link = response_msg.text.strip()
            match = re.match(r"https?://t\.me/(?:c/)?(\w+)/(\d+)", msg_link)
            if match:
                 channel_part = match.group(1)
                 last_msg_id = int(match.group(2))
                 try: chat_id = int(f"-100{channel_part}") # Handle private channel ID format
                 except ValueError: chat_id = channel_part # Assume public username
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
        return await response_msg.reply(f'❌ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴀᴄᴄᴇss ᴛʜᴇ ᴄʜᴀɴɴᴇʟ/ʟɪɴᴋ.\nMake sure the bot is an admin in the channel.\nError: {e}')

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
         if skip_ask_msg:
             try: await skip_ask_msg.delete()
             except: pass

    if not skip_response:
        logger.warning("No response received for skip number.")
        return

    try:
        skip = int(skip_response.text.strip())
        if skip < 0: raise ValueError("Skip number cannot be negative")
    except ValueError:
        await skip_response.reply("❌ ɪɴᴠᴀʟɪᴅ ɴᴜᴍʙᴇʀ. ᴘʟᴇᴀsᴇ ᴇɴᴛᴇʀ ᴀ ᴘᴏsɪᴛɪᴠᴇ ɪɴᴛᴇɢᴇʀ or 0.")
        return

    # Confirmation
    # Use chat_id (which can be int or string) directly in callback_data
    buttons = [[ InlineKeyboardButton('✅ ʏᴇs, Start Indexing', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}') ],
               [ InlineKeyboardButton('❌ ɴᴏ, Cancel', callback_data='close_data') ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    await skip_response.reply(f'❓ Do you want to index messages from channel `{chat.title}`?\n\n • Total Messages: ~<code>{last_msg_id}</code>\n • Skipping First: <code>{skip}</code> messages', reply_markup=reply_markup)


def get_progress_bar(percent, length=10):
    filled = int(length * percent / 100)
    unfilled = length - filled
    return '█' * filled + '▒' * unfilled

async def index_files_to_db(lst_msg_id, chat, msg, bot, skip):
    total_files = 0; duplicate = 0; errors = 0; deleted = 0; no_media = 0; unsupported = 0
    BATCH_SIZE = 200; start_time = time.time(); last_edit_time = 0; EDIT_INTERVAL = 20 # Edit interval in seconds
    global index_status_cache

    if lock.locked():
         logger.warning("index_files_to_db called while lock was already held.")
         try: await msg.edit("⚠️ Indexing lock is already held. Please wait.")
         except: pass
         return

    async with lock:
        try:
            current = skip; temp.CANCEL = False
            total_messages_in_channel = lst_msg_id
            # Estimate total messages to fetch (might be slightly off if messages deleted at the end)
            total_fetch = total_messages_in_channel - current
            if total_fetch <= 0:
                 final_text = "🚫 No new messages to index (Total <= Skip)."; await msg.edit(final_text); index_status_cache = {"text": final_text, "last_update": time.time()}; return

            batches = ceil(total_fetch / BATCH_SIZE) if BATCH_SIZE > 0 else 0
            if batches == 0 and total_fetch > 0: batches = 1 # Ensure at least one batch if there's anything to fetch
            batch_times = []
            # Status button callback data doesn't need extra info
            status_callback_data = 'index_status'

            initial_edit = (f"⏳ Indexing starting for channel ID: `{chat}`\n~ Skipping first {current} messages.")
            index_status_cache = {"text": initial_edit, "last_update": time.time()}

            try:
                await msg.edit(
                    initial_edit,
                    reply_markup=InlineKeyboardMarkup([
                        # Use simple 'index_status' for the callback data
                        [InlineKeyboardButton('📊 Status', callback_data=status_callback_data)],
                        [InlineKeyboardButton('❌ Cancel', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
                last_edit_time = time.time()
            except FloodWait as e: await asyncio.sleep(e.value); last_edit_time = time.time()
            except Exception as initial_edit_e: logger.error(f"Error editing initial index msg: {initial_edit_e}")

            # --- Batch Processing Loop ---
            for batch_num in range(batches):
                if temp.CANCEL: logger.warning("Cancellation requested, breaking index loop."); break
                batch_start_time = time.time()
                start_id = current + 1; end_id = min(current + BATCH_SIZE, lst_msg_id)
                message_ids = list(range(start_id, end_id + 1))
                if not message_ids: continue # Should not happen with ceil logic, but safety check

                messages_in_batch = []
                try: # Fetch messages
                    messages_in_batch = await bot.get_messages(chat, message_ids)
                    # Filter out potential None results if some messages weren't found
                    messages_in_batch = [m for m in messages_in_batch if m is not None]
                except FloodWait as e:
                     logger.warning(f"FloodWait on get_messages: sleep {e.value}s"); await asyncio.sleep(e.value);
                     try: messages_in_batch = await bot.get_messages(chat, message_ids); messages_in_batch = [m for m in messages_in_batch if m is not None] # Retry
                     except Exception as retry_e: errors += len(message_ids); current = end_id; logger.error(f"Retry fetch failed after FloodWait: {retry_e}"); continue # Skip batch on retry fail
                except Exception as e: errors += len(message_ids); current = end_id; logger.error(f"Fetch batch error ({start_id}-{end_id}): {e}"); continue # Skip batch on error

                batch_processed_count = len(message_ids) # How many IDs we requested
                batch_found_count = len(messages_in_batch) # How many messages we actually got
                deleted += (batch_processed_count - batch_found_count) # Assume difference is deleted/inaccessible
                save_tasks = []

                # Process messages in batch
                for message in messages_in_batch:
                     # Skip empty messages (though get_messages usually filters these)
                     if message.empty: continue
                     # Check for media
                     if not message.media: no_media += 1; continue
                     # Check media type (Video, Audio, Document)
                     if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.AUDIO, enums.MessageMediaType.DOCUMENT]: unsupported += 1; continue
                     # Get the media object
                     media = getattr(message, message.media.value, None)
                     # Check if media object exists and has a file_name attribute
                     if not media or not hasattr(media, 'file_name') or not media.file_name: unsupported += 1; continue
                     # Check file extension
                     file_name_lower = media.file_name.lower()
                     if not any(file_name_lower.endswith("." + ext.lstrip('.')) for ext in INDEX_EXTENSIONS): unsupported += 1; continue
                     # Add caption to media object for saving
                     media.caption = message.caption
                     # Add save task
                     save_tasks.append(save_file(media)) # save_file is async

                # Save files concurrently
                if save_tasks:
                    results = await asyncio.gather(*save_tasks, return_exceptions=True)
                    for result in results:
                         if isinstance(result, Exception): errors += 1; logger.error(f"Save error during gather: {result}")
                         elif result == 'suc': total_files += 1
                         elif result == 'dup': duplicate += 1
                         elif result == 'err': errors += 1
                current = end_id # Update position regardless of save errors

                # --- Update Status Cache ---
                batch_time = time.time() - batch_start_time; batch_times.append(batch_time)
                elapsed = time.time() - start_time; progress = current - skip # How many messages processed after skip
                percentage = min((progress / total_fetch) * 100, 100.0) if total_fetch > 0 else 100.0
                avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0.1 # Avoid division by zero
                remaining_messages = lst_msg_id - current
                remaining_batches = ceil(remaining_messages / BATCH_SIZE) if BATCH_SIZE > 0 else 0
                eta = remaining_batches * avg_batch_time
                progress_bar_str = get_progress_bar(int(percentage))
                status_text_for_cache = (
                     f"📊 Indexing Progress (Channel: `{chat}`)\n\n"
                     f"▷ Batch: {batch_num + 1}/{batches}\n"
                     f"▷ {progress_bar_str} {percentage:.1f}%\n"
                     f"▷ Processed: {progress}/{total_fetch} (Current ID: {current})\n"
                     f"▷ Saved: {total_files} | Duplicates: {duplicate}\n"
                     f"▷ Skipped (NoMedia/Type/Ext): {no_media + unsupported}\n"
                     f"▷ Deleted/Inaccessible: {deleted}\n"
                     f"▷ Errors (Fetch/Save): {errors}\n\n"
                     f"▷ Elapsed: {get_readable_time(elapsed)}\n"
                     f"▷ ETA: {get_readable_time(eta)}"
                 )
                index_status_cache = {"text": status_text_for_cache, "last_update": time.time()}

                # --- Edit message less frequently ---
                current_time = time.time()
                if current_time - last_edit_time > EDIT_INTERVAL or batch_num == batches - 1: # Update on last batch too
                    edit_text = (f"⏳ Indexing channel `{chat}`...\n"
                                f"{progress_bar_str} {percentage:.1f}%\n"
                                f"~ Processed {progress}/{total_fetch} messages after skipping.")
                    try:
                        await msg.edit_text(text=edit_text, reply_markup=InlineKeyboardMarkup([
                            # Use simple 'index_status' for the callback data
                            [InlineKeyboardButton('📊 Status', callback_data=status_callback_data)],
                            [InlineKeyboardButton('❌ Cancel', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
                        last_edit_time = current_time
                    except FloodWait as e: await asyncio.sleep(e.value); last_edit_time = time.time() # Reset timer after wait
                    except MessageNotModified: pass
                    except Exception as e_edit: logger.error(f"Error editing progress message: {e_edit}")

            # --- Final Status ---
            elapsed = time.time() - start_time
            final_status_msg = "✅ Indexing Completed!" if not temp.CANCEL else "🛑 Indexing Cancelled!"
            final_text_summary = (f"{final_status_msg}\nChannel: `{chat}`\nTook {get_readable_time(elapsed)}\n\n"
                                  f"▷ Total Processed: {current - skip}\n"
                                  f"▷ Successfully Saved: {total_files}\n"
                                  f"▷ Duplicates Found: {duplicate}\n"
                                  f"▷ Deleted/Inaccessible: {deleted}\n"
                                  f"▷ Skipped (NoMedia/Type/Ext): {no_media + unsupported}\n"
                                  f"▷ Errors (Fetch/Save): {errors}")
            index_status_cache = {"text": final_text_summary, "last_update": time.time()}
            await msg.edit(final_text_summary, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Close', callback_data='close_data')]]))

        except Exception as e:
            logger.exception(f"Fatal indexing error for chat {chat}: {e}") # Log full traceback
            final_error_text = f'❌ Fatal Indexing Error: {e}'
            index_status_cache = {"text": final_error_text, "last_update": time.time()}
            try: await msg.edit(final_error_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Close', callback_data='close_data')]]))
            except: pass # Ignore if msg edit fails on error
        finally:
            temp.CANCEL = False # Always reset cancel flag
            logger.info(f"Indexing process finished or cancelled for chat {chat}.")
            # Lock is automatically released by 'async with'

# --- Status Button Callback ---
# This regex is specific and won't clash with index#yes or index#cancel
@Client.on_callback_query(filters.regex(r"^index_status$"))
async def index_status_alert(bot, query: CallbackQuery):
    global index_status_cache
    last_updated_ago = time.time() - index_status_cache.get("last_update", time.time())
    status_text = index_status_cache.get("text", "Status not ready yet.")
    update_info = f"\n\n(Last updated: {int(last_updated_ago)}s ago)"

    try:
        max_alert_len = 195 # Approx limit for alerts, leave room for update info
        display_text = status_text
        if len(status_text + update_info) > max_alert_len:
             # Truncate the main status text if too long
             display_text = status_text[:max_alert_len - len(update_info) - 4] + "..."

        await query.answer(text=display_text + update_info, show_alert=True, cache_time=2) # Short cache time
    except MessageTooLong: await query.answer("Status message is too long to display in alert.", show_alert=True) # Fallback
    except Exception as e: logger.error(f"Index status alert error: {e}"); await query.answer("Error fetching status.", show_alert=True)

