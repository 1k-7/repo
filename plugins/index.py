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

# Global dictionary to store real-time stats for status updates
index_stats = {
    "status_message": "Initializing...",
    "start_time": 0,
    "total_files": 0,
    "duplicate": 0,
    "errors": 0,
    "deleted": 0,
    "no_media": 0,
    "unsupported": 0,
    "current": 0,
    "total_to_process": 0,
    "last_msg_id": 0,
    "chat_id": None,
    "skip": 0,
    "last_update_time": 0
}

# Specific regex to only catch index#yes or index#cancel
@Client.on_callback_query(filters.regex(r'^index#(yes|cancel)'))
async def index_files_callback(bot, query: CallbackQuery):
    global index_stats
    user_id = query.from_user.id
    if user_id not in ADMINS:
        return await query.answer("ᴏɴʟʏ ᴀᴅᴍɪɴs ᴄᴀɴ ᴍᴀɴᴀɢᴇ ɪɴᴅᴇxɪɴɢ.", show_alert=True)

    try:
        parts = query.data.split("#")
        if len(parts) != 5: raise ValueError("Incorrect callback data format for index control")
        _, ident, chat, lst_msg_id_str, skip_str = parts
        lst_msg_id = int(lst_msg_id_str)
        skip = int(skip_str)
        try: chat_id_int = int(chat)
        except ValueError: chat_id_int = chat # Keep as string for username
    except (ValueError, IndexError) as e:
        logger.error(f"Error splitting index control callback data '{query.data}': {e}")
        return await query.answer("Invalid index control callback data.", show_alert=True)

    if ident == 'yes':
        if lock.locked():
             return await query.answer("⏳ ᴀɴᴏᴛʜᴇʀ ɪɴᴅᴇxɪɴɢ ɪs ᴀʟʀᴇᴀᴅʏ ɪɴ ᴘʀᴏɢʀᴇss.", show_alert=True)

        # Reset stats before starting
        index_stats = {
            "status_message": "Starting...",
            "start_time": time.time(),
            "total_files": 0, "duplicate": 0, "errors": 0, "deleted": 0,
            "no_media": 0, "unsupported": 0, "current": skip,
            "total_to_process": lst_msg_id - skip, "last_msg_id": lst_msg_id,
            "chat_id": chat_id_int, "skip": skip, "last_update_time": time.time()
        }
        msg = query.message
        await msg.edit(f"⏳ Indexing starting for channel ID: `{chat_id_int}`\n~ Skipping first {skip} messages.")
        asyncio.create_task(index_files_to_db_iter(lst_msg_id, chat_id_int, msg, bot, skip))
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
            match = re.match(r"https?://t\.me/(?:c/)?([\w\-]+)/(\d+)", msg_link) # Allow hyphens in usernames/private links
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

    buttons = [[ InlineKeyboardButton('✅ ʏᴇs, Start Indexing', callback_data=f'index#yes#{chat_id}#{last_msg_id}#{skip}') ],
               [ InlineKeyboardButton('❌ ɴᴏ, Cancel', callback_data='close_data') ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    await skip_response.reply(f'❓ Do you want to index messages from channel `{chat.title}`?\n\n • Total Messages: ~<code>{last_msg_id}</code>\n • Skipping First: <code>{skip}</code> messages', reply_markup=reply_markup)


def get_progress_bar(percent, length=10):
    filled = int(length * percent / 100)
    unfilled = length - filled
    return '█' * filled + '▒' * unfilled

# Function using iter_messages and concurrent saving
async def index_files_to_db_iter(lst_msg_id, chat, msg, bot, skip):
    global index_stats
    SAVE_BATCH_SIZE = 100 # How many save tasks to gather at once
    EDIT_INTERVAL = 15 # Update status message every 15 seconds
    status_callback_data = 'index_status'

    if lock.locked():
         logger.warning("index_files_to_db_iter called while lock was already held.")
         try: await msg.edit("⚠️ Indexing lock is already held.")
         except: pass
         return

    async with lock:
        try:
            temp.CANCEL = False
            save_tasks = []
            last_processed_msg_id = skip # Track the ID of the last message processed
            index_stats["start_time"] = time.time() # Reset start time
            index_stats["last_update_time"] = time.time()

            # Initial message edit
            try:
                await msg.edit(
                    f"⏳ Indexing starting for channel ID: `{chat}`\n~ Skipping first {skip} messages.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('📊 Status', callback_data=status_callback_data)],
                        [InlineKeyboardButton('❌ Cancel', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
            except Exception as initial_edit_e:
                 logger.error(f"Error editing initial index msg: {initial_edit_e}")


            # Iterate through messages using bot.iter_messages
            # Start iterating from the message *after* the skip point
            async for message in bot.iter_messages(chat, limit=lst_msg_id + 1, offset=skip):
                if temp.CANCEL:
                    logger.warning("Cancellation requested, breaking index loop.")
                    break

                last_processed_msg_id = message.id # Update the last processed ID

                # Update current count in stats
                index_stats["current"] = last_processed_msg_id

                # Basic message checks (same as batch version)
                if message.empty: index_stats["deleted"] += 1; continue
                if not message.media: index_stats["no_media"] += 1; continue
                if message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.AUDIO, enums.MessageMediaType.DOCUMENT]: index_stats["unsupported"] += 1; continue
                media = getattr(message, message.media.value, None)
                if not media or not hasattr(media, 'file_name') or not media.file_name: index_stats["unsupported"] += 1; continue
                file_name_lower = media.file_name.lower()
                if not any(file_name_lower.endswith("." + ext.lstrip('.')) for ext in INDEX_EXTENSIONS): index_stats["unsupported"] += 1; continue

                media.caption = message.caption
                save_tasks.append(save_file(media)) # Add save task

                # Process save tasks in batches using asyncio.gather
                if len(save_tasks) >= SAVE_BATCH_SIZE:
                    results = await asyncio.gather(*save_tasks, return_exceptions=True)
                    for result in results:
                         if isinstance(result, Exception): index_stats["errors"] += 1; logger.error(f"Save error during gather: {result}")
                         elif result == 'suc': index_stats["total_files"] += 1
                         elif result == 'dup': index_stats["duplicate"] += 1
                         elif result == 'err': index_stats["errors"] += 1
                    save_tasks = [] # Clear the batch

                # Update status message periodically based on time
                current_time = time.time()
                if current_time - index_stats["last_update_time"] > EDIT_INTERVAL:
                    progress = last_processed_msg_id - skip
                    percentage = min((progress / index_stats["total_to_process"]) * 100, 100.0) if index_stats["total_to_process"] > 0 else 100.0
                    progress_bar_str = get_progress_bar(int(percentage))
                    elapsed = current_time - index_stats["start_time"]
                    # Simplified ETA - less accurate with iter_messages but gives an idea
                    processed_per_sec = progress / elapsed if elapsed > 0 else 0
                    remaining = index_stats["total_to_process"] - progress
                    eta = (remaining / processed_per_sec) if processed_per_sec > 0 else 0

                    index_stats["status_message"] = (
                         f"⏳ Indexing `{chat}`...\n"
                         f"{progress_bar_str} {percentage:.1f}%\n"
                         f"~ Msg ID: {last_processed_msg_id}/{lst_msg_id}\n"
                         f"~ Saved: {index_stats['total_files']} | Dup: {index_stats['duplicate']}\n"
                         f"~ Skip: {index_stats['no_media'] + index_stats['unsupported']} | Err: {index_stats['errors']}\n"
                         f"~ Elap: {get_readable_time(elapsed)} | ETA: {get_readable_time(eta)}"
                    )

                    try:
                        await msg.edit_text(
                            text=index_stats["status_message"],
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton('📊 Status', callback_data=status_callback_data)],
                                [InlineKeyboardButton('❌ Cancel', callback_data=f'index#cancel#{chat}#{lst_msg_id}#{skip}')]]))
                        index_stats["last_update_time"] = current_time
                    except FloodWait as e: await asyncio.sleep(e.value); index_stats["last_update_time"] = time.time()
                    except MessageNotModified: pass
                    except Exception as e_edit: logger.error(f"Error editing progress message (iter): {e_edit}")

            # Process any remaining save tasks after the loop finishes
            if save_tasks:
                results = await asyncio.gather(*save_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception): index_stats["errors"] += 1; logger.error(f"Save error during final gather: {result}")
                    elif result == 'suc': index_stats["total_files"] += 1
                    elif result == 'dup': index_stats["duplicate"] += 1
                    elif result == 'err': index_stats["errors"] += 1

            # --- Final Status ---
            elapsed = time.time() - index_stats["start_time"]
            final_status_msg = "✅ Indexing Completed!" if not temp.CANCEL else "🛑 Indexing Cancelled!"
            # Use final stats from the index_stats dictionary
            final_text_summary = (
                f"{final_status_msg}\nChannel: `{chat}`\nTook {get_readable_time(elapsed)}\n\n"
                f"▷ Last Processed ID: {last_processed_msg_id}\n"
                f"▷ Total Processed (after skip): {last_processed_msg_id - skip}\n"
                f"▷ Successfully Saved: {index_stats['total_files']}\n"
                f"▷ Duplicates Found: {index_stats['duplicate']}\n"
                f"▷ Deleted/Inaccessible (estimate): {index_stats['deleted']}\n"
                f"▷ Skipped (NoMedia/Type/Ext): {index_stats['no_media'] + index_stats['unsupported']}\n"
                f"▷ Errors (Save): {index_stats['errors']}"
            )
            # Update final status message in the global dict too
            index_stats["status_message"] = final_text_summary
            await msg.edit(final_text_summary, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Close', callback_data='close_data')]]))

        except Exception as e:
            logger.exception(f"Fatal indexing error (iter) for chat {chat}: {e}")
            final_error_text = f'❌ Fatal Indexing Error: {e}'
            index_stats["status_message"] = final_error_text
            try: await msg.edit(final_error_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Close', callback_data='close_data')]]))
            except: pass
        finally:
            temp.CANCEL = False
            logger.info(f"Indexing process (iter) finished or cancelled for chat {chat}.")

# Status Button Callback - Reads from global index_stats
@Client.on_callback_query(filters.regex(r"^index_status$"))
async def index_status_alert(bot, query: CallbackQuery):
    global index_stats

    # Construct the status text directly from the latest stats if indexing is active
    if lock.locked():
        elapsed = time.time() - index_stats.get("start_time", time.time())
        progress = index_stats.get("current", 0) - index_stats.get("skip", 0)
        total_to_process = index_stats.get("total_to_process", 1) # Avoid division by zero
        percentage = min((progress / total_to_process) * 100, 100.0) if total_to_process > 0 else 0.0
        progress_bar_str = get_progress_bar(int(percentage))

        processed_per_sec = progress / elapsed if elapsed > 1 else 0 # Avoid division by zero early on
        remaining = total_to_process - progress
        eta = (remaining / processed_per_sec) if processed_per_sec > 0 and remaining > 0 else 0

        status_text = (
             f"📊 Indexing Status (`{index_stats.get('chat_id', 'N/A')}`)\n"
             f"▷ {progress_bar_str} {percentage:.1f}%\n"
             f"▷ Msg ID: {index_stats.get('current', 0)}/{index_stats.get('last_msg_id', 0)}\n"
             f"▷ Saved: {index_stats.get('total_files', 0)} | Dup: {index_stats.get('duplicate', 0)}\n"
             f"▷ Skip: {index_stats.get('no_media', 0) + index_stats.get('unsupported', 0)} | Err: {index_stats.get('errors', 0)}\n"
             f"▷ Elap: {get_readable_time(elapsed)}\n"
             f"▷ ETA: {get_readable_time(eta)}"
         )
    else:
        # Use the last saved status message if indexing is not running
        status_text = index_stats.get("status_message", "No active indexing process.")


    try:
        # Display directly without "last updated"
        await query.answer(text=status_text[:199], show_alert=True, cache_time=2) # Truncate if needed for alert limit
    except MessageTooLong: await query.answer("Status too long for alert.", show_alert=True) # Fallback
    except Exception as e: logger.error(f"Index status alert error: {e}"); await query.answer("Error fetching status.", show_alert=True)
