from hydrogram import Client
from hydrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultCachedDocument, InlineQuery
from database.ia_filterdb import get_search_results
# Removed is_premium import
from utils import get_size, temp, get_verify_status, is_subscribed # Removed is_premium
from info import CACHE_TIME, SUPPORT_LINK, UPDATES_LINK, FILE_CAPTION, IS_VERIFY
import logging # Added logging

logger = logging.getLogger(__name__)
cache_time = CACHE_TIME # Use cache time from info

def is_banned(query: InlineQuery):
    # This function remains
    return query.from_user and query.from_user.id in temp.BANNED_USERS

@Client.on_inline_query()
async def inline_search(bot, query: InlineQuery): # Added type hint
    """Show search results for given inline query"""

    # Fsub Check remains
    is_fsub = await is_subscribed(bot, query)
    if is_fsub:
        await query.answer(results=[],
                           cache_time=0, # No cache for fsub prompt
                           switch_pm_text="⚠️ ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ(s) ғɪʀsᴛ!",
                           switch_pm_parameter="inline_fsub")
        return

    # Verify Check (No premium bypass)
    verify_status = await get_verify_status(query.from_user.id)
    if IS_VERIFY and not verify_status['is_verified']:
        await query.answer(results=[],
                           cache_time=0, # No cache for verify prompt
                           switch_pm_text="🔐 ʏᴏᴜ ɴᴇᴇᴅ ᴛᴏ ᴠᴇʀɪғʏ!",
                           switch_pm_parameter="inline_verify")
        return

    # Banned Check remains
    if is_banned(query):
        await query.answer(results=[],
                           cache_time=0, # No cache for banned prompt
                           switch_pm_text="🚫 ʏᴏᴜ'ʀᴇ ʙᴀɴɴᴇᴅ!",
                           switch_pm_parameter="start") # Parameter doesn't matter much here
        return

    results = []
    string = query.query.strip() # Strip whitespace from query
    if len(string) < 2: # Optional: Add minimum query length
         await query.answer(results=[],
                            cache_time=cache_time,
                            switch_pm_text="➡️ ᴇɴᴛᴇʀ ᴀᴛ ʟᴇᴀsᴛ 2 ᴄʜᴀʀᴀᴄᴛᴇʀs...",
                            switch_pm_parameter="start")
         return

    offset = int(query.offset) if query.offset else 0
    # Add error handling for search results fetch
    try:
        files, next_offset, total = await get_search_results(string, offset=offset)
    except Exception as e:
        logger.error(f"Error getting inline search results for '{string}': {e}", exc_info=True)
        await query.answer(results=[],
                           cache_time=5, # Short cache on error
                           switch_pm_text="❌ ᴇʀʀᴏʀ sᴇᴀʀᴄʜɪɴɢ ғɪʟᴇs.",
                           switch_pm_parameter="start")
        return

    # Process results if found
    if files:
        for file in files:
            # Use .get with default for caption
            caption_text = file.get('caption', '')
            # Ensure FILE_CAPTION is defined and imported
            try:
                 f_caption=FILE_CAPTION.format(
                     file_name=file.get('file_name', 'N/A'),
                     file_size=get_size(file.get('file_size', 0)),
                     caption=caption_text if caption_text else "" # Handle None caption
                 )
            except KeyError as e:
                 logger.warning(f"Missing key in FILE_CAPTION format: {e}. Using default.")
                 f_caption = file.get('file_name', 'N/A')
            except Exception as e:
                 logger.error(f"Error formatting caption: {e}")
                 f_caption = file.get('file_name', 'N/A')

            try:
                results.append(
                    InlineQueryResultCachedDocument(
                        title=file.get('file_name', 'N/A'),
                        document_file_id=file['_id'], # Ensure _id exists
                        caption=f_caption[:1024], # Enforce caption limit
                        description=f"sɪᴢᴇ: {get_size(file.get('file_size', 0))}",
                        reply_markup=get_reply_markup(string) # Pass query string
                    )
                )
            except Exception as e:
                 logger.error(f"Error creating InlineQueryResult for file {file.get('_id', 'N/A')}: {e}")
                 # Skip this result if it causes an error

    # Answer the query
    if results:
        switch_pm_text = f"✅ {total} ʀᴇsᴜʟᴛs ғᴏʀ: {string}" if string else f"✅ {total} ʀᴇsᴜʟᴛs"
        try:
            await query.answer(results=results,
                            # is_personal = True, # Consider removing if causing issues
                            cache_time=cache_time,
                            switch_pm_text=switch_pm_text[:64], # Enforce limit
                            switch_pm_parameter="start", # Parameter can be simple
                            next_offset=str(next_offset) if next_offset else None) # Use None for last page
        except Exception as e:
             logger.error(f"Error answering inline query for '{string}': {e}", exc_info=True)
             # Attempt to answer with error message if possible
             try:
                  await query.answer(results=[], cache_time=5, switch_pm_text="❌ ᴇʀʀᴏʀ ɢᴇɴᴇʀᴀᴛɪɴɢ ʀᴇsᴜʟᴛs.", switch_pm_parameter="start")
             except: pass # Ignore if even error answer fails
    else:
        switch_pm_text = f"🚫 ɴᴏ ʀᴇsᴜʟᴛs ғᴏʀ: {string}" if string else "🚫 ɴᴏ ʀᴇsᴜʟᴛs"
        await query.answer(results=[],
                           # is_personal = True,
                           cache_time=cache_time,
                           switch_pm_text=switch_pm_text[:64], # Enforce limit
                           switch_pm_parameter="start")


def get_reply_markup(s):
    # This function remains the same
    buttons = [[ InlineKeyboardButton('🔎 sᴇᴀʀᴄʜ ᴀɢᴀɪɴ', switch_inline_query_current_chat=s or '') ],
               [ InlineKeyboardButton('✨ ᴜᴘᴅᴀᴛᴇs', url=UPDATES_LINK),
                 InlineKeyboardButton('💬 sᴜᴘᴘᴏʀᴛ', url=SUPPORT_LINK) ]]
    return InlineKeyboardMarkup(buttons)
