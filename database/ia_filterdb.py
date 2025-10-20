import logging
from struct import pack
import re
import base64
import asyncio
from functools import partial
from hydrogram.file_id import FileId
from hydrogram import enums # Import enums for FileType check
from pymongo import MongoClient, TEXT
from pymongo.errors import DuplicateKeyError, OperationFailure
# Import DB clients/collections defined in users_chats_db
from database.users_chats_db import (
    files_db_client as client, files_db as db, files_db as collection, # Use primary by default
    second_files_db_client, second_files_db, second_files_db as second_collection # Use secondary if available
)
from info import USE_CAPTION_FILTER, SECOND_FILES_DATABASE_URL, MAX_BTN, DB_CHANGE_LIMIT
from utils import get_size

logger = logging.getLogger(__name__)

# Note: Indexes should have been created on startup by users_chats_db.py

# --- DB Count Functions (Synchronous) ---
def db_count_documents():
     if collection is None: return 0
     try: return collection.count_documents({})
     except Exception as e: logger.error(f"Error counting primary DB: {e}"); return 0

def second_db_count_documents():
     if second_collection is None: return 0
     try: return second_collection.count_documents({})
     except Exception as e: logger.error(f"Error counting secondary DB: {e}"); return 0

# --- Async Save File ---
async def save_file(media):
    """Save file in database with size check and cross-DB duplicate check"""
    loop = asyncio.get_event_loop()
    if collection is None: logger.error("Primary DB unavailable."); return 'err'

    file_id = unpack_new_file_id(media.file_id)
    if not file_id: return 'err' # unpack failed

    raw_file_name = str(media.file_name) if media.file_name else "UnknownFile"
    file_name = re.sub(r"@\w+|(_|\-|\.|\+)", " ", raw_file_name)
    file_name = re.sub(r'\s+', ' ', file_name).strip()

    caption_text = str(media.caption) if media.caption is not None else ""
    # Further sanitize caption if needed (e.g., remove links)
    file_caption = re.sub(r"@\w+|(_|\-|\.|\+)|https?://\S+", " ", caption_text) # Example: remove links too
    file_caption = re.sub(r'\s+', ' ', file_caption).strip()

    document = {
        '_id': file_id, 'file_name': file_name,
        'file_size': media.file_size if media.file_size else 0, 'caption': file_caption
    }

    # --- Cross-DB Duplicate Check ---
    try:
        exists_in_primary = await loop.run_in_executor(None, partial(collection.find_one, {'_id': file_id}, {'_id': 1})) # Only fetch ID
        if exists_in_primary: logger.warning(f'[Dup Check] Primary: {file_name}'); return 'dup'
        if second_collection:
             exists_in_secondary = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': file_id}, {'_id': 1}))
             if exists_in_secondary: logger.warning(f'[Dup Check] Secondary: {file_name}'); return 'dup'
    except Exception as check_e: logger.error(f"Dup check error: {check_e}. Proceeding...")

    # --- Select DB based on size ---
    use_second_db = False; db_to_use = collection; db_name_log = "primary"
    if second_collection:
        try:
            primary_db_stats = await loop.run_in_executor(None, client.admin.command, 'dbstats')
            primary_db_size = primary_db_stats.get('dataSize', 0)
            db_change_limit_bytes = DB_CHANGE_LIMIT * 1024 * 1024
            if primary_db_size >= db_change_limit_bytes:
                 use_second_db = True; db_to_use = second_collection; db_name_log = "secondary"
                 logger.info(f"Primary DB size {get_size(primary_db_size)} >= {DB_CHANGE_LIMIT}MB. Using {db_name_log} DB.")
        except Exception as e: logger.error(f"Size check error: {e}. Defaulting to primary.")

    # --- Attempt insert ---
    try:
        await loop.run_in_executor(None, partial(db_to_use.insert_one, document))
        logger.info(f'Saved to {db_name_log} db - {file_name}')
        return 'suc'
    except DuplicateKeyError: logger.warning(f'Already Saved (DupKey on {db_name_log}) - {file_name}'); return 'dup'
    except OperationFailure as e:
         logger.error(f"Mongo OpFail on {db_name_log}: {e}")
         if db_name_log == "primary" and second_collection:
             logger.warning("OpFail on primary, trying secondary...")
             try: # Existence already checked, try insert
                 await loop.run_in_executor(None, partial(second_collection.insert_one, document))
                 logger.info(f'Saved to secondary after primary failure - {file_name}')
                 return 'suc'
             except DuplicateKeyError: logger.warning(f'Already Saved in secondary (OpFail Dupkey): {file_name}'); return 'dup'
             except Exception as e2: logger.error(f"Failed save to secondary after OpFail: {e2}"); return 'err'
         else: return 'err' # No fallback or secondary failed too
    except Exception as e: logger.error(f"Unexpected save error ({db_name_log}): {e}", exc_info=True); return 'err'


# --- Async Get Search Results ---
async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    loop = asyncio.get_event_loop()
    query = str(query).strip()
    if not query: return [], '', 0 # Return empty if query is empty

    # --- Compile Regex ---
    if ' ' not in query: raw_pattern = r'(\b|[\.\+\-_])' + re.escape(query) + r'(\b|[\.\+\-_])'
    else: raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]') # Basic space handling
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error as e: logger.error(f"Regex error: {e}, using plain text for: {query}"); regex = query

    # --- Define Filter ---
    # Using regex for broader matching, consider text index for performance on large DBs
    filter_query = {'file_name': regex}
    if USE_CAPTION_FILTER: filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}

    # --- Query Execution ---
    results = []
    total_results = 0

    async def run_find_and_count(db_collection, query_filter):
         if db_collection is None: return [], 0
         try:
              # Count first for total
              count = await loop.run_in_executor(None, partial(db_collection.count_documents, query_filter))
              # Find documents with skip and limit
              cursor = db_collection.find(query_filter).skip(offset).limit(max_results) # Apply offset/limit in DB query
              docs = await loop.run_in_executor(None, list, cursor)
              return docs, count
         except Exception as e: logger.error(f"Error querying {db_collection.name}: {e}"); return [], 0

    # Query primary DB
    primary_docs, primary_count = await run_find_and_count(collection, filter_query)
    results.extend(primary_docs)
    total_results += primary_count

    # Query secondary DB if exists
    if second_collection:
        # Adjust offset/limit for secondary based on primary results?
        # For simplicity now, we fetch max_results from both and combine/slice later
        # A more efficient approach would adjust the secondary query based on primary results count.
        secondary_docs, secondary_count = await run_find_and_count(second_collection, filter_query)
        results.extend(secondary_docs)
        total_results += secondary_count # Note: This simple addition might overestimate if counts overlap heavily with regex; fine for estimations

    # --- Filter by Language (if applicable, Post-Fetch) ---
    if lang:
        lang = lang.lower()
        lang_files = [file for file in results if lang in file.get('file_name', '').lower()]
        files_to_return = lang_files[:max_results] # Slice after combining and filtering
        current_total = len(lang_files) # Total relevant to language filter
    else:
        # Slice combined results if no language filter
        # Note: Results aren't explicitly sorted across DBs here.
        files_to_return = results[:max_results]
        current_total = total_results # Use estimated total

    # --- Calculate Next Offset ---
    # This pagination isn't perfect across two unsorted result sets, but provides basic functionality.
    # True pagination needs DB-level sorting and skipping across both sources or a unified view.
    next_offset = offset + len(files_to_return) # Simplified next offset based on returned count
    if next_offset >= current_total or len(files_to_return) < max_results: # Check if we reached the end
        next_offset = ''

    logger.debug(f"Query '{query}'|Lang '{lang}'|Offset {offset}|Limit {max_results}|Found {len(files_to_return)}|Total ~{current_total}|Next '{next_offset}'")
    return files_to_return, next_offset, current_total


# --- Async Delete Files ---
async def delete_files(query):
    loop = asyncio.get_event_loop()
    query = str(query).strip(); total_deleted = 0
    if not query: return 0

    if ' ' not in query: raw_pattern = r'(\b|[\.\+\-_])' + re.escape(query) + r'(\b|[\.\+\-_])'
    else: raw_pattern = query.replace(' ', r'.*[\s\.\+\-_]')
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error: regex = query # Fallback

    filter_query = {'file_name': regex} if isinstance(regex, re.Pattern) else {'$text': {'$search': query}}

    async def run_delete(db_collection, query_filter):
        if db_collection is None: return 0
        try: result = await loop.run_in_executor(None, partial(db_collection.delete_many, query_filter)); return result.deleted_count
        except Exception as e: logger.error(f"Error deleting from {db_collection.name}: {e}"); return 0

    deleted1 = await run_delete(collection, filter_query); total_deleted += deleted1
    if second_collection: deleted2 = await run_delete(second_collection, filter_query); total_deleted += deleted2

    logger.info(f"Deleted {total_deleted} files matching query: '{query}'")
    return total_deleted


# --- Async Get File Details ---
async def get_file_details(query_id):
    loop = asyncio.get_event_loop()
    file_details = None
    if collection:
         try: file_details = await loop.run_in_executor(None, partial(collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one primary {query_id}: {e}")
    if not file_details and second_collection:
         try: file_details = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one secondary {query_id}: {e}")
    return file_details


# --- File ID Packing/Unpacking ---
def encode_file_id(s: bytes) -> str:
    # Encodes v2 packed bytes to string ID
    r = b""; n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0
            r += bytes([i])
    if n: r += b"\x00" + bytes([n])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """Unpacks v2/v3 file_id, returns packed v2 file_id string for db"""
    try:
        decoded = FileId.decode(new_file_id)
        # Map Hydrogram FileType enum to integer representation expected by pack
        # These integers might correspond to older TG layer types, adjust if necessary
        file_type_int = 0 # Default (Photo)
        if decoded.file_type == enums.FileType.DOCUMENT: file_type_int = 2
        elif decoded.file_type == enums.FileType.VIDEO: file_type_int = 3
        elif decoded.file_type == enums.FileType.AUDIO: file_type_int = 1
        # Add mappings for other types like VOICE, STICKER, ANIMATION if needed

        packed_v2 = pack( "<iiqq", file_type_int, decoded.dc_id, decoded.media_id, decoded.access_hash )
        return encode_file_id(packed_v2)
    except Exception as e:
        logger.error(f"Error unpacking file_id {new_file_id}: {e}", exc_info=True)
        return None # Return None on failure
