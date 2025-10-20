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
# --- CORRECTED IMPORTS ---
from database.users_chats_db import files_db_client as client, files_db as db # Keep client and db objects
from database.users_chats_db import second_files_db_client, second_files_db # Keep secondary client/db
from info import (COLLECTION_NAME, USE_CAPTION_FILTER, SECOND_FILES_DATABASE_URL,
                  MAX_BTN, DB_CHANGE_LIMIT) # Import necessary info vars
# --- CORRECTLY GET COLLECTIONS ---
collection = db.get_collection(COLLECTION_NAME) if db else None
second_collection = second_files_db.get_collection(COLLECTION_NAME) if second_files_db else None
# --- END CORRECTIONS ---
from utils import get_size

logger = logging.getLogger(__name__)

# Ensure indexes (Optional - better managed externally)
# try:
#     if collection: collection.create_index([("file_name", TEXT), ("caption", TEXT)], background=True, name="search_index") # Add caption
#     if second_collection: second_collection.create_index([("file_name", TEXT), ("caption", TEXT)], background=True, name="search_index_secondary")
#     logger.info("Ensured text indexes on file_name and caption.")
# except Exception as e:
#     logger.warning(f"Could not ensure text indexes: {e}")


# --- DB Count Functions (Synchronous) ---
def db_count_documents():
     # ** FIX: Check collection is not None **
     if collection is None: logger.error("Primary collection not available for count."); return 0
     try: return collection.count_documents({})
     except Exception as e: logger.error(f"Error counting primary DB: {e}"); return 0

def second_db_count_documents():
     # ** FIX: Check second_collection is not None **
     if second_collection is None: return 0 # Return 0 if no secondary collection
     try: return second_collection.count_documents({})
     except Exception as e: logger.error(f"Error counting secondary DB: {e}"); return 0


# --- Async Save File with Cross-DB Check ---
async def save_file(media):
    loop = asyncio.get_running_loop()
    # ** FIX: Check collection **
    if collection is None: logger.error("Primary DB unavailable."); return 'err'

    file_id = unpack_new_file_id(media.file_id)
    if not file_id: return 'err'

    raw_fn = str(media.file_name) if media.file_name else "UnknownFile"; fn = re.sub(r"[@\(\)\[\]]", "", raw_fn); fn = re.sub(r"(_|\-|\.|\+)+", " ", fn); fn = re.sub(r'\s+', ' ', fn).strip()
    cap_txt = str(media.caption) if media.caption is not None else ""; fc = re.sub(r"@\w+|(_|\-|\.|\+)|https?://\S+", " ", cap_txt); fc = re.sub(r'\s+', ' ', fc).strip()
    doc = { '_id': file_id, 'file_name': fn, 'file_size': media.file_size or 0, 'caption': fc }

    try: # Cross-DB Check
        ex_p = await loop.run_in_executor(None, partial(collection.find_one, {'_id': file_id}, {'_id': 1}))
        if ex_p: logger.warning(f'[Dup] Primary: {fn}'); return 'dup'
        # ** FIX: Check second_collection is not None **
        if second_collection is not None:
             ex_s = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': file_id}, {'_id': 1}))
             if ex_s: logger.warning(f'[Dup] Secondary: {fn}'); return 'dup'
    except Exception as e_chk: logger.error(f"Dup check error: {e_chk}. Proceeding...")

    use_s_db = False; db_use = collection; log_db = "primary"
    # ** FIX: Check second_collection is not None **
    if second_collection is not None:
        try: # Size Check
            p_stats = await loop.run_in_executor(None, client.admin.command, 'dbstats')
            p_size = p_stats.get('dataSize', 0); limit_b = DB_CHANGE_LIMIT * 1024 * 1024
            if p_size >= limit_b: use_s_db = True; db_use = second_collection; log_db = "secondary"; logger.info(f"Primary DB ({get_size(p_size)}) >= Limit. Using {log_db}.")
        except Exception as e_size: logger.error(f"Size check error: {e_size}. Defaulting primary.")

    try: # Insert
        await loop.run_in_executor(None, partial(db_use.insert_one, doc))
        logger.info(f'Saved [{get_size(doc["file_size"])}] to {log_db}: {fn}')
        return 'suc'
    except DuplicateKeyError: logger.warning(f'DupKey {log_db}: {fn}'); return 'dup'
    except OperationFailure as e_op:
         logger.error(f"Mongo OpFail {log_db}: {e_op}")
         # ** FIX: Check second_collection is not None **
         if log_db == "primary" and second_collection is not None:
             logger.warning("OpFail primary, trying secondary...")
             try: await loop.run_in_executor(None, partial(second_collection.insert_one, doc)); logger.info(f'Saved secondary after OpFail: {fn}'); return 'suc'
             except DuplicateKeyError: logger.warning(f'Already secondary (OpFail Dupkey): {fn}'); return 'dup'
             except Exception as e2: logger.error(f"Failed save secondary after OpFail: {e2}"); return 'err'
         else: return 'err'
    except Exception as e_ins: logger.error(f"Save error ({log_db}): {e_ins}", exc_info=True); return 'err'


# --- Async Get Search Results ---
async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    loop = asyncio.get_running_loop()
    query = str(query).strip()
    if not query: return [], '', 0

    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*' # Match words in order
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error as e: logger.error(f"Regex error: {e}, using plain text: {query}"); regex = query

    filter_query = {'file_name': regex}
    if USE_CAPTION_FILTER: filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    # Text search fallback (requires index):
    # if not isinstance(regex, re.Pattern): filter_query = {'$text': {'$search': query}}

    results = []; total_results = 0

    async def run_find_and_count(db_collection, query_filter, skip, limit):
        # ** FIX: Check db_collection is not None **
        if db_collection is None: return [], 0
        try:
            count = await loop.run_in_executor(None, partial(db_collection.count_documents, query_filter))
            cursor = db_collection.find(query_filter).skip(skip).limit(limit) # Add sort if needed
            docs = await loop.run_in_executor(None, list, cursor)
            return docs, count
        except Exception as e: logger.error(f"Error querying {db_collection.name}: {e}"); return [], 0

    # Query primary
    primary_docs, primary_count = await run_find_and_count(collection, filter_query, offset, max_results)
    results.extend(primary_docs); total_results += primary_count
    logger.debug(f"Primary search: Found {len(primary_docs)}, Total {primary_count}")

    # Query secondary if needed
    remaining_limit = max_results - len(primary_docs)
    # ** FIX: Check second_collection is not None **
    if second_collection is not None and remaining_limit > 0:
        # Simple pagination offset adjustment across DBs (might not be perfect)
        secondary_offset = max(0, offset - primary_count) if offset >= primary_count else 0
        secondary_docs, secondary_count = await run_find_and_count(second_collection, filter_query, secondary_offset, remaining_limit)
        results.extend(secondary_docs); total_results += secondary_count
        logger.debug(f"Secondary search: Found {len(secondary_docs)}, Total {secondary_count}")

    # Post-Fetch Language Filter
    if lang:
        lang = lang.lower()
        lang_files = [f for f in results if lang in f.get('file_name', '').lower() or lang in f.get('caption', '').lower()]
        current_total = len(lang_files)
        # Re-apply offset/limit after filtering (inefficient but simpler)
        files_to_return = lang_files[offset : offset + max_results]
    else:
        # Slice combined results to max_results (already partially limited by DB queries)
        files_to_return = results[:max_results]
        current_total = total_results # Estimated total

    # Calculate Next Offset
    next_offset_val = offset + len(files_to_return)
    has_more = next_offset_val < current_total
    next_offset = str(next_offset_val) if has_more else ''

    logger.debug(f"Q:'{query}'|L:'{lang}'|Off:{offset}|Lim:{max_results}|Ret:{len(files_to_return)}|Tot:~{current_total}|Next:'{next_offset}'")
    return files_to_return, next_offset, current_total


# --- Async Delete Files ---
async def delete_files(query):
    loop = asyncio.get_running_loop(); total_deleted = 0
    query = str(query).strip();
    if not query: return 0

    words = [re.escape(word) for word in query.split()]; raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error: regex = query
    filter_query = {'file_name': regex} # Match filename only for delete
    # filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]} # Optionally include caption

    async def run_delete(db_collection, query_filter):
        # ** FIX: Check db_collection **
        if db_collection is None: return 0
        try: result = await loop.run_in_executor(None, partial(db_collection.delete_many, query_filter)); return result.deleted_count
        except Exception as e: logger.error(f"Error deleting {db_collection.name}: {e}"); return 0

    deleted1 = await run_delete(collection, filter_query); total_deleted += deleted1
    # ** FIX: Check second_collection **
    if second_collection is not None: deleted2 = await run_delete(second_collection, filter_query); total_deleted += deleted2

    logger.info(f"Deleted {total_deleted} files matching: '{query}'")
    return total_deleted


# --- Async Get File Details ---
async def get_file_details(query_id):
    loop = asyncio.get_running_loop()
    file_details = None
    # ** FIX: Check collection **
    if collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one primary {query_id}: {e}")
    # ** FIX: Check second_collection **
    if not file_details and second_collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one secondary {query_id}: {e}")
    # Return list to match previous expectation
    return [file_details] if file_details else []


# --- File ID Packing/Unpacking ---
def encode_file_id(s: bytes) -> str:
    r=b""; n=0
    for i in s+bytes([22])+bytes([4]):
        if i==0: n+=1
        else:
            if n: r+=b"\x00"+bytes([n]); n=0
            r+=bytes([i])
    if n: r+=b"\x00"+bytes([n])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    try:
        decoded = FileId.decode(new_file_id)
        type_map = { enums.FileType.PHOTO: 0, enums.FileType.AUDIO: 1, enums.FileType.DOCUMENT: 2, enums.FileType.VIDEO: 3, enums.FileType.STICKER: 4, enums.FileType.VOICE: 5, enums.FileType.ANIMATION: 6, enums.FileType.VIDEO_NOTE: 7 }
        ftype = type_map.get(decoded.file_type, 2) # Default Document
        packed = pack("<iiqq", ftype, decoded.dc_id, decoded.media_id, decoded.access_hash)
        return encode_file_id(packed)
    except Exception as e: logger.error(f"Unpack file_id error {new_file_id}: {e}", exc_info=True); return None

