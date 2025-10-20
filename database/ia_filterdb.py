import logging
from struct import pack
import re
import base64
import asyncio
from functools import partial
from hydrogram.file_id import FileId
from hydrogram import enums
from pymongo import MongoClient, TEXT
from pymongo.errors import DuplicateKeyError, OperationFailure
from info import (
    FILES_DATABASE_URL, SECOND_FILES_DATABASE_URL, DATABASE_NAME, COLLECTION_NAME,
    USE_CAPTION_FILTER, MAX_BTN, DB_CHANGE_LIMIT
)
from utils import get_size

logger = logging.getLogger(__name__)

# --- Database Connections ---
client = None
db = None
collection = None
second_client = None
second_db = None
second_collection = None

try:
    client = MongoClient(FILES_DATABASE_URL)
    db = client[DATABASE_NAME]
    # ** FIX: Check for db object correctly before getting collection **
    collection = db.get_collection(COLLECTION_NAME) if db is not None else None
    if collection is not None:
        collection.create_index([("file_name", TEXT)], background=True)
    logger.info("Connected to Primary Files DB.")
except Exception as e:
    logger.critical(f"Cannot connect to Primary Files DB: {e}", exc_info=True)
    # exit() # Exiting here is a valid option if primary DB is essential

if SECOND_FILES_DATABASE_URL:
    try:
        second_client = MongoClient(SECOND_FILES_DATABASE_URL)
        second_db = second_client[DATABASE_NAME]
        # ** FIX: Check for second_db object correctly **
        second_collection = second_db.get_collection(COLLECTION_NAME) if second_db is not None else None
        if second_collection is not None:
            second_collection.create_index([("file_name", TEXT)], background=True)
        logger.info("Connected to Secondary Files DB.")
    except Exception as e:
        logger.error(f"Cannot connect to Secondary Files DB: {e}. Secondary DB disabled.")
        second_client, second_db, second_collection = None, None, None
else:
     logger.info("Secondary Files DB URL not provided or disabled.")


def db_count_documents():
     if collection is None: return 0
     try: return collection.count_documents({})
     except Exception as e: logger.error(f"Error counting primary DB: {e}"); return 0

def second_db_count_documents():
     if second_collection is None: return 0
     try: return second_collection.count_documents({})
     except Exception as e: logger.error(f"Error counting secondary DB: {e}"); return 0

async def save_file(media):
    loop = asyncio.get_running_loop()
    if collection is None: logger.error("Primary DB unavailable."); return 'err'

    file_id = unpack_new_file_id(media.file_id)
    if not file_id: return 'err'

    raw_file_name = str(media.file_name) if media.file_name else "UnknownFile"
    file_name = re.sub(r"[@\(\)\[\]]", "", raw_file_name)
    file_name = re.sub(r"(_|\-|\.|\+)+", " ", file_name)
    file_name = re.sub(r'\s+', ' ', file_name).strip()
    caption_text = str(media.caption) if media.caption is not None else ""
    file_caption = re.sub(r"@\w+|(_|\-|\.|\+)|https?://\S+", " ", caption_text)
    file_caption = re.sub(r'\s+', ' ', file_caption).strip()
    document = { '_id': file_id, 'file_name': file_name, 'file_size': media.file_size if media.file_size else 0, 'caption': file_caption }

    try:
        exists_in_primary = await loop.run_in_executor(None, partial(collection.find_one, {'_id': file_id}, {'_id': 1}))
        if exists_in_primary: logger.warning(f'[Dup] Primary: {file_name}'); return 'dup'
        if second_collection is not None:
             exists_in_secondary = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': file_id}, {'_id': 1}))
             if exists_in_secondary: logger.warning(f'[Dup] Secondary: {file_name}'); return 'dup'
    except Exception as check_e: logger.error(f"Dup check error: {check_e}. Proceeding...")

    db_to_use = collection; db_name_log = "primary"
    if second_collection is not None:
        try:
            primary_db_stats = await loop.run_in_executor(None, client.admin.command, 'dbstats')
            primary_db_size = primary_db_stats.get('dataSize', 0)
            db_change_limit_bytes = DB_CHANGE_LIMIT * 1024 * 1024
            if primary_db_size >= db_change_limit_bytes:
                 db_to_use = second_collection; db_name_log = "secondary"
                 logger.info(f"Primary DB ({get_size(primary_db_size)}) >= Limit ({DB_CHANGE_LIMIT}MB). Using {db_name_log} DB.")
        except Exception as e: logger.error(f"Size check error: {e}. Defaulting to primary.")

    try:
        await loop.run_in_executor(None, partial(db_to_use.insert_one, document))
        logger.info(f'Saved [{get_size(document["file_size"])}] to {db_name_log}: {file_name}')
        return 'suc'
    except DuplicateKeyError: logger.warning(f'Already Saved (DupKey {db_name_log}) - {file_name}'); return 'dup'
    except OperationFailure as e:
         logger.error(f"Mongo OpFail on {db_name_log}: {e}")
         if db_name_log == "primary" and second_collection is not None:
             logger.warning("OpFail primary, trying secondary...")
             try:
                 await loop.run_in_executor(None, partial(second_collection.insert_one, document))
                 logger.info(f'Saved to secondary after primary OpFail: {file_name}')
                 return 'suc'
             except DuplicateKeyError: logger.warning(f'Already Saved in secondary (OpFail Dupkey): {file_name}'); return 'dup'
             except Exception as e2: logger.error(f"Failed save to secondary after OpFail: {e2}"); return 'err'
         else: return 'err'
    except Exception as e: logger.error(f"Unexpected save error ({db_name_log}): {e}", exc_info=True); return 'err'

async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    loop = asyncio.get_running_loop()
    query = str(query).strip()
    if not query: return [], '', 0
    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error as e: logger.error(f"Regex compile error: {e}, using plain text: {query}"); regex = query
    filter_query = {'file_name': regex}
    if USE_CAPTION_FILTER: filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}
    results = []; total_results = 0

    async def run_find_and_count(db_collection, query_filter, skip, limit):
        if db_collection is None: return [], 0
        try:
            count = await loop.run_in_executor(None, partial(db_collection.count_documents, query_filter))
            cursor = db_collection.find(query_filter).skip(skip).limit(limit)
            docs = await loop.run_in_executor(None, list, cursor)
            return docs, count
        except Exception as e: logger.error(f"Error querying {db_collection.name}: {e}"); return [], 0

    primary_docs, primary_count = await run_find_and_count(collection, filter_query, offset, max_results)
    results.extend(primary_docs)
    total_results += primary_count
    logger.debug(f"Primary search: Found {len(primary_docs)}, Total {primary_count}")

    remaining_limit = max_results - len(primary_docs)
    if second_collection is not None and remaining_limit > 0:
        secondary_docs, secondary_count = await run_find_and_count(second_collection, filter_query, max(0, offset - primary_count), remaining_limit)
        results.extend(secondary_docs)
        total_results += secondary_count
        logger.debug(f"Secondary search: Found {len(secondary_docs)}, Total {secondary_count}")

    if lang:
        lang = lang.lower()
        lang_files = [file for file in results if lang in file.get('file_name', '').lower() or lang in file.get('caption', '').lower()]
        current_total = len(lang_files)
        files_to_return = lang_files[offset : offset + max_results]
    else:
        files_to_return = results[:max_results]
        current_total = total_results

    next_offset_val = offset + len(files_to_return)
    has_more = next_offset_val < current_total
    next_offset = str(next_offset_val) if has_more else ''

    logger.debug(f"Query '{query}'|Lang '{lang}'|Offset {offset}|Returned {len(files_to_return)}|Total ~{current_total}|Next '{next_offset}'")
    return files_to_return, next_offset, current_total

async def delete_files(query):
    loop = asyncio.get_running_loop(); total_deleted = 0
    query = str(query).strip()
    if not query: return 0
    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error: regex = query
    filter_query = {'file_name': regex}

    async def run_delete(db_collection, query_filter):
        if db_collection is None: return 0
        try: result = await loop.run_in_executor(None, partial(db_collection.delete_many, query_filter)); return result.deleted_count
        except Exception as e: logger.error(f"Error deleting from {db_collection.name}: {e}"); return 0

    deleted1 = await run_delete(collection, filter_query); total_deleted += deleted1
    if second_collection is not None: deleted2 = await run_delete(second_collection, filter_query); total_deleted += deleted2
    logger.info(f"Deleted {total_deleted} files for query: '{query}'")
    return total_deleted

async def get_file_details(query_id):
    loop = asyncio.get_running_loop()
    file_details = None
    if collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one primary {query_id}: {e}")
    if not file_details and second_collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one secondary {query_id}: {e}")
    return [file_details] if file_details else []

def encode_file_id(s: bytes) -> str:
    r = b""; n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0
            r += bytes([i])
    if n: r += b"\x00" + bytes([n])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    try:
        decoded = FileId.decode(new_file_id)
        file_type_int_map = { enums.FileType.PHOTO: 0, enums.FileType.AUDIO: 1, enums.FileType.DOCUMENT: 2, enums.FileType.VIDEO: 3, enums.FileType.STICKER: 4, enums.FileType.VOICE: 5, enums.FileType.ANIMATION: 6, enums.FileType.VIDEO_NOTE: 7 }
        file_type_int = file_type_int_map.get(decoded.file_type, 2)
        packed_v2 = pack("<iiqq", file_type_int, decoded.dc_id, decoded.media_id, decoded.access_hash)
        return encode_file_id(packed_v2)
    except Exception as e:
        logger.error(f"Error unpacking file_id {new_file_id}: {e}", exc_info=True)
        return None
