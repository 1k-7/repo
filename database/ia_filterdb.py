import logging
from struct import pack
import re
import base64
import asyncio
from functools import partial
from hydrogram.file_id import FileId
# Corrected import: Use MessageMediaType instead of FileType
from hydrogram import enums
from pymongo import MongoClient, TEXT
from pymongo.errors import DuplicateKeyError, OperationFailure
from info import (
    FILES_DATABASE_URL, SECOND_FILES_DATABASE_URL, DATABASE_NAME, COLLECTION_NAME,
    USE_CAPTION_FILTER, MAX_BTN, DB_CHANGE_LIMIT
)
from utils import get_size

logger = logging.getLogger(__name__)

client = None
db = None
collection = None
second_client = None
second_db = None
second_collection = None

try:
    client = MongoClient(FILES_DATABASE_URL)
    db = client[DATABASE_NAME]
    collection = db.get_collection(COLLECTION_NAME) if db is not None else None
    if collection is not None:
        try:
            # Create a text index on file_name for searching
            collection.create_index([("file_name", TEXT)], background=True)
        except OperationFailure as e:
            # Log critical error if index creation fails (often due to DB being full)
            logger.critical(f"Primary DB Full or Error! Couldn't create index: {e}")
    logger.info("Connected to Primary Files DB.")
except Exception as e:
    logger.critical(f"Cannot connect to Primary Files DB: {e}", exc_info=True)
    # Depending on requirements, you might want to exit() here if primary DB is essential

if SECOND_FILES_DATABASE_URL:
    try:
        second_client = MongoClient(SECOND_FILES_DATABASE_URL)
        second_db = second_client[DATABASE_NAME]
        second_collection = second_db.get_collection(COLLECTION_NAME) if second_db is not None else None
        if second_collection is not None:
            try:
                # Create text index on secondary DB as well
                second_collection.create_index([("file_name", TEXT)], background=True)
            except OperationFailure as e:
                logger.warning(f"Secondary DB may be full or error! Couldn't create index: {e}")
        logger.info("Connected to Secondary Files DB.")
    except Exception as e:
        logger.error(f"Cannot connect to Secondary Files DB: {e}. Secondary DB will be disabled.")
        second_client, second_db, second_collection = None, None, None # Disable secondary DB
else:
     logger.info("Secondary Files DB URL not provided or disabled.")

def db_count_documents():
     """Counts documents in the primary collection."""
     if collection is None: return 0
     try: return collection.count_documents({})
     except Exception as e: logger.error(f"Error counting primary DB: {e}"); return 0

def second_db_count_documents():
     """Counts documents in the secondary collection."""
     if second_collection is None: return 0
     try: return second_collection.count_documents({})
     except Exception as e: logger.error(f"Error counting secondary DB: {e}"); return 0

async def save_file(media):
    """Saves file metadata to the database."""
    loop = asyncio.get_running_loop()
    if collection is None: logger.error("Primary DB unavailable, cannot save file."); return 'err'

    # Unpack file_id using the corrected function
    file_id = unpack_new_file_id(media.file_id)
    if not file_id: return 'err' # Error during unpacking

    # Clean file name
    raw_file_name = str(media.file_name) if media.file_name else "UnknownFile"
    # Remove potentially problematic characters for searching/indexing
    file_name = re.sub(r"[@\(\)\[\]]", "", raw_file_name)
    file_name = re.sub(r"(_|\-|\.|\+)+", " ", file_name).strip() # Replace separators with space
    file_name = re.sub(r'\s+', ' ', file_name) # Condense multiple spaces

    # Clean caption
    caption_text = str(media.caption) if media.caption is not None else ""
    # Remove mentions, links, and separators from caption for cleaner search data
    file_caption = re.sub(r"@\w+|(_|\-|\.|\+)|https?://\S+", " ", caption_text).strip()
    file_caption = re.sub(r'\s+', ' ', file_caption) # Condense multiple spaces

    document = {
        '_id': file_id, # Use the unpacked file_id as the unique document ID
        'file_name': file_name,
        'file_size': media.file_size or 0,
        'caption': file_caption
        }

    # Check for duplicates in both databases before inserting
    try:
        # Check primary DB
        is_in_primary = await loop.run_in_executor(None, partial(collection.find_one, {'_id': file_id}, {'_id': 1}))
        if is_in_primary:
            logger.debug(f'[Duplicate] Already in Primary DB: {file_name}'); return 'dup'
        # Check secondary DB if it exists
        if second_collection is not None:
            is_in_secondary = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': file_id}, {'_id': 1}))
            if is_in_secondary:
                logger.debug(f'[Duplicate] Already in Secondary DB: {file_name}'); return 'dup'
    except Exception as e:
        logger.error(f"Duplicate check failed for file ID {file_id}: {e}. Proceeding with insert attempt...")

    # Determine which DB to use based on primary DB size limit
    db_to_use, db_name_log = (collection, "primary") # Default to primary
    if second_collection is not None:
        try:
            # Check primary DB size (run synchronously in executor)
            primary_db_stats = await loop.run_in_executor(None, client.admin.command, 'dbstats') # Use client, not db
            current_size_bytes = primary_db_stats.get('dataSize', 0)
            size_limit_bytes = DB_CHANGE_LIMIT * 1024 * 1024

            if current_size_bytes >= size_limit_bytes:
                 db_to_use, db_name_log = (second_collection, "secondary")
                 logger.info(f"Primary DB size ({get_size(current_size_bytes)}) >= limit ({DB_CHANGE_LIMIT}MB). Using secondary DB.")
        except Exception as e:
            logger.error(f"Failed to check primary DB size: {e}. Defaulting to primary DB.")

    # Attempt to insert the document
    try:
        await loop.run_in_executor(None, partial(db_to_use.insert_one, document))
        logger.info(f'Saved [{get_size(document["file_size"])}] to {db_name_log} DB: {file_name}')
        return 'suc' # Success
    except DuplicateKeyError:
        # This might happen in rare race conditions if duplicate check failed earlier
        logger.warning(f'Duplicate Key error on insert ({db_name_log} DB): {file_name}'); return 'dup'
    except OperationFailure as e:
         # Likely DB is full or other Atlas/Mongo error
         logger.error(f"MongoDB Operation Failure on {db_name_log} DB: {e}")
         # If primary failed and secondary exists, try saving to secondary
         if db_name_log == "primary" and second_collection is not None:
             logger.warning("Operation Failure on primary, attempting to save to secondary DB...")
             try:
                 # Ensure it's not already in secondary (another check in case of race condition)
                 if await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': file_id}, {'_id': 1})):
                     logger.warning(f'[Duplicate] Found in Secondary DB after primary OpFail: {file_name}'); return 'dup'
                 # Try inserting into secondary
                 await loop.run_in_executor(None, partial(second_collection.insert_one, document))
                 logger.info(f'Saved [{get_size(document["file_size"])}] to secondary DB after primary OpFail: {file_name}')
                 return 'suc'
             except DuplicateKeyError:
                 logger.warning(f'Duplicate Key error on secondary DB insert (after primary OpFail): {file_name}'); return 'dup'
             except Exception as e2:
                 logger.error(f"Failed to save to secondary DB after primary OpFail: {e2}"); return 'err'
         else:
             return 'err' # Return error if primary failed and no secondary, or if secondary failed too
    except Exception as e:
        logger.error(f"Unexpected error saving file ({db_name_log} DB): {e}", exc_info=True); return 'err'

async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    """Searches for files in the database(s)."""
    loop = asyncio.get_running_loop()
    query = str(query).strip()
    if not query: return [], '', 0 # Return empty if query is empty

    # Prepare regex pattern for searching words in order
    words = [re.escape(word) for word in query.split()] # Escape special regex chars in user query
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*' # \b ensures word boundaries, .*? matches anything non-greedily between words
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        logger.warning(f"Invalid regex pattern generated for query: '{query}'. Falling back to simple text search.")
        regex = query # Fallback, might not work well with MongoDB text index

    # Define the base filter query
    filter_query = {'file_name': regex}
    if USE_CAPTION_FILTER: # Optionally search in captions too
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}

    results = []; total_results = 0

    async def run_find(db_collection, q_filter, skip, limit):
        """Helper to run find and count in executor."""
        if db_collection is None: return [], 0
        try:
            # Count matching documents (run in executor)
            count = await loop.run_in_executor(None, partial(db_collection.count_documents, q_filter))
            # Find documents with skip and limit (run in executor)
            cursor = db_collection.find(q_filter).skip(skip).limit(limit)
            # Convert cursor to list (run in executor)
            docs = await loop.run_in_executor(None, list, cursor)
            return docs, count
        except Exception as e:
            logger.error(f"Database query error ({db_collection.name}): {e}"); return [], 0

    # Search primary database
    primary_docs, primary_count = await run_find(collection, filter_query, offset, max_results)
    results.extend(primary_docs)
    total_results += primary_count

    # If secondary DB exists and we need more results
    remaining_limit = max_results - len(primary_docs)
    if second_collection is not None and remaining_limit > 0:
        # Calculate offset for secondary DB
        # If offset is within primary results, start secondary search from 0 relative to its own results
        # If offset is beyond primary results, adjust secondary offset accordingly
        secondary_offset = max(0, offset - primary_count) if primary_count > 0 and offset >= primary_count else 0
        secondary_docs, secondary_count = await run_find(second_collection, filter_query, secondary_offset, remaining_limit)
        results.extend(secondary_docs)
        total_results += secondary_count

    # Filter by language if specified (applied after combined results)
    if lang:
        lang = lang.lower() # Ensure case-insensitivity
        # Filter results list comprehension
        lang_files = [
            f for f in results
            if lang in f.get('file_name', '').lower() or lang in f.get('caption', '').lower()
        ]
        total_results = len(lang_files) # Update total count based on language filter
        # Apply offset and limit to the language-filtered list
        files_to_return = lang_files[offset : offset + max_results] # Python slicing handles offset/limit
    else:
        # If no language filter, just take the combined results up to max_results
        files_to_return = results[:max_results]

    # Calculate next offset for pagination
    current_page_count = len(files_to_return)
    next_offset_val = offset + current_page_count
    # If the next offset is greater than or equal to total results, there are no more pages
    next_offset_str = str(next_offset_val) if next_offset_val < total_results else ''

    return files_to_return, next_offset_str, total_results


async def delete_files(query):
    """Deletes files matching the query from both databases."""
    loop = asyncio.get_running_loop(); total_deleted = 0
    query = str(query).strip()
    if not query: return 0
    # Prepare regex for deletion
    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error: regex = query # Fallback
    filter_query = {'file_name': regex}
    # Could add caption filter here too if needed:
    # filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}

    async def run_delete(db_collection, q_filter):
        """Helper to run delete_many in executor."""
        if db_collection is None: return 0
        try:
            result = await loop.run_in_executor(None, partial(db_collection.delete_many, q_filter))
            return result.deleted_count if result else 0
        except Exception as e:
            logger.error(f"Error deleting from {db_collection.name}: {e}"); return 0

    # Delete from primary and secondary DBs
    deleted1 = await run_delete(collection, filter_query); total_deleted += deleted1
    if second_collection is not None:
        deleted2 = await run_delete(second_collection, filter_query); total_deleted += deleted2

    logger.info(f"Deleted {total_deleted} files matching query: '{query}'")
    return total_deleted

async def get_file_details(query_id):
    """Retrieves file details by its unique ID from either database."""
    loop = asyncio.get_running_loop()
    file_details = None
    # Try primary DB first
    if collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one in primary DB ({query_id}): {e}")
    # If not found or primary DB error, try secondary DB
    if not file_details and second_collection is not None:
         try: file_details = await loop.run_in_executor(None, partial(second_collection.find_one, {'_id': query_id}))
         except Exception as e: logger.error(f"Error find_one in secondary DB ({query_id}): {e}")
    # Return as a list (consistent with old code) or empty list
    return [file_details] if file_details else []

def encode_file_id(s: bytes) -> str:
    """Encodes byte data into a URL-safe base64 string."""
    r = b""; n = 0
    for i in s + bytes([22]) + bytes([4]): # Append specific bytes used in older formats?
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0 # Run-length encode consecutive zeros
            r += bytes([i])
    if n: r += b"\x00" + bytes([n]) # Handle trailing zeros
    return base64.urlsafe_b64encode(r).decode().rstrip("=") # Base64 encode and remove padding

def unpack_new_file_id(new_file_id):
    """
    Unpacks a Hydrogram file_id string into an older encoded format.
    Handles the AttributeError by using enums.MessageMediaType.
    """
    try:
        decoded = FileId.decode(new_file_id)

        # --- Corrected File Type Mapping ---
        # Use enums.MessageMediaType which exists in recent Hydrogram versions
        file_type_map = {
            enums.MessageMediaType.PHOTO: 0,
            enums.MessageMediaType.AUDIO: 1,
            enums.MessageMediaType.DOCUMENT: 2,
            enums.MessageMediaType.VIDEO: 3,
            enums.MessageMediaType.STICKER: 4,
            enums.MessageMediaType.VOICE: 5,
            enums.MessageMediaType.ANIMATION: 6,
            enums.MessageMediaType.VIDEO_NOTE: 7,
            # Add mappings for other types if needed, defaulting to DOCUMENT (2)
        }
        # Get the integer type, default to 2 (DOCUMENT) if type not in map
        file_type = file_type_map.get(decoded.file_type, 2)
        # --- End Correction ---

        # Pack data into bytes (assuming little-endian format)
        packed_data = pack("<iiqq", file_type, decoded.dc_id, decoded.media_id, decoded.access_hash)
        # Encode the packed bytes
        return encode_file_id(packed_data)
    except Exception as e:
        logger.error(f"Error unpacking file_id {new_file_id}: {e}", exc_info=True)
        return None # Return None on any error during unpacking

