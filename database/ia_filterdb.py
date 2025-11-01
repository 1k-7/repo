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
    DATABASE_URIS, DATABASE_NAME, COLLECTION_NAME, # Use new DATABASE_URIS
    USE_CAPTION_FILTER, MAX_BTN
)
# from utils import get_size # <--- REMOVED THIS LINE TO FIX CIRCULAR IMPORT

logger = logging.getLogger(__name__)

# Function duplicated from utils.py to break circular import
def get_size(size_bytes):
    if size_bytes is None or not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "0 B"
    size = float(size_bytes); units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]; i = 0
    while size >= 1024.0 and i < len(units) - 1: i += 1; size /= 1024.0
    return "%.2f %s" % (size, units[i])

# --- New Multi-DB Setup ---
file_db_clients = []
file_db_collections = []

try:
    # Split the URIs string into a list
    uris = DATABASE_URIS.split()
    if not uris:
        raise ValueError("DATABASE_URIS environment variable is empty.")
        
    for i, uri in enumerate(uris):
        try:
            client = MongoClient(uri)
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]
            
            # Try to create index, but log error if it fails (e.g., DB full)
            # Try to create index, but log error if it fails (e.g., DB full)
            try:
                collection.create_index([("file_name", TEXT)], background=True)
                # --- ADD THIS LINE ---
                collection.create_index([("file_name", 1), ("file_size", 1)], background=True) 
                
            except OperationFailure as e:
                if e.code == 8000: # AtlasError: "you are over your space quota"
                    logger.critical(f"Database #{i+1} is FULL! Couldn't create index: {e.details.get('errmsg', e)}")
                else:
                    logger.warning(f"Couldn't create index for Database #{i+1}: {e}")
            
            file_db_clients.append(client)
            file_db_collections.append(collection)
            logger.info(f"Connected to Files Database #{i+1}.")
            
        except Exception as e:
            logger.error(f"Failed to connect to Files Database #{i+1} (URI: {uri}): {e}")
            # Continue trying to connect to other DBs

    if not file_db_collections:
        logger.critical("No valid file database connections established. Exiting.")
        exit()

except Exception as e:
    logger.critical(f"Error processing DATABASE_URIS: {e}", exc_info=True)
    exit()
# --- End New Multi-DB Setup ---


def get_total_files_count():
     """Counts total documents across all file collections."""
     if not file_db_collections: return 0
     total_count = 0
     for collection in file_db_collections:
         try:
             total_count += collection.count_documents({})
         except Exception as e:
             logger.error(f"Error counting DB {collection.database.name}: {e}")
     return total_count

# This function is now the source for the total count
db_count_documents = get_total_files_count
# This is no longer needed
def second_db_count_documents(): return 0


async def save_file(media):
    """Saves file metadata to the first available database."""
    loop = asyncio.get_running_loop()
    if not file_db_collections:
        logger.error("No file DBs available, cannot save file.")
        return 'err'

    file_id = media.file_id
    if not file_id:
        logger.error("Received media with no file_id")
        return 'err'

    # --- Improved Cleaning Logic ---
    raw_file_name = str(media.file_name) if media.file_name else "UnknownFile"
    file_name = raw_file_name.strip() # 1. Strip whitespace
    file_name = re.sub(r"[@\(\)\[\]]", "", file_name) # 2. Remove special chars
    file_name = re.sub(r"(_|\-|\.|\+)+", " ", file_name) # 3. Replace separators with space
    file_name = re.sub(r'\s+', ' ', file_name).strip() # 4. Collapse spaces and final strip
    # --- End Improved Cleaning ---

    caption_text = str(media.caption) if media.caption is not None else ""
    file_caption = re.sub(r"@\w+|(_|\-|\.|\+)|https?://\S+", " ", caption_text).strip()
    file_caption = re.sub(r'\s+', ' ', file_caption)

    document = {
        '_id': file_id,
        'file_name': file_name,
        'file_size': media.file_size or 0,
        'caption': file_caption
    }

    # --- MODIFIED DUPLICATE CHECK ---
    
    # 1. Check for exact file_id (re-index check) across ALL databases
    try:
        id_query_filter = {'_id': file_id}
        id_find_tasks = [
            loop.run_in_executor(None, partial(collection.find_one, id_query_filter, {'_id': 1}))
            for collection in file_db_collections
        ]
        id_duplicates = await asyncio.gather(*id_find_tasks)
        if any(id_duplicates):
            logger.debug(f'[Duplicate] File already in DB (by ID): {file_name}')
            return 'dup'
    except Exception as e:
        logger.error(f"Duplicate ID check failed for {file_id}: {e}. Proceeding...")

    # 2. If no ID match, check for (file_name, file_size) (re-upload check)
    try:
        fn_query_filter = {'file_name': document['file_name'], 'file_size': document['file_size']}
        fn_find_tasks = [
            loop.run_in_executor(None, partial(collection.find_one, fn_query_filter, {'_id': 1}))
            for collection in file_db_collections
        ]
        fn_duplicates = await asyncio.gather(*fn_find_tasks)
        if any(fn_duplicates):
            logger.debug(f'[Duplicate] File already in DB (by name+size): {file_name}')
            return 'dup'
    except Exception as e:
        logger.error(f"Duplicate name+size check failed for {file_name}: {e}. Proceeding...")
    # --- END MODIFICATION ---


    # Try to insert into the first available (non-full) database
    for i, collection in enumerate(file_db_collections):
        db_name_log = f"DB #{i+1}"
        try:
            await loop.run_in_executor(None, partial(collection.insert_one, document))
            logger.info(f'Saved [{get_size(document["file_size"])}] to {db_name_log}: {file_name}')
            return 'suc' # Success!
        except DuplicateKeyError:
            logger.warning(f'Duplicate Key error on insert ({db_name_log}): {file_name}')
            return 'dup' # Should have been caught by the check above, but as a failsafe
        except OperationFailure as e:
             # Error code 8000 is "AtlasError" for "over space quota"
             if e.code == 8000:
                 logger.warning(f"{db_name_log} is FULL. Trying next DB... (Error: {e.details.get('errmsg', e)})")
                 continue # Try the next database in the list
             else:
                 logger.error(f"MongoDB Operation Failure on {db_name_log}: {e}")
                 return 'err' # Return error for other operation failures
        except Exception as e:
            logger.error(f"Unexpected error saving file to {db_name_log}: {e}", exc_info=True)
            return 'err' # Return error for other exceptions
    
    # If loop finishes, all databases are full or failed
    logger.critical(f"All {len(file_db_collections)} databases are full or failed. Cannot save file: {file_name}")
    return 'err' # Return error for other exceptions
    
    # If loop finishes, all databases are full or failed
    logger.critical(f"All {len(file_db_collections)} databases are full or failed. Cannot save file: {file_name}")
    return 'err'


async def get_search_results(query, max_results=MAX_BTN, offset=0, lang=None):
    """Searches for files across all databases."""
    loop = asyncio.get_running_loop()
    query = str(query).strip()
    if not query: return [], '', 0

    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        logger.warning(f"Invalid regex for query: '{query}'. Falling back.")
        simple_regex_pattern = r".*".join(words)
        regex = re.compile(simple_regex_pattern, flags=re.IGNORECASE)

    filter_query = {'file_name': regex}
    if USE_CAPTION_FILTER:
        filter_query = {'$or': [{'file_name': regex}, {'caption': regex}]}

    results = []; total_results = 0

    async def run_find(db_collection, q_filter):
        """Helper to run find and count in executor for one collection."""
        if db_collection is None: return [], 0
        try:
            # Note: We query *all* results and slice later.
            # For large DBs, this is inefficient but simpler than managing offsets across N DBs.
            cursor = db_collection.find(q_filter) 
            docs = await loop.run_in_executor(None, list, cursor)
            count = len(docs) # Count from the returned docs
            return docs, count
        except Exception as e:
            logger.error(f"Database query error ({db_collection.database.name}): {e}"); return [], 0

    # Run find on all collections concurrently
    find_tasks = [run_find(collection, filter_query) for collection in file_db_collections]
    all_db_results = await asyncio.gather(*find_tasks)

    for docs, count in all_db_results:
        results.extend(docs)
        total_results += count

    # Filter by language if specified (applied after combined results)
    if lang:
        lang = lang.lower()
        lang_files = [
            f for f in results
            if lang in f.get('file_name', '').lower() or lang in f.get('caption', '').lower()
        ]
        total_results = len(lang_files) # Update total count
        files_to_return = lang_files[offset : offset + max_results]
    else:
        # If no language filter, just slice the combined results
        files_to_return = results[offset : offset + max_results]
        # total_results is already sum of all counts

    # Calculate next offset
    next_offset_val = offset + len(files_to_return)
    next_offset_str = str(next_offset_val) if next_offset_val < total_results else ''

    return files_to_return, next_offset_str, total_results


async def delete_files(query):
    """Deletes files matching the query from all databases."""
    loop = asyncio.get_running_loop(); total_deleted = 0
    query = str(query).strip()
    if not query: return 0

    words = [re.escape(word) for word in query.split()]
    raw_pattern = r'\b' + r'.*?\b'.join(words) + r'.*'
    try: regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error: 
        simple_regex_pattern = r".*".join(words)
        regex = re.compile(simple_regex_pattern, flags=re.IGNORECASE)
        
    filter_query = {'file_name': regex}

    async def run_delete(db_collection, q_filter):
        """Helper to run delete_many in executor."""
        if db_collection is None: return 0
        try:
            result = await loop.run_in_executor(None, partial(db_collection.delete_many, q_filter))
            return result.deleted_count if result else 0
        except Exception as e:
            logger.error(f"Error deleting from {db_collection.database.name}: {e}"); return 0

    # Run delete on all collections concurrently
    delete_tasks = [run_delete(collection, filter_query) for collection in file_db_collections]
    deleted_counts = await asyncio.gather(*delete_tasks)
    total_deleted = sum(deleted_counts)

    logger.info(f"Deleted {total_deleted} files matching query: '{query}' from all DBs.")
    return total_deleted

async def get_file_details(query_id):
    """Retrieves file details by its unique ID from any database."""
    loop = asyncio.get_running_loop()
    
    # Check all databases
    for collection in file_db_collections:
        file_details = None
        try:
            file_details = await loop.run_in_executor(None, partial(collection.find_one, {'_id': query_id}))
        except Exception as e:
            logger.error(f"Error find_one in {collection.database.name} ({query_id}): {e}")
        
        if file_details:
            return [file_details] # Return as list if found
            
    return [] # Return empty list if not found in any DB