from hydrogram.errors import UserNotParticipant, FloodWait
from info import LONG_IMDB_DESCRIPTION, ADMINS, TIME_ZONE, INDEX_EXTENSIONS
from imdb import Cinemagoer
import asyncio
from functools import partial # Import partial
from hydrogram.types import InlineKeyboardButton, Message # Import Message
from hydrogram import enums, types # Import types
import re, os, requests, pytz, logging
from datetime import datetime, timedelta, timezone # Added timezone
from database.users_chats_db import db # db object uses sync pymongo
from shortzy import Shortzy

logger = logging.getLogger(__name__) # Add logger

# Initialize Cinemagoer instance
try:
    imdb = Cinemagoer()
except Exception as e:
    logger.error(f"Failed to initialize Cinemagoer: {e}")
    imdb = None

class temp(object):
    START_TIME = 0
    BANNED_USERS = []
    BANNED_CHATS = []
    ME = None
    CANCEL = False
    U_NAME = None
    B_NAME = None
    SETTINGS = {}
    VERIFICATIONS = {}
    FILES = {}
    USERS_CANCEL = False
    GROUPS_CANCEL = False
    BOT = None
    # No PREMIUM cache

# Placeholder function - Premium system disabled
async def is_premium(user_id, bot):
    """Placeholder: Premium system is disabled."""
    return True # Grant access as if premium is not required

async def is_subscribed(bot, query_or_message: types.Message | types.CallbackQuery): # Accept query or message
    btn = []
    loop = asyncio.get_running_loop() # Get loop
    stg = await loop.run_in_executor(None, db.get_bot_sttgs) # Wrap sync call
    if not stg: return btn

    user_id = query_or_message.from_user.id
    # message_obj = query_or_message if isinstance(query_or_message, types.Message) else query_or_message.message

    fsub_channels_str = stg.get('FORCE_SUB_CHANNELS', '')
    req_fsub_channel_str = stg.get('REQUEST_FORCE_SUB_CHANNELS')

    all_fsub_ids = []
    if req_fsub_channel_str:
        try: req_chan_id = int(req_fsub_channel_str); all_fsub_ids.append(req_chan_id)
        except ValueError: logger.warning(f"Invalid REQUEST_FORCE_SUB_CHANNELS ID: {req_fsub_channel_str}")

    for chan_id_str in fsub_channels_str.split(' '):
        if chan_id_str:
            try:
                chan_id_int = int(chan_id_str)
                if chan_id_int not in all_fsub_ids: all_fsub_ids.append(chan_id_int)
            except ValueError: logger.warning(f"Invalid FORCE_SUB_CHANNELS ID found: {chan_id_str}")

    if not all_fsub_ids: return btn

    req_fsub_channel_int = int(req_fsub_channel_str) if req_fsub_channel_str and req_fsub_channel_str.lstrip('-').isdigit() else None

    for chat_id in all_fsub_ids:
        is_request_channel = (chat_id == req_fsub_channel_int)
        user_is_member = False; invite_link = None

        try:
            # Wrap sync db call
            has_pending_req = await loop.run_in_executor(None, db.find_join_req, user_id)

            if is_request_channel and has_pending_req:
                 logger.debug(f"User {user_id} has pending join request for {chat_id}")
                 user_is_member = True # Treat pending request as sufficient
            else:
                 member = await bot.get_chat_member(chat_id, user_id) # Async, OK
                 if member.status not in [enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED]:
                      user_is_member = True
        except UserNotParticipant: logger.debug(f"User {user_id} not participant in {chat_id}")
        except Exception as e: logger.error(f"Error checking member {user_id} in {chat_id}: {e}")

        if not user_is_member:
            try:
                 chat = await bot.get_chat(chat_id) # Async, OK
                 invite_link = chat.invite_link
                 if not invite_link:
                      creates_join_req = is_request_channel
                      invite_link_obj = await bot.create_chat_invite_link(chat_id, creates_join_request=creates_join_req) # Async, OK
                      invite_link = invite_link_obj.invite_link
                 btn.append([InlineKeyboardButton(f'{chat.title}', url=invite_link)])
            except Exception as e: logger.error(f"Could not get/create invite link for {chat_id}: {e}")

    return btn


def upload_image(file_path): # This is sync, calls to it must be wrapped
    try:
        with open(file_path, 'rb') as f:
            files = {'files[]': f}
            response = requests.post("https://uguu.se/upload", files=files, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data['files'][0]['url'].replace('\\/', '/')
    except requests.RequestException as e: logger.error(f"uguu.se upload request error: {e}"); return None
    except (KeyError, IndexError, Exception) as e:
        logger.error(f"uguu.se response/upload error: {e}")
        try: os.remove(file_path)
        except: pass
        return None

def list_to_str(k): # Sync
    if not k: return "ɴ/ᴀ"
    str_list = [str(elem).strip() for elem in k if elem is not None]
    if not str_list: return "ɴ/ᴀ"
    if len(str_list) == 1: return str_list[0]
    return ', '.join(str_list)

async def get_poster(query, bulk=False, id=False, file=None):
    if not imdb: logger.warning("IMDb lookup skipped: Cinemagoer not initialized."); return None
    loop = asyncio.get_running_loop()

    # Define synchronous parts to run in executor
    def _sync_imdb_search(title_search, year_str_search):
        movies_search = imdb.search_movie(title_search, results=15)
        if not movies_search: return None, None
        filtered = movies_search
        if year_str_search:
            year_filtered_search = [m for m in movies_search if str(m.get('year', '')) == year_str_search]
            if year_filtered_search: filtered = year_filtered_search
        kind_filtered_search = [m for m in filtered if m.get('kind') in ['movie', 'tv series']]
        if not kind_filtered_search: kind_filtered_search = filtered # Fallback
        return kind_filtered_search, movies_search

    def _sync_imdb_get_details(movie_id_get):
        movie_get = imdb.get_movie(movie_id_get)
        if movie_get: imdb.update(movie_get, info=['main', 'plot', 'critic reviews'])
        return movie_get

    movie_id = None
    if not id:
        query_lower = (str(query).strip()).lower()
        title = query_lower
        year_search_re = re.findall(r'[1-2]\d{3}$', query_lower, re.IGNORECASE)
        year_str = None
        if year_search_re:
            year_str = list_to_str(year_search_re[:1])
            title = re.sub(r'[1-2]\d{3}$', '', title, flags=re.IGNORECASE).strip()
        elif file is not None:
            year_search_file = re.findall(r'[1-2]\d{3}', str(file), re.IGNORECASE)
            if year_search_file: year_str = list_to_str(year_search_file[:1])

        try:
            logger.debug(f"Searching IMDb (async wrap): title='{title}', year={year_str}")
            # Run blocking search in executor
            kind_filtered, _ = await loop.run_in_executor(None, partial(_sync_imdb_search, title, year_str))

            if not kind_filtered: logger.debug("No movie/tv series found after filter"); return None
            if bulk: return kind_filtered

            movie_obj = kind_filtered[0]
            movie_id = movie_obj.movieID
            logger.debug(f"Selected IMDb: {movie_obj.get('title')} ({movie_obj.get('year')}) ID: {movie_id}")

        except Exception as e: logger.error(f"IMDb search exception (async wrap): {e}", exc_info=False); return None
    else:
        movie_id = query

    if not movie_id: return None # Ensure we have an ID

    # Fetch full details
    try:
        logger.debug(f"Fetching details (async wrap) for IMDb ID: {movie_id}")
        # Run blocking get_movie in executor
        movie = await loop.run_in_executor(None, partial(_sync_imdb_get_details, movie_id))

        if not movie: logger.warning(f"Could not get details for ID {movie_id}"); return None
        logger.debug(f"Fetched details for {movie.get('title')}")
    except Exception as e: logger.error(f"IMDb get_movie exception (async wrap): {e}", exc_info=False); return None

    # --- Extract data (this part is CPU bound, fine in async) ---
    na_styled = "ɴ/ᴀ"
    m_title = movie.get('title', na_styled)
    year = movie.get('year', '')
    year_info = f"({year})" if year else ""
    genres = list_to_str(movie.get("genres", [na_styled]))
    rating = str(movie.get("rating", na_styled))
    votes_data = movie.get('votes', na_styled) # Votes fetched by update
    votes = f"{votes_data:,}" if isinstance(votes_data, int) else votes_data
    languages = list_to_str(movie.get("languages", [na_styled]))
    runtime_data = movie.get("runtimes")
    runtime = f"{runtime_data[0]} ᴍɪɴs" if isinstance(runtime_data, list) and runtime_data else na_styled

    if LONG_IMDB_DESCRIPTION: plot = movie.get('plot outline') or (movie.get('plot') and movie.get('plot')[0]) or na_styled
    else: plot = (movie.get('plot') and movie.get('plot')[0]) or movie.get('plot outline') or na_styled
    if plot != na_styled and len(plot) > 400: plot = plot[:397] + "..."
    poster_url = movie.get('full-size cover url')

    return {
        'title': m_title, 'year': year, 'year_info': year_info, 'genres': genres,
        'rating': rating, 'votes': votes, 'languages': languages, 'runtime': runtime,
        'plot': plot, 'poster': poster_url, 'url': f'https://www.imdb.com/title/tt{movie_id}',
        "aka": list_to_str(movie.get("akas", [])),
        "seasons": movie.get("number of seasons", na_styled),
        "box_office": movie.get('box office', na_styled),
        "localized_title": movie.get('localized title', m_title),
        "kind": movie.get("kind", na_styled),
        "imdb_id": f"tt{movie.get('imdbID', movie_id)}",
        "cast": list_to_str(movie.get("cast", [])),
        "countries": list_to_str(movie.get("countries", [])),
        "certificates": list_to_str(movie.get("certificates", [])),
        "director": list_to_str(movie.get("director", [])),
        "writer": list_to_str(movie.get("writer", [])),
        "producer": list_to_str(movie.get("producer", [])),
        "composer": list_to_str(movie.get("composer", [])),
        "cinematographer": list_to_str(movie.get("cinematographer", [])),
        "music_team": list_to_str(movie.get("music department", [])),
        "distributors": list_to_str(movie.get("distributors", [])),
        'release_date': movie.get("original air date") or movie.get("year") or na_styled,
    }


async def is_check_admin(bot, chat_id, user_id):
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]
    except Exception as e: logger.error(f"Admin check error ({chat_id}, {user_id}): {e}"); return False

# --- CORRECTED DB Wrappers ---
async def get_verify_status(user_id):
    user_id = int(user_id)
    verify = temp.VERIFICATIONS.get(user_id)
    if not verify:
        loop = asyncio.get_running_loop()
        # Wrap the sync db call
        verify = await loop.run_in_executor(None, db.get_verify_status, user_id)
        temp.VERIFICATIONS[user_id] = verify # Cache result

    # Expiry logic (CPU bound, fine in async)
    expire_time = verify.get('expire_time')
    # Ensure times are datetime objects or None before comparison
    if not isinstance(expire_time, datetime) and expire_time is not None:
        verify['expire_time'] = None # Invalidate non-datetime
        
    if not isinstance(expire_time, datetime):
         verified_time = verify.get('verified_time')
         if not isinstance(verified_time, datetime) and verified_time is not None:
             verified_time = None
             
         if isinstance(verified_time, datetime):
              base_time = verified_time.replace(tzinfo=timezone.utc) if verified_time.tzinfo is None else verified_time
              verify['expire_time'] = base_time + timedelta(seconds=VERIFY_EXPIRE)
         else:
              verify['expire_time'] = datetime.fromtimestamp(0, tz=timezone.utc) if verify.get('is_verified') else datetime.fromtimestamp(0, tz=timezone.utc)

    return verify.copy() # Return copy

async def update_verify_status(user_id, verify_token="", is_verified=False, link="", expire_time=None):
    user_id = int(user_id)
    current = await get_verify_status(user_id) # Use the async wrapper

    current['verify_token'] = verify_token
    current['is_verified'] = is_verified
    current['link'] = link
    if isinstance(expire_time, datetime): current['expire_time'] = expire_time
    if is_verified and (not isinstance(current.get('verified_time'), datetime) or not current.get('is_verified')):
         current['verified_time'] = datetime.now(timezone.utc)

    temp.VERIFICATIONS[user_id] = current.copy() # Update cache

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_verify_status, user_id, current) # Wrap sync db call

async def get_settings(group_id):
    group_id = int(group_id)
    settings = temp.SETTINGS.get(group_id)
    if not settings:
        loop = asyncio.get_running_loop()
        settings = await loop.run_in_executor(None, db.get_settings, group_id)
        temp.SETTINGS[group_id] = settings
    return settings.copy() if settings else db.default_setgs.copy()

async def save_group_settings(group_id, key, value):
    group_id = int(group_id)
    current = await get_settings(group_id)
    current[key] = value
    temp.SETTINGS[group_id] = current.copy()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.update_settings, group_id, current)

# broadcast_messages remains async
async def broadcast_messages(user_id, message, pin):
    try:
        m = await message.copy(chat_id=user_id)
        if pin: await m.pin(disable_notification=True)
        return "Success"
    except FloodWait as e: logger.warning(f"FloodWait user {user_id}: sleep {e.value}"); await asyncio.sleep(e.value); return await broadcast_messages(user_id, message, pin)
    except Exception as e: logger.error(f"Broadcast user error {user_id}: {e}"); db.delete_user(int(user_id)); return "Error"

# groups_broadcast_messages remains async
async def groups_broadcast_messages(chat_id, message, pin):
    try:
        k = await message.copy(chat_id=chat_id)
        if pin:
            try: await k.pin(disable_notification=True)
            except Exception as pin_e: logger.warning(f"Pin error group {chat_id}: {pin_e}")
        return "Success"
    except FloodWait as e: logger.warning(f"FloodWait group {chat_id}: sleep {e.value}"); await asyncio.sleep(e.value); return await groups_broadcast_messages(chat_id, message, pin)
    except Exception as e: logger.error(f"Broadcast group error {chat_id}: {e}"); db.delete_chat(chat_id); return "Error"

# get_size remains sync
def get_size(size_bytes):
    if size_bytes is None or not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "0 B"
    size = float(size_bytes); units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]; i = 0
    while size >= 1024.0 and i < len(units) - 1: i += 1; size /= 1024.0
    return "%.2f %s" % (size, units[i])

# get_shortlink remains async
async def get_shortlink(url, api, link):
    if not url or not api: logger.warning("Shortlink URL/API missing."); return link
    try: shortzy = Shortzy(api_key=api, base_site=url); short_link = await shortzy.convert(link)
    except Exception as e: logger.error(f"Shortzy Error: {e}"); return link
    return short_link if short_link and isinstance(short_link, str) and short_link.startswith(('http://', 'https://')) else link

# get_readable_time remains sync
def get_readable_time(seconds):
    if seconds is None or not isinstance(seconds, (int, float)) or seconds < 0: return "0s"
    seconds = int(seconds); result = ''; periods = [('d', 86400), ('h', 3600), ('m', 60), ('s', 1)]
    for name, secs in periods:
        if seconds >= secs: val, seconds = divmod(seconds, secs); result += f'{val}{name}'
    return result if result else '0s'

# get_wish remains sync
def get_wish():
    try:
        time_now = datetime.now(pytz.timezone(TIME_ZONE))
        hour = int(time_now.strftime("%H"))
        if 5 <= hour < 12: return "ɢᴏᴏᴅ ᴍᴏʀɴɪɴɢ ☀️"
        elif 12 <= hour < 18: return "ɢᴏᴏᴅ ᴀғᴛᴇʀɴᴏᴏɴ 🌤️"
        else: return "ɢᴏᴏᴅ ᴇᴠᴇɴɪɴɢ 🌙"
    except Exception as e: logger.error(f"Get wish error: {e}"); return "ʜᴇʟʟᴏ"

# --- CORRECTED get_seconds ---
def get_seconds(time_string): # Changed to regular def
    match = re.match(r"(\d+)\s*(s|sec|m|min|h|hr|d|day|w|week|month|y|year)$", str(time_string).lower().strip(), re.IGNORECASE)
    if not match: return 0
    value = int(match.group(1)); unit = match.group(2)
    if unit.startswith('s'): return value
    elif unit.startswith('m') and "month" not in unit: return value * 60
    elif unit.startswith('h'): return value * 3600
    elif unit.startswith('d'): return value * 86400
    elif unit.startswith('w'): return value * 86400 * 7
    elif unit.startswith('month'): return value * 86400 * 30 # Approx
    elif unit.startswith('y'): return value * 86400 * 365 # Approx
    else: return 0
