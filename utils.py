from hydrogram.errors import UserNotParticipant, FloodWait
from info import LONG_IMDB_DESCRIPTION, ADMINS, TIME_ZONE, INDEX_EXTENSIONS
from imdb import Cinemagoer
import asyncio
from hydrogram.types import InlineKeyboardButton
from hydrogram import enums
import re, os, requests, pytz, logging
from datetime import datetime, timedelta # Added timedelta
from database.users_chats_db import db
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

async def is_subscribed(bot, query):
    btn = []
    stg = db.get_bot_sttgs()
    if not stg: return btn

    fsub_channels_str = stg.get('FORCE_SUB_CHANNELS', '')
    req_fsub_channel_str = stg.get('REQUEST_FORCE_SUB_CHANNELS')

    all_fsub_ids = []
    # Process REQUEST_FORCE_SUB_CHANNELS first if it exists
    if req_fsub_channel_str:
        try:
            req_chan_id = int(req_fsub_channel_str)
            all_fsub_ids.append(req_chan_id)
        except ValueError:
            logger.warning(f"Invalid REQUEST_FORCE_SUB_CHANNELS ID: {req_fsub_channel_str}")

    # Process FORCE_SUB_CHANNELS
    for chan_id_str in fsub_channels_str.split(' '):
        if chan_id_str: # Avoid empty strings
            try:
                chan_id_int = int(chan_id_str)
                if chan_id_int not in all_fsub_ids:
                    all_fsub_ids.append(chan_id_int)
            except ValueError:
                 logger.warning(f"Invalid FORCE_SUB_CHANNELS ID found: {chan_id_str}")

    if not all_fsub_ids:
        return btn # No valid force sub channels configured

    user_id = query.from_user.id
    req_fsub_channel_int = int(req_fsub_channel_str) if req_fsub_channel_str and req_fsub_channel_str.lstrip('-').isdigit() else None

    for chat_id in all_fsub_ids:
        is_request_channel = (chat_id == req_fsub_channel_int)
        user_is_member = False
        invite_link = None

        try:
            # Check if user has a pending join request (only for request channel)
            if is_request_channel and db.find_join_req(user_id):
                 logger.debug(f"User {user_id} has pending join request for {chat_id}")
                 user_is_member = True # Treat pending request as sufficient for now
            else:
                 member = await bot.get_chat_member(chat_id, user_id)
                 if member.status not in [enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED]:
                      user_is_member = True
        except UserNotParticipant:
            logger.debug(f"User {user_id} is not a participant in {chat_id}")
        except Exception as e:
            logger.error(f"Error checking chat member status for chat {chat_id}, user {user_id}: {e}")
            # Assume user is not member if error occurs, try to get invite link

        # If user is not considered a member, get invite link
        if not user_is_member:
            try:
                 chat = await bot.get_chat(chat_id) # Get chat object for title and potentially existing link
                 invite_link = chat.invite_link
                 if not invite_link:
                      # Create join request link ONLY for the designated request channel
                      creates_join_req = is_request_channel
                      invite_link_obj = await bot.create_chat_invite_link(chat_id, creates_join_request=creates_join_req)
                      invite_link = invite_link_obj.invite_link
                 btn.append([InlineKeyboardButton(f'{chat.title}', url=invite_link)])
            except Exception as e:
                logger.error(f"Could not get/create invite link for {chat_id}: {e}")
                # Optionally add a button indicating an error or skip this channel

    return btn


def upload_image(file_path):
    # Keep if used, otherwise remove
    try:
        with open(file_path, 'rb') as f:
            files = {'files[]': f}
            response = requests.post("https://uguu.se/upload", files=files, timeout=10)
        response.raise_for_status() # Raise error for bad status codes
        data = response.json()
        return data['files'][0]['url'].replace('\\/', '/')
    except requests.RequestException as e:
         logger.error(f"Error during uguu.se upload request: {e}")
         return None
    except (KeyError, IndexError, Exception) as e:
        logger.error(f"Error processing uguu.se response or uploading image: {e}")
        try: os.remove(file_path)
        except: pass
        return None

# Ensure list_to_str handles empty lists or None gracefully
def list_to_str(k):
    if not k: return "ɴ/ᴀ" # Use styled N/A
    str_list = [str(elem).strip() for elem in k if elem is not None]
    if not str_list: return "ɴ/ᴀ"
    if len(str_list) == 1: return str_list[0]
    # from info import MAX_LIST_ELM # Optional limit
    # if MAX_LIST_ELM: str_list = str_list[:int(MAX_LIST_ELM)]
    return ', '.join(str_list)

# Refined get_poster function (Ensure IMDb is initialized)
async def get_poster(query, bulk=False, id=False, file=None):
    if not imdb:
         logger.warning("Cinemagoer (imdb) not initialized. Cannot fetch details.")
         return None

    if not id:
        query_lower = (str(query).strip()).lower()
        title = query_lower
        year_search = re.findall(r'[1-2]\d{3}$', query_lower, re.IGNORECASE)
        year_str = None
        if year_search:
            year_str = list_to_str(year_search[:1])
            title = re.sub(r'[1-2]\d{3}$', '', title, flags=re.IGNORECASE).strip()
        elif file is not None:
            year_search_file = re.findall(r'[1-2]\d{3}', str(file), re.IGNORECASE)
            if year_search_file: year_str = list_to_str(year_search_file[:1])

        try:
            logger.debug(f"Searching IMDb for title: '{title}', year: {year_str}")
            # Increase results slightly to have better chance finding right type/year
            movies = imdb.search_movie(title, results=15)
            if not movies: logger.debug(f"No IMDb results for '{title}'"); return None

            filtered_movies = movies
            if year_str:
                year_filtered = [k for k in movies if str(k.get('year', '')) == year_str]
                # Only use year filter if it yields results, otherwise keep broader list
                if year_filtered: filtered_movies = year_filtered; logger.debug("Filtered by year")
                else: logger.debug("Year filter applied but no matches found, using original results")

            # Prioritize exact kind matches, then fallback
            kind_filtered = [k for k in filtered_movies if k.get('kind') in ['movie', 'tv series']]
            if not kind_filtered: kind_filtered = filtered_movies # Fallback

            if not kind_filtered: logger.debug("No movie or tv series found after filtering"); return None

            if bulk: return kind_filtered

            movie_obj = kind_filtered[0]
            movie_id = movie_obj.movieID
            logger.debug(f"Selected movie: {movie_obj.get('title')} ({movie_obj.get('year')}) ID: {movie_id}")

        except Exception as e:
            logger.error(f"IMDb search exception for '{title}': {e}", exc_info=False) # Keep logs concise
            return None
    else:
        movie_id = query

    # Fetch full movie details
    try:
        logger.debug(f"Fetching details for IMDb ID: {movie_id}")
        movie = imdb.get_movie(movie_id)
        if not movie: logger.warning(f"Could not get movie details for ID {movie_id}"); return None
        # Update might fetch more, especially plot outline and votes
        imdb.update(movie, info=['main', 'plot', 'critic reviews'])
        logger.debug(f"Successfully fetched details for {movie.get('title')}")
    except Exception as e:
        logger.error(f"IMDb get_movie exception for ID '{movie_id}': {e}", exc_info=False)
        return None

    # --- Extract data with safe defaults (using new styled N/A) ---
    na_styled = "ɴ/ᴀ"
    m_title = movie.get('title', na_styled)
    year = movie.get('year', '')
    year_info = f"({year})" if year else ""
    genres = list_to_str(movie.get("genres", [na_styled]))
    rating = str(movie.get("rating", na_styled))
    # Fetch votes after update
    votes_data = movie.get('votes', na_styled)
    votes = f"{votes_data:,}" if isinstance(votes_data, int) else votes_data # Format votes with comma

    languages = list_to_str(movie.get("languages", [na_styled]))
    runtime_data = movie.get("runtimes")
    runtime = f"{runtime_data[0]} ᴍɪɴs" if isinstance(runtime_data, list) and runtime_data else na_styled

    # Plot logic
    if LONG_IMDB_DESCRIPTION:
        plot = movie.get('plot outline') or (movie.get('plot') and movie.get('plot')[0]) or na_styled
    else:
        plot = (movie.get('plot') and movie.get('plot')[0]) or movie.get('plot outline') or na_styled
    if plot != na_styled and len(plot) > 400: plot = plot[:397] + "..."

    poster_url = movie.get('full-size cover url')

    # Return dict matching the updated template
    return {
        'title': m_title, 'year': year, 'year_info': year_info, 'genres': genres,
        'rating': rating, 'votes': votes, 'languages': languages, 'runtime': runtime,
        'plot': plot, 'poster': poster_url, 'url': f'https://www.imdb.com/title/tt{movie_id}',
        # Add other fields needed, using na_styled as default
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
    except Exception as e:
        logger.error(f"Error checking admin status for user {user_id} in chat {chat_id}: {e}")
        return False

# get_verify_status remains the same (handles verification, not premium)
async def get_verify_status(user_id):
    user_id = int(user_id) # Ensure int
    verify = temp.VERIFICATIONS.get(user_id)
    if not verify:
        verify = await db.get_verify_status(user_id) # Assumes db returns default if not found
        temp.VERIFICATIONS[user_id] = verify # Cache it

    # Ensure expire_time exists and is valid datetime
    expire_time = verify.get('expire_time')
    if not isinstance(expire_time, datetime):
         verified_time = verify.get('verified_time')
         if isinstance(verified_time, datetime):
              from info import VERIFY_EXPIRE # Import locally if needed
              verify['expire_time'] = verified_time + timedelta(seconds=VERIFY_EXPIRE)
         else:
              # Set to epoch start if times are invalid but somehow verified
              verify['expire_time'] = datetime.utcfromtimestamp(0) if verify.get('is_verified') else datetime.utcfromtimestamp(0)

    return verify.copy() # Return a copy


# update_verify_status remains the same
async def update_verify_status(user_id, verify_token="", is_verified=False, link="", expire_time=None): # Use None default for expire_time
    user_id = int(user_id)
    current = await get_verify_status(user_id) # Fetch current status first

    current['verify_token'] = verify_token
    current['is_verified'] = is_verified
    current['link'] = link

    # Only update expire_time if a valid datetime is provided
    if isinstance(expire_time, datetime):
         current['expire_time'] = expire_time
    # Update verified_time when verification becomes true
    if is_verified and not current.get('is_verified'): # Check if status changed to verified
         current['verified_time'] = datetime.now(pytz.utc) # Use timezone-aware datetime

    temp.VERIFICATIONS[user_id] = current # Update cache
    await db.update_verify_status(user_id, current) # Update DB

# broadcast_messages remains the same
async def broadcast_messages(user_id, message, pin):
    try:
        m = await message.copy(chat_id=user_id)
        if pin:
            await m.pin(disable_notification=True)
        return "Success"
    except FloodWait as e:
        logger.warning(f"FloodWait for {e.value}s during user broadcast to {user_id}")
        await asyncio.sleep(e.value)
        return await broadcast_messages(user_id, message, pin)
    except Exception as e:
        logger.error(f"Error broadcasting to user {user_id}: {e}")
        await db.delete_user(int(user_id))
        return "Error" # Consider more specific error types

# groups_broadcast_messages remains the same
async def groups_broadcast_messages(chat_id, message, pin):
    try:
        k = await message.copy(chat_id=chat_id)
        if pin:
            try: await k.pin(disable_notification=True)
            except Exception as pin_e: logger.warning(f"Could not pin in group {chat_id}: {pin_e}")
        return "Success"
    except FloodWait as e:
        logger.warning(f"FloodWait for {e.value}s during group broadcast to {chat_id}")
        await asyncio.sleep(e.value)
        return await groups_broadcast_messages(chat_id, message, pin)
    except Exception as e:
        logger.error(f"Error broadcasting to group {chat_id}: {e}")
        await db.delete_chat(chat_id)
        return "Error"

# get_settings remains the same
async def get_settings(group_id):
    group_id = int(group_id)
    settings = temp.SETTINGS.get(group_id)
    if not settings:
        settings = await db.get_settings(group_id) # Returns merged defaults
        temp.SETTINGS[group_id] = settings
    return settings

# save_group_settings remains the same
async def save_group_settings(group_id, key, value):
    group_id = int(group_id)
    current = await get_settings(group_id)
    current[key] = value
    temp.SETTINGS[group_id] = current
    await db.update_settings(group_id, current)

# get_size remains the same
def get_size(size_bytes):
    if size_bytes is None or not isinstance(size_bytes, (int, float)) or size_bytes < 0: return "0 B"
    size = float(size_bytes); units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]; i = 0
    while size >= 1024.0 and i < len(units) - 1: i += 1; size /= 1024.0
    return "%.2f %s" % (size, units[i])

# get_shortlink remains the same
async def get_shortlink(url, api, link):
    if not url or not api:
        logger.warning("Shortlink URL or API is missing in settings.")
        return link
    try:
        # Ensure base_site format is correct for the library if needed
        shortzy = Shortzy(api_key=api, base_site=url)
        short_link = await shortzy.convert(link)
        # Add basic validation if possible
        if short_link and isinstance(short_link, str) and short_link.startswith(('http://', 'https://')):
             return short_link
        else:
             logger.error(f"Shortzy returned invalid link: {short_link}")
             return link
    except Exception as e:
        logger.error(f"Shortzy Error: {e}")
        return link

# get_readable_time remains the same
def get_readable_time(seconds):
    if seconds is None or not isinstance(seconds, (int, float)) or seconds < 0: return "0s"
    seconds = int(seconds); result = ''; periods = [('d', 86400), ('h', 3600), ('m', 60), ('s', 1)]
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f'{period_value}{period_name}'
    return result if result else '0s'

# get_wish remains the same
def get_wish():
    try:
        time_now = datetime.now(pytz.timezone(TIME_ZONE))
        hour = int(time_now.strftime("%H"))
        if 5 <= hour < 12: status = "ɢᴏᴏᴅ ᴍᴏʀɴɪɴɢ ☀️"
        elif 12 <= hour < 18: status = "ɢᴏᴏᴅ ᴀғᴛᴇʀɴᴏᴏɴ 🌤️"
        else: status = "ɢᴏᴏᴅ ᴇᴠᴇɴɪɴɢ 🌙"
        return status
    except Exception as e: logger.error(f"Error getting wish: {e}"); return "ʜᴇʟʟᴏ"

# get_seconds remains the same
async def get_seconds(time_string):
    match = re.match(r"(\d+)\s*(s|sec|second|seconds|m|min|minute|minutes|h|hr|hour|hours|d|day|days|w|week|weeks|month|months|y|year|years)$", str(time_string).lower().strip())
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
