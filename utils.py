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
    START_TIME = 0; BANNED_USERS = []; BANNED_CHATS = []; ME = None; CANCEL = False
    U_NAME = None; B_NAME = None; SETTINGS = {}; VERIFICATIONS = {}; FILES = {}
    USERS_CANCEL = False; GROUPS_CANCEL = False; BOT = None

async def is_premium(user_id, bot): return True # Placeholder

async def is_subscribed(bot, query_or_message: types.Message | types.CallbackQuery): # Accept query or message
    btn = []
    loop = asyncio.get_running_loop()
    stg = await loop.run_in_executor(None, db.get_bot_sttgs) # Wrap sync
    if not stg: return btn
    user_id = query_or_message.from_user.id
    fsub_str = stg.get('FORCE_SUB_CHANNELS', ''); req_str = stg.get('REQUEST_FORCE_SUB_CHANNELS')
    all_ids = []
    if req_str: try: all_ids.append(int(req_str)) except ValueError: logger.warning(f"Invalid REQ_FSUB ID: {req_str}")
    for id_str in fsub_str.split():
        if id_str: try: id_int = int(id_str); if id_int not in all_ids: all_ids.append(id_int) except ValueError: logger.warning(f"Invalid FSUB ID: {id_str}")
    if not all_ids: return btn
    req_int = int(req_str) if req_str and req_str.lstrip('-').isdigit() else None
    for chat_id in all_ids:
        is_req = (chat_id == req_int); is_mem = False; link = None
        try:
            has_pending = await loop.run_in_executor(None, db.find_join_req, user_id) # Wrap sync
            if is_req and has_pending: is_mem = True
            else: member = await bot.get_chat_member(chat_id, user_id); is_mem = member.status not in [enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED]
        except UserNotParticipant: pass
        except Exception as e: logger.error(f"Check member {user_id} in {chat_id}: {e}")
        if not is_mem:
            try: chat = await bot.get_chat(chat_id); link = chat.invite_link or (await bot.create_chat_invite_link(chat_id, creates_join_request=is_req)).invite_link; btn.append([InlineKeyboardButton(f'{chat.title}', url=link)])
            except Exception as e: logger.error(f"Invite link {chat_id}: {e}")
    return btn

def upload_image(fp): # Sync, wrap calls
    try:
        with open(fp, 'rb') as f: files = {'files[]': f}; r = requests.post("https://uguu.se/upload", files=files, timeout=10)
        r.raise_for_status(); data = r.json(); return data['files'][0]['url'].replace('\\/', '/')
    except requests.RequestException as e: logger.error(f"Upload req err: {e}"); return None
    except Exception as e: logger.error(f"Upload resp/proc err: {e}"); try: os.remove(fp) except: pass; return None

def list_to_str(k): # Sync
    if not k: return "ɴ/ᴀ"; sl = [str(e).strip() for e in k if e is not None];
    if not sl: return "ɴ/ᴀ"; return sl[0] if len(sl) == 1 else ', '.join(sl)

async def get_poster(query, bulk=False, id=False, file=None): # Wraps sync imdb calls
    if not imdb: logger.warning("IMDb skip: not init."); return None
    loop = asyncio.get_running_loop()
    def _sync_search(t, y): m = imdb.search_movie(t, results=15); if not m: return None, None; f = m; if y: yf = [k for k in m if str(k.get('year',''))==y]; if yf: f=yf; kf = [k for k in f if k.get('kind') in ['movie','tv series']]; if not kf: kf=f; return kf, m
    def _sync_get(mid): mg = imdb.get_movie(mid); if mg: imdb.update(mg, info=['main','plot','reviews']); return mg
    mid = None
    if not id:
        q = str(query).strip().lower(); t = q; yr = re.findall(r'[1-2]\d{3}$', q, re.I); ys = None
        if yr: ys = list_to_str(yr[:1]); t = re.sub(r'[1-2]\d{3}$','', t, flags=re.I).strip()
        elif file: yf = re.findall(r'[1-2]\d{3}', str(file), re.I); if yf: ys = list_to_str(yf[:1])
        try: kf, _ = await loop.run_in_executor(None, partial(_sync_search, t, ys));
        except Exception as e: logger.error(f"IMDb search wrap err: {e}"); return None
        if not kf: logger.debug("No movie/tv found"); return None
        if bulk: return kf
        mo = kf[0]; mid = mo.movieID; logger.debug(f"Selected: {mo.get('title')} ({mo.get('year')}) ID:{mid}")
    else: mid = query
    if not mid: return None
    try: movie = await loop.run_in_executor(None, partial(_sync_get, mid));
    except Exception as e: logger.error(f"IMDb get wrap err: {e}"); return None
    if not movie: logger.warning(f"Could not get details ID {mid}"); return None
    nas="ɴ/ᴀ"; mt=movie.get('title', nas); y=movie.get('year',''); yi=f"({y})" if y else ""; g=list_to_str(movie.get("genres",[nas])); r=str(movie.get("rating",nas)); v=movie.get('votes',nas); vo=f"{v:,}" if isinstance(v,int) else v; l=list_to_str(movie.get("languages",[nas])); rt=movie.get("runtimes"); run=f"{rt[0]} ᴍɪɴs" if isinstance(rt,list) and rt else nas
    if LONG_IMDB_DESCRIPTION: plot = movie.get('plot outline') or (movie.get('plot') and movie.get('plot')[0]) or nas
    else: plot = (movie.get('plot') and movie.get('plot')[0]) or movie.get('plot outline') or nas
    if plot!=nas and len(plot)>400: plot=plot[:397]+"..."
    purl=movie.get('full-size cover url')
    return {'title':mt,'year':y,'year_info':yi,'genres':g,'rating':r,'votes':vo,'languages':l,'runtime':run,'plot':plot,'poster':purl,'url':f'https://imdb.com/title/tt{mid}', "aka":list_to_str(movie.get("akas",[])), "seasons":movie.get("number of seasons",nas), "box_office":movie.get('box office',nas), "localized_title":movie.get('localized title',mt), "kind":movie.get("kind",nas), "imdb_id":f"tt{movie.get('imdbID',mid)}", "cast":list_to_str(movie.get("cast",[])), "countries":list_to_str(movie.get("countries",[])), "certificates":list_to_str(movie.get("certificates",[])), "director":list_to_str(movie.get("director",[])), "writer":list_to_str(movie.get("writer",[])), "producer":list_to_str(movie.get("producer",[])), "composer":list_to_str(movie.get("composer",[])), "cinematographer":list_to_str(movie.get("cinematographer",[])), "music_team":list_to_str(movie.get("music department",[])), "distributors":list_to_str(movie.get("distributors",[])), 'release_date':movie.get("original air date") or movie.get("year") or nas,}

async def is_check_admin(bot, cid, uid):
    try: m = await bot.get_chat_member(cid, uid); return m.status in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]
    except Exception as e: logger.error(f"Admin check {cid},{uid}: {e}"); return False

# --- CORRECTED DB Wrappers ---
async def get_verify_status(user_id):
    user_id = int(user_id); verify = temp.VERIFICATIONS.get(user_id)
    if not verify: loop = asyncio.get_running_loop(); verify = await loop.run_in_executor(None, db.get_verify_status, user_id); temp.VERIFICATIONS[user_id] = verify # Wrap sync
    et = verify.get('expire_time')
    if not isinstance(et, datetime) and et is not None: verify['expire_time'] = None
    if not isinstance(et, datetime):
        vt = verify.get('verified_time')
        if not isinstance(vt, datetime) and vt is not None: vt = None
        if isinstance(vt, datetime): from info import VERIFY_EXPIRE; base = vt.replace(tzinfo=timezone.utc) if vt.tzinfo is None else vt; verify['expire_time'] = base + timedelta(seconds=VERIFY_EXPIRE)
        else: verify['expire_time'] = datetime.fromtimestamp(0, tz=timezone.utc) if verify.get('is_verified') else datetime.fromtimestamp(0, tz=timezone.utc)
    return verify.copy()

async def update_verify_status(user_id, vt="", iv=False, lnk="", et=None): # Use shorter var names
    user_id = int(user_id); current = await get_verify_status(user_id) # Async wrapper
    current['verify_token'] = vt; current['is_verified'] = iv; current['link'] = lnk
    if isinstance(et, datetime): current['expire_time'] = et
    if iv and (not isinstance(current.get('verified_time'), datetime) or not current.get('is_verified')): current['verified_time'] = datetime.now(timezone.utc)
    temp.VERIFICATIONS[user_id] = current.copy()
    loop = asyncio.get_running_loop(); await loop.run_in_executor(None, db.update_verify_status, user_id, current) # Wrap sync

async def get_settings(group_id):
    group_id = int(group_id); settings = temp.SETTINGS.get(group_id)
    if not settings: loop = asyncio.get_running_loop(); settings = await loop.run_in_executor(None, db.get_settings, group_id); temp.SETTINGS[group_id] = settings # Wrap sync
    return settings.copy() if settings else db.default_setgs.copy()

async def save_group_settings(group_id, key, value):
    group_id = int(group_id); current = await get_settings(group_id) # Async wrapper
    current[key] = value; temp.SETTINGS[group_id] = current.copy()
    loop = asyncio.get_running_loop(); await loop.run_in_executor(None, db.update_settings, group_id, current) # Wrap sync

async def broadcast_messages(uid, msg, pin): # Async OK
    try: m = await msg.copy(uid); await m.pin(True) if pin else None; return "Success"
    except FloodWait as e: logger.warning(f"Flood user {uid}: {e.value}"); await asyncio.sleep(e.value); return await broadcast_messages(uid, msg, pin)
    except Exception as e: logger.error(f"Bcast user err {uid}: {e}"); db.delete_user(int(uid)); return "Error"

async def groups_broadcast_messages(cid, msg, pin): # Async OK
    try: k = await msg.copy(cid); await k.pin(True) if pin else None; return "Success"
    except FloodWait as e: logger.warning(f"Flood group {cid}: {e.value}"); await asyncio.sleep(e.value); return await groups_broadcast_messages(cid, msg, pin)
    except Exception as e: logger.error(f"Bcast group err {cid}: {e}"); db.delete_chat(cid); return "Error"

def get_size(b): # Sync OK
    if b is None or not isinstance(b,(int,float)) or b<0: return "0 B"; s=float(b); u=["B","KB","MB","GB","TB"]; i=0
    while s>=1024.0 and i<len(u)-1: i+=1; s/=1024.0; return "%.2f %s"%(s,u[i])

async def get_shortlink(url, api, link): # Async OK
    if not url or not api: logger.warning("Shortlink conf miss."); return link
    try: sz = Shortzy(api, url); sl = await sz.convert(link)
    except Exception as e: logger.error(f"Shortzy Err: {e}"); return link
    return sl if sl and isinstance(sl, str) and sl.startswith(('http:','https:')) else link

def get_readable_time(s): # Sync OK
    if s is None or not isinstance(s,(int,float)) or s<0: return "0s"; s=int(s); r=''; p=[('d',86400),('h',3600),('m',60),('s',1)]
    for n, sec in p: if s>=sec: v, s = divmod(s, sec); r+=f'{v}{n}'
    return r if r else '0s'

def get_wish(): # Sync OK
    try: now = datetime.now(pytz.timezone(TIME_ZONE)); h = int(now.strftime("%H"))
    except Exception as e: logger.error(f"Wish TZ err: {e}"); h = datetime.now().hour
    if 5<=h<12: return "ɢᴏᴏᴅ ᴍᴏʀɴɪɴɢ ☀️"; elif 12<=h<18: return "ɢᴏᴏᴅ ᴀғᴛᴇʀɴᴏᴏɴ 🌤️"; else: return "ɢᴏᴏᴅ ᴇᴠᴇɴɪɴɢ 🌙"

def get_seconds(ts): # Sync OK
    m = re.match(r"(\d+)\s*(s|sec|m|min|h|hr|d|day|w|week|month|y|year)$",str(ts).lower().strip(),re.I)
    if not m: return 0; v=int(m.group(1)); u=m.group(2)
    if u.startswith('s'): return v; elif u.startswith('m') and "month" not in u: return v*60; elif u.startswith('h'): return v*3600; elif u.startswith('d'): return v*86400; elif u.startswith('w'): return v*86400*7; elif u.startswith('month'): return v*86400*30; elif u.startswith('y'): return v*86400*365; else: return 0
