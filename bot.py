import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', # Added module name
    handlers=[logging.StreamHandler()]
)
logging.getLogger('hydrogram').setLevel(logging.WARNING) # Changed to WARNING to see more hydro errors if needed
logger = logging.getLogger(__name__)

import os
import time
import asyncio
import uvloop
import threading # For ping thread
import requests  # For ping thread
from hydrogram import types
from hydrogram import Client
from hydrogram.errors import FloodWait
from aiohttp import web
from typing import Union, Optional, AsyncGenerator
from web import web_app # Assuming web_app is defined in web/__init__.py
# Removed check_premium import
from info import (INDEX_CHANNELS, SUPPORT_GROUP, LOG_CHANNEL, API_ID, DATA_DATABASE_URL,
                  API_HASH, BOT_TOKEN, PORT, BIN_CHANNEL, ADMINS,
                  SECOND_FILES_DATABASE_URL, FILES_DATABASE_URL, URL) # Added URL
from utils import temp, get_readable_time # Removed check_premium
from database.users_chats_db import db
# Removed pymongo imports if not used directly here

uvloop.install()

class Bot(Client):
    def __init__(self):
        super().__init__(
            name='Auto_Filter_Bot', # Consider using SESSION variable from info.py if defined
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            plugins={"root": "plugins"},
            # Consider adding workers parameter if needed: workers=4
        )

    async def start(self):
        await super().start()
        temp.START_TIME = time.time()
        b_users, b_chats = await db.get_banned() # Ensure get_banned is async or handled properly
        temp.BANNED_USERS = b_users
        temp.BANNED_CHATS = b_chats
        temp.BOT = self # Store bot instance

        # Restart message handling remains
        if os.path.exists('restart.txt'):
            try:
                with open("restart.txt") as file:
                    chat_id, msg_id = map(int, file)
                await self.edit_message_text(chat_id=chat_id, message_id=msg_id, text='✅ Restarted Successfully!')
                os.remove('restart.txt')
            except Exception as e:
                logger.error(f"Error handling restart message: {e}")

        # Get bot info
        me = await self.get_me()
        temp.ME = me.id
        temp.U_NAME = me.username
        temp.B_NAME = me.first_name

        # Start web server for keepalive/streaming
        try:
            app = web.AppRunner(web_app)
            await app.setup()
            # Use PORT from info.py
            await web.TCPSite(app, "0.0.0.0", PORT).start()
            logger.info(f"Web server started successfully on port {PORT}.")
        except Exception as e:
            logger.error(f"Error starting web server: {e}", exc_info=True)
            # Decide if bot should exit or continue without web server
            # exit() or pass

        # Remove premium check task
        # asyncio.create_task(check_premium(self))

        # Log bot start
        try:
            startup_msg = f"<b>✅ {me.mention} Restarted!</b>"
            # Add version or other info if desired
            await self.send_message(chat_id=LOG_CHANNEL, text=startup_msg)
        except Exception as e:
            logger.error(f"Bot could not send startup message to LOG_CHANNEL {LOG_CHANNEL}. Error: {e}")
            logger.warning("Please ensure the bot is an admin in the log channel.")
            # Consider exiting if log channel is critical
            # exit()

        logger.info(f"@{me.username} is started successfully. ✓")

    async def stop(self, *args):
        logger.info("Stopping bot...")
        await super().stop()
        logger.info("Bot Stopped! Bye...")

    # iter_messages remains the same
    async def iter_messages(self: Client, chat_id: Union[int, str], limit: int, offset: int = 0) -> Optional[AsyncGenerator["types.Message", None]]:
        current = offset
        while True:
            new_diff = min(200, limit - current) # Batch size
            if new_diff <= 0: return
            try:
                 messages = await self.get_messages(chat_id, list(range(current, current + new_diff))) # Fetch IDs in range
                 if not messages: return # Stop if no messages found (end of chat)
            except FloodWait as e:
                 logger.warning(f"FloodWait in iter_messages: sleeping for {e.value}s")
                 await asyncio.sleep(e.value)
                 continue # Retry same batch
            except Exception as e:
                 logger.error(f"Error in iter_messages for chat {chat_id}: {e}")
                 return # Stop iteration on error

            for message in messages:
                if message is None: continue # Skip if get_messages returned None for an ID
                yield message
                current = message.id # Move to the ID of the last successfully processed message

# --- Keepalive Ping Thread ---
def ping_loop():
    ping_interval = 180 # Ping every 3 minutes
    while True:
        try:
            if not URL: # Don't ping if URL isn't set
                 logger.debug("Keepalive ping skipped: URL not configured.")
                 time.sleep(ping_interval * 2) # Check less frequently if URL isn't set
                 continue

            ping_url = URL if URL.endswith('/') else URL + '/'
            logger.info(f"Pinging URL: {ping_url}")
            r = requests.get(ping_url, timeout=20) # Slightly longer timeout
            if r.status_code == 200:
                logger.info(f"Keepalive ping successful ✅ (Status: {r.status_code})")
            else:
                logger.error(f"Keepalive ping failed: {r.status_code} ⚠️ - {r.text[:200]}")
        except requests.exceptions.Timeout:
            logger.error("Keepalive ping timed out ❌")
        except requests.exceptions.RequestException as e:
            logger.error(f"Keepalive ping exception: {e} ❌")
        except Exception as e:
            logger.error(f"Unexpected error during ping: {e} ❌", exc_info=True) # Log full traceback
        finally:
             # Wait for the interval regardless of success or failure
             time.sleep(ping_interval)

# --- Start Bot ---
if __name__ == "__main__":
    try:
        # Start the ping loop in a separate daemon thread
        threading.Thread(target=ping_loop, daemon=True, name="PingThread").start()

        app = Bot()
        app.run()
    except Exception as e:
        logger.critical(f"Critical error during bot startup or runtime: {e}", exc_info=True)
        # Optional: Add cleanup or specific exit codes here
