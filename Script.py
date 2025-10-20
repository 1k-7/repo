class script(object):

    START_TXT = """<b><blockquote>ʜᴇʏ {}, <i>{}</i></blockquote>
    
ɪ'ᴍ ᴀ ᴘᴏᴡᴇʀꜰᴜʟ ᴀᴜᴛᴏꜰɪʟᴛᴇʀʙᴏᴛ ᴅᴇꜱɪɢɴᴇᴅ ᴛᴏ ᴘʀᴏᴠɪᴅᴇ ᴍᴏᴠɪᴇꜱ & ꜱᴇʀɪᴇꜱ. ᴊᴏɪɴ ᴏᴜʀ ɢʀᴏᴜᴘꜱ ᴛᴏ ɢᴇᴛ ʟᴀᴛᴇꜱᴛ ᴍᴏᴠɪᴇꜱ ᴀɴᴅ ꜱᴇʀɪᴇꜱ ✅👇

<blockquote>Jᴏɪɴ : @norFederation Fᴏʀ Uᴘᴅᴀᴛᴇꜱ ✅</blockquote></b>"""

    MY_ABOUT_TXT = """★ Server: <a href=https://www.heroku.com>Heroku</a>
★ Database: <a href=https://www.mongodb.com>MongoDB</a>
★ Language: <a href=https://www.python.org>Python</a>
★ Library: <a href=https://t.me/HydrogramNews>Hydrogram</a>"""

    MY_OWNER_TXT = """★ Name: Zach
★ Username: @actualhomie
★ Country: India 🇮🇳"""

    STATUS_TXT = """👤 Total Users: <code>{}</code>
😎 Users: <code>{}</code>
👥 Total Chats: <code>{}</code>
🗳 Data database used: <code>{}</code>

🗂 1st database Files: <code>{}</code>
🗳 1st files database used: <code>{}</code>

🗂 2nd database Files: <code>{}</code>
🗳 2nd files database used: <code>{}</code>

🚀 Bot Uptime: <code>{}</code>"""

    NEW_GROUP_TXT = """#NewGroup
Title - {}
ID - <code>{}</code>
Username - {}
Total - <code>{}</code>"""

    NEW_USER_TXT = """#NewUser
★ Name: {}
★ ID: <code>{}</code>"""

    NOT_FILE_TXT = """👋 Hello {},

I can't find the <b>{}</b> in my database! 🥲

👉 Google Search and check your spelling is correct.
👉 Please read the Instructions to get better results.
👉 Or not been released yet."""
    
    IMDB_TEMPLATE = """✅ I Found: <code>{query}</code>

🏷 Title: <a href={url}>{title}</a>
🎭 Genres: {genres}
📆 Year: <a href={url}/releaseinfo>{year}</a>
🌟 Rating: <a href={url}/ratings>{rating} / 10</a>
☀️ Languages: {languages}
📀 RunTime: {runtime} Minutes

🗣 Requested by: {message.from_user.mention}
©️ Powered by: <b>{message.chat.title}</b>"""

    FILE_CAPTION = """<i>{file_name}</i>

🚫 ᴘʟᴇᴀsᴇ ᴄʟɪᴄᴋ ᴏɴ ᴛʜᴇ ᴄʟᴏsᴇ ʙᴜᴛᴛᴏɴ ɪꜰ ʏᴏᴜ ʜᴀᴠᴇ sᴇᴇɴ ᴛʜᴇ ᴍᴏᴠɪᴇ 🚫"""

    WELCOME_TEXT = """👋 Hello {mention}, Welcome to {title} group! 💞"""

    HELP_TXT = """👋 Hello {},
    
I can filter movie and series you want
Just type you want movie or series in my PM or adding me in to group
And i have more feature for you
Just try my commands"""

    ADMIN_COMMAND_TXT = """<b>Here is bot admin commands 👇


/index_channels - to check how many index channel id added
/stats - to get bot status
/delete - to delete files using query
/delete_all - to delete all indexed file
/broadcast - to send message to all bot users
/grp_broadcast - to send message to all groups
/pin_broadcast - to send message as pin to all bot users.
/pin_grp_broadcast - to send message as pin to all groups.
/restart - to restart bot
/leave - to leave your bot from particular group
/users - to get all users details
/chats - to get all groups
/invite_link - to generate invite link
/index - to index bot accessible channels
/delreq - to delete join request in db (if change REQUEST_FORCE_SUB_CHANNELS using /set_req_fsub then must need use this command)
/set_req_fsub - to set request force subscribe channel
/set_fsub - to set force subscribe channels</b>"""
    
    PLAN_TXT = """Activate any premium plan to get exclusive features.

You can activate any premium plan and then you can get exclusive features.

- INR {} for pre day -

Basic premium features:
Ad free experience
Online watch and fast download
No need joind channels
No need verify
No shortlink
Admins support
And more...

Support: {}"""

    USER_COMMAND_TXT = """<b>Here is bot user commands 👇

/start - to check bot alive or not
/settings - to change group settings as your wish
/connect - to connect group settings to PM
/id - to check group or channel id</b>"""
    
    SOURCE_TXT = """<b>😆✅

- ᴛʜɪꜱ ʙᴏᴛ ɪꜱ ᴀɴ ᴏᴘᴇɴ ꜱᴏᴜʀᴄᴇ ᴘʀᴏᴊᴇᴄᴛ.

- ᴅᴇᴠʟᴏᴘᴇʀ - @actualhomie"""

