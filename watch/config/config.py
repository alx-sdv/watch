from datetime import timedelta
from os import path
from random import choice
from string import ascii_letters, digits, punctuation

from pygal import Config
from pygal.style import Style

from watch.config.menu import menu_tree

# Please create local_config.py file and set all [REQUIRED] parameters.
# #############################################################################################

# Flask debug. Turning it to True leads to unexpected behaviour: internal threads will be started twice.
DEBUG = False

# Flask secret key. Set this key to a fixed value to keep user sessions valid even if your server is restarted.
SECRET_KEY = ''.join([choice(ascii_letters + digits + punctuation) for n in range(32)])

# [REQUIRED]
# On production Set host to 0.0.0.0, choose port and proper server_name (must include port, mycompany.com:8181).
# It is possible to use default local settings, buuuuut.....
# Please note:
#     1) Chrome does not send a cookie to localhost, so you may not be authenticated.
#     2) Telegram does not recognize local hyperlinks.
# That's why it is recommended to use "global" settings on a local machine.
# See the official Flask docs to learn more about these params.
HOST = '127.0.0.1'  # [REQUIRED]
PORT = 5000
SERVER_NAME = None
CUSTOM_SERVER_NAME = None

# How much time client browser should keep our cookies.
PERMANENT_USER_SESSION = True
PERMANENT_SESSION_LIFETIME = timedelta(days=7)

# Limit parallel queries count for each user
MAX_DB_SESSIONS_PER_USER = 20

# How many rows can be fetched
ORA_NUM_ROWS = 100_000

# Limit parallel session count for each target
ORA_MAX_POOL_SIZE = 40

# Datetime display format
DATETIME_FORMAT = '%d.%m.%Y %H:%M:%S'

# The main menu structure. Can be imported from other source.
MENU_TREE = menu_tree

# Logger params
LOG_MAX_BYTES = 1024 * 1024
LOG_BACKUP_COUNT = 3
ERROR_LOG_NAME = 'error.log'
ACCESS_LOG_NAME = 'access.log'
ENABLE_ACCESS_LOG = False

# Background task worker tries to process all active tasks, then sleep for this period (seconds).
# 0 value means that the worker will not be started. So you can turn it off if you are not going to create tasks.
WORKER_FREQ_SEC = 10  # set to 0 to turn it off

# Limit the maximum number of notifications to show in "Tasks notifications" view.
MAX_KEPT_NOTIFICATIONS = 100

# All active tasks can be stored to disk before the server shutdown.
# Set it to '' to disable task storing.
STORE_FILE = path.join(path.dirname(__file__), 'stored_tasks')

# Each task remembers sent warnings and doesn't repeat it twice.
# This option limits the maximum number of database objects which the task marks as sent.
# When the limit will be exceeded the oldest object removing (FIFO), even if it's warning is actual.
MAX_STORED_OBJECTS = 1000

# If the task message is a list, this parameter limits it's length.
MAX_MESSAGE_ITEMS = 10

# Your telegram bot name and token.
# These params must be set to use messaging (sending notifications and receiving commands)
# See the official telegram docs to know how to create your own bot.
BOT_NAME = ''
BOT_TOKEN = ''
BOT_PATH = 'https://api.telegram.org/bot'

# If you have to use a proxy put it here. For example {'https': 'https://127.0.0.1:81'}
BOT_PROXY = {}
# For proxy servers which accept original address as /path
BOT_SIMPLE_PROXY = ''

# Watch server receives chat messages via long polls.
# Set this parameter to 60 (seconds) or even more. if you are going to use a chat bot.
# Please note: there is no reason to use extremely small values for this parameter.
# 0 means that the bot is turned off (no income messages).
BOT_POLLING_FREQ_SEC = 0

# User can choose one of these chats while creating a task. All the task notifications will be sent to selected chat.
# Example: {-123: 'Critical alerts', -124:'Other alerts'}.
# You must get -123 and -124 values from telegram:
#     1) Create your own bot.
#     2) Set bot params mentioned above.
#     3) Start the app.
#     4) Create a new group.
#     5) Add the bot to your new group.
#     6) Send /id command.
#     7) The bot will show you group id.
BOT_CHAT_LIST = {}

# Don't send anything, ignore notifying options for tasks. Turn WORKER_FREQ_SEC to 0 to stop task processing.
MUTE_MESSAGES = False

# Do not perform task till reset if previous message was not sent.
FAIL_TASK_ON_MSG_ERROR = True

# Do not disturb hours [from, to].
# Tasks with `default` sound mode will send silent notifications. Examples: [22, 7] or [0, 8]
DND_HOURS = []

# Pygal charts configuration. See the official Pygal docs.
CHART_CONFIG = {'style': Style(font_family='Arial'
                               , guide_stroke_dasharray='1,1'
                               , major_guide_stroke_dasharray='1,1'
                               , label_font_size=12
                               , major_label_font_size=12
                               , value_font_size=12
                               , value_label_font_size=12
                               , legend_font_size=12
                               , background='#FFFFFF'
                               , plot_background='#FFFFFF'
                               , title_font_family='Arial'
                               , title_font_size=12)
                , 'explicit_size': True
                , 'height': 400
                , 'width': 1000
                , 'margin': 4
                , 'show_x_guides': True
                , 'tooltip_border_radius': 2
                , 'dots_size': 2
                , 'stroke_style': {'width': 1}}


# [REQUIRED]
# Here is our targets. Each target describes an Oracle DB connection.
TARGETS = {
    # 'OUR-DEV': {'host': '127.0.0.1',
    #             'port': '0000',
    #             'sid': 'SID',
    #             'encoding': 'windows-1251',
    #             'user': 'username',
    #             'password': 'userpassword'}
    # , ...
    }

# [REQUIRED]
# Add users to the system.
# key = login (str), must be in a lowercase.
# value[0] = password (str).
# value[1] = telegram account id (int).
# user should send you id, to be able to communicate with the bot in private chat.
# if it is not necessary, set to None
# user can send /id command to your bot in private chat to find out it's id.
# value[2] = list of targets (str) allowed to user.
USERS = {
  # 'admin': ['p#ssw0rd', 123, ['OUR-DEV', ...]],
  # 'guest': ['psswrOrd', None, ['OUR-DEV', ...]]
  }

# [REQUIRED]
# List of users, which allowed to:
# - shutdown the app server;
# - remove other users' tasks.
ADMIN_GROUP = []
ADMIN_ONLY_VIEWS = ['get_access_log', 'get_error_log', 'stop_server']

# If your custom view is specific for some target it will not be shown for other targets.
# {view_name: [target_name1, target_name2, ...], ...}
TARGET_SPECIFIC_VIEWS = {}

# Now let's try to import settings which you have set in local_config.py
try:
    from watch.config.local_config import *
except ImportError:
    pass

# Finishing touch
CHART_CONFIG['config'] = Config(js=[f'http://{SERVER_NAME or CUSTOM_SERVER_NAME or (HOST + ":" + str(PORT))}'
                                    f'/static/pygal-tooltips.min.js'])

# That's all. Now try to start the app. Good luck!
