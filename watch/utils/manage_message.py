from html import escape
from json import loads
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from watch import app


def send_message(parameters):
    url = '{}{}{}/{}?{}'.format(app.config['BOT_SIMPLE_PROXY'], app.config['BOT_PATH'], app.config['BOT_TOKEN']
                                , 'sendMessage', urlencode(parameters))
    try:
        with urlopen(url) as r:
            return 0 if loads(r.read().decode('utf-8')).get('ok', False) else -1
    except URLError as e:
        app.logger.error(f'messaging error: {e}')
        app.logger.error(f'client request: {url}')
        # app.logger.error(f"server response: {e.read().decode('utf-8')}")
        return -1


def get_updates(offset):
    return urlopen('{}{}/{}?{}'.format(app.config['BOT_PATH']
                                       , app.config['BOT_TOKEN']
                                       , 'getUpdates'
                                       , urlencode({'offset': offset
                                                    , 'timeout': app.config['BOT_TOKEN']})))


def t_esc(s):
    return escape(s)


def t_link(url_part, text):
    return f'<a href="http://{app.config["SERVER_NAME"] or app.config["CUSTOM_SERVER_NAME"]}' \
           f'/{t_esc(url_part)}">{t_esc(text)}</a>'


def t_pre(text):
    return f'<pre>{t_esc(text)}</pre>'


def t_italic(text):
    return f'<i>{t_esc(text)}</i>'
