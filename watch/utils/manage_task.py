from collections import deque
from datetime import datetime
from os import path
from pickle import HIGHEST_PROTOCOL, dump as pickle, load as unpickle
from pprint import pformat
from uuid import uuid4

from watch import app, lock


class Task:
    def __init__(self,
                 uuid=None, endpoint=None, name=None, create_date=None, user_name=None, target=None, last_call=None,
                 execs=None, state=None, parameters=None, period=None, chat_id=None, reply_to_message_id=None,
                 data=None, optional=None, priority=None, finished=None, text=None, sound=None, duration=None):
        self.uuid = uuid or uuid4().hex
        self.endpoint = endpoint
        self.name = name or getattr(app.view_functions[endpoint], 'title', 'Task')
        self.create_date = create_date or datetime.now()
        self.user_name = user_name
        self.target = target
        self.last_call = last_call
        self.execs = execs or 0
        self.state = 'wait' if (state is None or state == 'run') else state  # if an exception has happened
        self.parameters = parameters
        self.period = period or app.view_functions[endpoint].period
        self.chat_id = chat_id
        self.reply_to_message_id = reply_to_message_id
        self.data = data
        self.optional = optional
        self.priority = priority
        self.finished = finished or False
        self.text = text
        self.sound = sound
        self.duration = duration

    def __str__(self):
        return pformat(self.__dict__, width=160).replace('\'', '').replace(',\n', '\n')

    def finish(self, message):
        self.finished = True
        return message

    def abort(self, message):
        self.finished = True
        return '❎ ' + message

    def get_message(self, result, message_item, header=None, n=None, k=None):
        message_type = getattr(app.view_functions[self.endpoint], 'message_type', '')  # list, outstanding, threshold
        if not message_type:
            return
        if message_type == 'threshold':
            if not self.data or (result[0] or 0) < n :
                self.data = n
            if result[0] >= self.data:
                self.data = round(result[0] * k)
                return message_item(self, str(result[0]))
            return
        if not result:
            if message_type == 'outstanding':
                self.data = None
            return
        if message_type == 'outstanding':
            if self.data is None:
                self.data = deque(maxlen=app.config['MAX_STORED_OBJECTS'])
            else:
                for item in self.data.copy():
                    if item not in [r_item[n] for r_item in result]:
                        self.data.remove(item)
            result = [item for item in result if item[n] not in self.data]
            if not result:
                return
        message_text = header(self) + ':\n' if header else ''
        max_count = app.config['MAX_MESSAGE_ITEMS']
        message_text += '\n'.join(message_item(self, item) for item in result[:max_count - 1])
        if len(result) > max_count:
            message_text += f'\n and {str(len(result) - max_count)} more...'
        if message_type == 'outstanding':
            for item in result:
                self.data.appendleft(item[n])
        return message_text


def cancel_task(task_pool, uuid):
    try:
        del task_pool[uuid]
    except KeyError:
        pass


def reset_task(task_pool, uuid=None):
    with lock:
        if uuid:
            try:
                if task_pool[uuid].state.endswith('error'):
                    task_pool[uuid].state = 'wait'
            except KeyError:
                pass
        else:
            for task in task_pool.values():
                if task.state.endswith('error'):
                    task.state = 'wait'


def store_tasks(task_pool):
    with lock:
        if app.config['STORE_FILE']:
            with open(app.config['STORE_FILE'], 'wb') as f:
                pickle({k: [*v.__dict__.values()] for k, v in task_pool.items()}, f, HIGHEST_PROTOCOL)
                return True
        else:
            return False


def restore_tasks():
    if app.config['STORE_FILE'] and path.exists(app.config['STORE_FILE']):
        with open(app.config['STORE_FILE'], 'rb') as f:
            return {k: Task(*v) for k, v in unpickle(f).items()}
    return {}
