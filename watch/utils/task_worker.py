import threading
from datetime import datetime
from time import sleep

from cx_Oracle import DatabaseError, OperationalError

from watch import app, lock, notification_pool, task_pool, unsent_pool
from watch.utils.chat_bot import send_message
from watch.utils.manage_message import t_italic
from watch.utils.parse_args import get_offset


def check_dnd_time():
    if not app.config['DND_HOURS']:
        return False
    start_dnd_hour = app.config['DND_HOURS'][0]
    end_dnd_hour = app.config['DND_HOURS'][1]
    now_hour = datetime.now().hour
    if start_dnd_hour < end_dnd_hour and start_dnd_hour <= now_hour < end_dnd_hour:
        return True
    if start_dnd_hour >= end_dnd_hour and (now_hour >= start_dnd_hour or now_hour < end_dnd_hour):
        return True
    return False


def prepare_and_send(chat_id, reply_to_message_id, message, sound):
    message_parameters = {'chat_id': chat_id
                          , 'text': message
                          , 'parse_mode': 'HTML'
                          , 'disable_web_page_preview': 'true'}
    if reply_to_message_id:
        message_parameters['reply_to_message_id'] = reply_to_message_id
    if sound == 'no':
        message_parameters['disable_notification'] = 'true'
    elif sound == 'default':
        if check_dnd_time():
            message_parameters['disable_notification'] = 'true'
    return send_message(message_parameters)


class Worker(threading.Thread):
    def __init__(self):
        super(Worker, self).__init__()
        self.active = True

    def run(self):
        while self.active:
            # Now dnd means "send with no sound". Formerly tasks did were't processing at dnd hours.
            # if check_dnd_time():
            #     sleep(app.config['WORKER_FREQ_SEC'])
            #     continue

            with lock:
                active_tasks = tuple(t for t in sorted(task_pool.values()
                                                       , key=lambda x: x.priority) if t.state == 'wait')
            for task in active_tasks:
                with lock:
                    if not task_pool.get(task.uuid):
                        continue
                    if task.last_call:
                        pt = task.period[-1:]
                        pv = task.period[:-1]
                        next_call = task.last_call + get_offset(pv, pt)
                        if next_call > datetime.now():
                            continue
                    task.state = 'run'
                try:
                    task_start_time = datetime.now()
                    message = app.view_functions[task.endpoint](task)
                    r = 0
                    if message:
                        if task.text:
                            message = f'{t_italic(task.text)}\n{message}'
                        notification_pool.appendleft((datetime.now()
                                                      , task.uuid
                                                      , task.name
                                                      , message))
                        if task.chat_id and not app.config['MUTE_MESSAGES']:
                            r = prepare_and_send(task.chat_id, task.reply_to_message_id, message, task.sound)
                        if r != 0:
                            unsent_pool.appendleft((datetime.now()
                                                    , task.uuid
                                                    , task.name
                                                    , task.chat_id
                                                    , task.reply_to_message_id
                                                    , message))
                    if task.finished:
                        del task_pool[task.uuid]
                    else:
                        task.last_call = datetime.now()
                        task.duration = (task.last_call - task_start_time).seconds
                        task.execs += 1
                        task.state = 'wait' if r == 0 or not app.config['FAIL_TASK_ON_MSG_ERROR'] else 'msg error'
                    # retry sending even if prev msg had no recipient
                    if r == 0 and message and not app.config['MUTE_MESSAGES']:
                        while r == 0 and len(unsent_pool) > 0:
                            m = unsent_pool.popleft()
                            r = prepare_and_send(m[3]
                                                 , m[4]
                                                 , f'{t_italic("This message was postponed due to network problem")}'
                                                   f'\n{m[5]}'
                                                 , 'default')
                            if r == 0 and task_pool.get(m[1], None):
                                task_pool[m[1]].state = 'wait'
                            if r != 0:
                                unsent_pool.appendleft(m)
                except (DatabaseError, OperationalError) as e:
                    app.logger.error(f'{task.uuid} {e.args[0].message}')
                    task.state = 'db error'
                    if app.config['SLEEP_ON_FAIL_SEC'] > 0 and e.args[0].code in (12170, 3113):
                       app.logger.error(f"Sleeping for {app.config['SLEEP_ON_FAIL_SEC']} seconds...")
                       sleep(app.config['SLEEP_ON_FAIL_SEC'])
                       break
            sleep(app.config['WORKER_FREQ_SEC'])

    def shutdown(self):
        self.active = False
