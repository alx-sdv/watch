from os import path
from platform import platform
from time import sleep

from cx_Oracle import DatabaseError, OperationalError, clientversion
from flask import abort, flash, redirect, render_template, request, send_file, session, url_for

from watch import active_connections, app, bot, lock, notification_pool, startup_time, target_pool, task_pool, \
    unsent_pool, worker
from watch.utils.decorate_view import *
from watch.utils.manage_task import cancel_task, reset_task, store_tasks


@app.route('/login', methods=['GET', 'POST'])
@title('Login')
def login():
    if not app.config['TARGETS'] or not app.config['USERS']:
        flash('It seems the app is not configured.')
    if request.method == 'GET':
        if 'user_name' in session:
            return redirect(request.args.get('link', url_for('get_welcome_page')))
        else:
            return render_template('login.html')
    if request.method == 'POST':
        if request.form['name'] and request.form['password']:
            user = app.config['USERS'].get(request.form['name'].lower())
            if user is None or user[0] != request.form['password']:
                flash('Incorrect login or password')
                return render_template('login.html')
            else:
                session['user_name'] = request.form['name'].lower()
                session.permanent = app.config['PERMANENT_USER_SESSION']
                return redirect(request.args.get('link', url_for('get_welcome_page')))
        else:
            return render_template('login.html')


@app.route('/')
def get_welcome_page():
    return render_template('about.html')


@app.route('/get_user')
def get_user():
    return render_template('layout.html'
                           , text=f'Hello, {session["user_name"]}! Someday you will see here your profile settings...')


@app.route('/adm')
@title('Administration')
def get_app():
    info = [('Startup time', startup_time.strftime(app.config['DATETIME_FORMAT']))
            , ('Oracle client version', '.'.join((str(x) for x in clientversion())))
            , ('OS version ', platform())]
    with lock:
        if target_pool:
            info.append(('Session pools ', ', '.join(target_pool.keys())))

        info.append(('Task worker', 'ON' if worker.is_alive() else 'OFF'))
        info.append(('Chat bot', 'ON' if bot.is_alive() else 'OFF'))

        if app.config['DND_HOURS']:
            info.append(('Do not disturb hours', f"from {app.config['DND_HOURS'][0]}:00"
                                                 f" to {app.config['DND_HOURS'][1]}:00"))

        t = render_template('administration.html'
                            , info=info
                            , active_connections=active_connections
                            , task_pool=task_pool
                            , task_id=request.args.get('task_id', ''))
    return t


@app.route('/cancel_sql')
def cancel_sql():
    try:
        with lock:
            active_connections[request.args['id']][5] = 'Cancelling...'
            active_connections[request.args['id']][0].cancel()
    except KeyError:
        pass
    sleep(1)
    return redirect(url_for('get_app'))


@app.route('/task/<action>')
@title('Manage task')
def manage_task(action):  # TODO: divide to 3 views
    if action == 'browse':
        if not task_pool.get(request.args['id']):
            abort(404)
        return render_template('layout.html', formatted_text=task_pool[request.args['id']])
    with lock:
        if action == 'reset_all':
            reset_task(task_pool)
        elif not task_pool.get(request.args['id']):
            abort(404)
        elif task_pool[request.args['id']].state == 'run':
            flash(f'Can\'t {action} an active task.')
        elif action == 'cancel':
            if session['user_name'] != task_pool[request.args['id']].user_name \
                    and session['user_name'] not in app.config['ADMIN_GROUP']:
                flash('You must be an admin to cancel other users\' task.')
            else:
                cancel_task(task_pool, request.args['id'])
        elif action == 'reset':
            reset_task(task_pool, request.args['id'])
        else:
            abort(400)
    return redirect(url_for('get_app'))


@app.route('/logout')
@title('Log out')
def logout():
    session.pop('user_name', None)
    return redirect(url_for('login'))


@app.route('/stop_server')
@title('Shutdown server')
def stop_server():
    with lock:
        app.config['TARGETS'].clear()
        app.config['USERS'].clear()
    if worker.is_alive():
        worker.shutdown()
        worker.join()
    if bot.is_alive():
        bot.shutdown()
        bot.join()
    store_tasks(task_pool)
    with lock:
        for c in active_connections:
            try:
                c.cancel()
            except (DatabaseError, OperationalError):
                pass
    f = request.environ.get('werkzeug.server.shutdown')
    if f:
        f()
        return 'Good bye.'
    elif request.environ.get('uwsgi.version'):
        import uwsgi
        uwsgi.stop()
        return 'Good bye.'
    else:
        return 'Web server does not recognized, kill it manually.'


@app.route('/error_log')
@title('View error log')
def get_error_log():
    file = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'logs', app.config['ERROR_LOG_NAME'])
    if not path.exists(file):
        abort(404)
    return send_file(file, mimetype='text/plain', cache_timeout=0)


@app.route('/access_log')
@title('View access log')
def get_access_log():
    file = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'logs', app.config['ACCESS_LOG_NAME'])
    if not path.exists(file):
        abort(404)
    return send_file(file, mimetype='text/plain', cache_timeout=0)


@app.route('/notifications')
@title('Tasks notifications')
@columns({'time': 'datetime'
          , 'uuid': 'str'
          , 'task': 'str'
          , 'message': 'str'})
def get_notifications():
    with lock:
        task_count = len(tuple(1 for v in task_pool.values() if v.state in ('wait', 'run')))
        t = render_template('static_list.html'
                            , text=f'{task_count} tasks are active. '
                                   f'Only last {app.config["MAX_KEPT_NOTIFICATIONS"]} task messages will be kept.'
                            , data=notification_pool)
    return t


@app.route('/unsent')
@title('Unsent messages')
@columns({'time': 'datetime'
          , 'uuid': 'str'
          , 'task': 'str'
          , 'chat_id': 'str'
          , 'reply_to': 'str'
          , 'message': 'str'})
def get_unsent_messages():
    with lock:
        t = render_template('static_list.html'
                            , text=f'These task messages were not sent due to network problems.'
                            , data=unsent_pool)
    return t


@app.route('/<target>/get_ext')
@title('Extensions')
def get_ext(target):
    return render_template('layout.html')


@app.route('/<target>/search')
@title('Search')
def search(target):
    text = request.args['text'].replace(' ', '')
    if len(text) <= 4 and text.isdigit():
        return redirect(url_for('get_session', target=target, sid=text))
    else:
        return redirect(url_for('get_query', target=target, query=text))
