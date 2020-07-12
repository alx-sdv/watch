from time import time

from flask import abort, flash, g, redirect, render_template, request, session, url_for

from watch import task_pool, title, view_attr, worker
from watch.utils.manage_task import Task
from watch.utils.parse_args import *


def validate_request():
    app.logger.info(f"{session.get('user_name', 'unknown')} {' '.join(request.access_route)} {request.full_path}")
    if 'favicon.ico' in request.url or 'apple-touch-icon' in request.url:
        return 'not today', 404
    elif not request.endpoint:
        abort(404)
    elif request.endpoint in ('login', 'static'):
        return None
    elif 'user_name' not in session:
        if request.url != request.url_root:
            return redirect(url_for('login', link=request.url))
        else:
            return redirect(url_for('login'))
    elif request.endpoint in app.config['ADMIN_ONLY_VIEWS'] and session['user_name'] not in app.config['ADMIN_GROUP']:
        abort(403)
    elif not request.view_args.get('target'):
        return None
    elif request.view_args['target'] in app.config['USERS'][session['user_name']][2]:
        return None
    else:
        abort(403)


def set_template_context():
    g.request_time = time()
    g.title = title
    for k, v in view_attr[request.endpoint].items():
        setattr(g, k, v)
    if g.template == 'task':
        g.notification_list = {0: 'Nobody'}
        r = app.config['USERS'][session['user_name']][1]
        if r:
            g.notification_list[r] = 'Me'
        g.notification_list.update(app.config['BOT_CHAT_LIST'])


def render_form():
    if getattr(app.view_functions[request.endpoint], 'template', '') == 'list':
        return render_list()
    elif getattr(app.view_functions[request.endpoint], 'template', '') == 'task':
        return render_task()
    else:
        return None


def render_list():
    f = app.view_functions[request.endpoint]
    if 'do' not in request.args and hasattr(f, 'auto'):
        p = {}
        p.update(request.view_args)
        if hasattr(f, 'default_filters') and len(f.default_filters) > 0:
            p['filter'] = f.default_filters[0]
        if hasattr(f, 'default_sort'):
            p['sort'] = f.default_sort
        p['do'] = ''
        return redirect(url_for(request.endpoint, **p))

    if 'do' not in request.args:
        return render_template('list.html')
    rf = rs = 0
    rr = ''
    if request.args.get('filter'):
        rf, g.filter_expr, g.filter_values = parse_filter_expr(request.args['filter'], f.columns)
        if rf:
            flash(f'Incorrect filter expression at char: {rf}')
    if request.args.get('sort'):
        rs, g.sort_expr = parse_sort(request.args['sort'], f.columns)
        if rs:
            flash(f'Incorrect sort expression at char: {rs}')
    if g.parameters:
        rr, g.required_values = parse_parameters(request.args, g.parameters)
        if rr:
            flash(f'Incorrect value for required parameter: {rr}')
    if rf or rs or rr:
        return render_template('list.html')
    else:
        return None


def render_task():
    if 'do' not in request.args:
        return render_template('task.html')
    rr, required_values = parse_parameters(request.args, g.parameters)
    if rr:
        flash(f'Incorrect value for required parameter: {rr}')
        return render_template('task.html')
    rr, optional = parse_parameters(request.args, g.optional, True)
    if rr:
        flash(f'Incorrect value for optional parameter: {rr}')
        return render_template('task.html')
    if not worker.is_alive():
        flash('Background task worker is inactive, please contact your system administrator')
        return render_template('task.html')
    period_value = None
    if request.args.get('period'):
        rr, period_value = parse_period(request.args['period'])
    if rr:
        flash(rr)
        return render_template('task.html')
    chat_id = None
    if request.args.get('notify', '') != '0' and request.args.get('notify', '') \
            in (str(k) for k in g.notification_list.keys()):
        chat_id = request.args['notify']
    try:
        priority = int(request.args.get('priority', '2'))
        if priority not in (1, 2, 3):
            raise ValueError
    except ValueError:
        flash('Incorrect priority')
        return render_template('task.html')
    sound = request.args.get('sound', 'default')
    if sound not in ('default', 'yes', 'no'):
        flash('Incorrect sound mode')
        return render_template('task.html')
    task = Task(endpoint=request.endpoint
                , user_name=session['user_name']
                , target=request.view_args.get('target', '')
                , parameters={k: v for k, v in required_values.items()}
                , chat_id=chat_id
                , period=period_value
                , optional=optional
                , priority=priority
                , sound=sound
                , text=request.args.get('text', None))
    task_pool[task.uuid] = task
    return render_template('task.html', uuid=task.uuid)
