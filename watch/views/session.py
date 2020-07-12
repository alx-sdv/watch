from flask import redirect, url_for

from watch import app
from watch.utils.decorate_view import *
from watch.utils.render_page import render_page


@app.route('/<target>/S/<sid>')
@title('Session')
@template('single')
@columns({"sid": 'int'
          , "serial#": 'int'
          , "username": 'str'
          , "status": 'str'
          , "schemaname": 'str'
          , "osuser": 'str'
          , "machine": 'str'
          , "terminal": 'str'
          , "program": 'str'
          , "type": 'str'
          , "sql_id": 'str'
          , "sql_child_number": 'int'
          , "sql_exec_start": 'datetime'
          , "prev_sql_id": 'str'
          , "prev_child_number": 'int'
          , "prev_exec_start": 'datetime'
          , "module": 'str'
          , "action": 'str'
          , "logon_time": 'datetime'
          , "event": 'str'
          , "wait_class": 'str'
          , "seconds_in_wait": 'int'
          , "state": 'str'
          , "blocking_session": 'int'
          , "blocking_session_status": 'str'
          , "final_blocking_session": 'int'})
@select("v$session where sid = :sid")
def get_session(target, sid):
    return render_page()


@app.route('/<target>/S/<sid>/notify_if_inactive')
@title('Notify if inactive')
def notify_if_inactive(target, sid):
    return redirect(url_for('wait_for_session', target=target, sid=sid))


@app.route('/<target>/S/<sid>/session_stats')
@title('Session stats')
@template('list')
@auto()
@columns({"decode(n.class, 1, 'User', 2, 'Redo', 4, 'Enqueue', '8', 'Cache', 16, 'OS',"
          " 32, 'Real Application Clusters', 64, 'SQL', 128, 'Debug') stat_class": 'str'
          , "n.name": 'str'
          , "s.value": 'int'})
@select("v$sesstat s join v$statname n on s.statistic# = n.statistic# where s.sid = :sid")
@default_filters("", "name like '%memory%'")
@default_sort("value desc")
def get_session_stats(target, sid):
    return render_page()
