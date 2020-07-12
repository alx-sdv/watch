from copy import deepcopy

from flask import flash, render_template, request, url_for
from pygal import HorizontalStackedBar, Pie, StackedLine

from watch import app
from watch.utils.decorate_view import *
from watch.utils.oracle import execute
from watch.utils.parse_args import parse_parameters


@app.route('/<target>/top')
@title('Top activity')
def get_top_activity(target):
    if 'do' not in request.args.keys():
        return render_template('top_activity.html')
    required_source = {'start_date': request.args.get('start_date', '-1h')
                       , 'end_date': request.args.get('end_date', '-0h')}
    required = {'start_date': 'datetime'
                , 'end_date': 'datetime'}
    error, required_values = parse_parameters(required_source, required)
    if error:
        flash(f'Incorrect value: {error}')
        return render_template('top_activity.html')
    optional_source = {'wait_class': request.args.get('wait_class', '')
                       , 'event': request.args.get('event', '')
                       , 'session_id': request.args.get('session_id', '')
                       , 'user_name': request.args.get('user_name', '')
                       , 'sql_id': request.args.get('sql_id', '')
                       , 'object_name': request.args.get('object_name', '')}
    _optional = {'wait_class': 'str'
                 , 'event': 'str'
                 , 'session_id': 'int'
                 , 'user_name': 'str'
                 , 'sql_id': 'str'
                 , 'object_name': 'str'}
    error, optional_values = parse_parameters(optional_source, _optional, True)
    if error:
        flash(f'Incorrect value: {error}')
        return render_template('top_activity.html')
    optional_values = {k: v for k, v in optional_values.items() if v}
    r = execute(target
                , "with h as (select sample_id, sample_time,"
                  " sql_id, o.object_name, event, event_id, user_id, session_id,"
                  " to_char(session_id) || ':' || to_char(session_serial#) sess"
                  ", nvl(wait_class, 'CPU') wait_class"
                  ", nvl(wait_class_id, -1) wait_class_id"
                  ", wait_time, time_waited from v$active_session_history ash"
                  " left join dba_objects o on o.object_id = ash.current_obj#"
                  " where sample_time >= trunc(:start_date, 'mi') and sample_time < trunc(:end_date, 'mi')"
                  " and sample_time > trunc(sysdate){}{}{}{}{}{})"
                  " select 1 t, to_char(sample_time, 'hh24:mi') s, wait_class v1, wait_class_id v2, count(1) c"
                  " from h group by to_char(sample_time, 'hh24:mi'), wait_class, wait_class_id union all"
                  " select 2 t, sql_id s, wait_class v1, wait_class_id v2, count(1) c from h"
                  " where sql_id is not null and sql_id in (select sql_id"
                  " from (select sql_id, row_number() over (order by tc desc) rn"
                  " from (select sql_id, count(1) tc from h"
                  " where sql_id is not null group by sql_id)) where rn <= 10)"
                  " group by sql_id, wait_class, wait_class_id union all"
                  " select 6 t, to_char(h.session_id) || ':' || nvl(u.username, '') s,"
                  " wait_class v1, wait_class_id v2, count(1) c from h"
                  " left join dba_users u on u.user_id = h.user_id"
                  " where sess in (select sess"
                  " from (select sess, row_number() over (order by tc desc) rn"
                  " from (select sess, count(1) tc from h"
                  " group by sess)) where rn <= 10)"
                  " group by to_char(h.session_id) || ':' || nvl(u.username, ''), wait_class, wait_class_id union all"
                  " select 3 t, object_name s, wait_class v1, wait_class_id v2, count(1) c from h"
                  " where object_name is not null and object_name in (select object_name"
                  " from (select object_name, row_number() over (order by tc desc) rn"
                  " from (select object_name, count(1) tc from h"
                  " where object_name is not null group by object_name))"
                  " where rn <= 10) group by object_name, wait_class, wait_class_id union all"
                  " select 4 t, null s, wait_class v1, wait_class_id v2, count(1) c"
                  " from h group by wait_class, wait_class_id union all"
                  " select 5 t, null s, event v1, event_id v2, count(1) c"
                  " from h group by event, event_id union all"
                  " select 7 t, to_char(sample_time, 'hh24:mi:ss') s, null v1, null v2, count(distinct session_id) c"
                  " from h group by to_char(sample_time, 'hh24:mi:ss') union all"
                  " select 8 t, null s, null v1, null v2, to_number(value) c"
                  " from v$parameter where name = 'cpu_count' union all"
                  " select 9 t, null s, null v1, null v2, to_number(value) c"
                  " from v$parameter where name = 'sessions' order by 1, 4, 2"
                  .format(" and nvl(wait_class, 'CPU') like :wait_class" if optional_values
                          .get('wait_class', '') else ""
                          , " and event like :event" if optional_values
                            .get('event', '') else ""
                          , " and session_id = :session_id" if optional_values
                          .get('session_id', '') else ""
                          , " and sql_id = :sql_id" if optional_values
                          .get('sql_id', '') else ""
                          , " and object_name like :object_name" if optional_values
                          .get('object_name', '') else ""
                          , " and user_id in (select user_id from dba_users "
                            "where username like :user_name)" if optional_values
                          .get('user_name', '') else ""
                          )
                , {**required_values, **optional_values})
    colors = {'Other': '#F06EAA'
              , 'Application': '#C02800'
              , 'Configuration': '#5C440B'
              , 'Administrative': '#717354'
              , 'Concurrency': '#8B1A00'
              , 'Commit': '#E46800'
              , 'Idle': '#FFFFFF'
              , 'Network': '#9F9371'
              , 'User I/O': '#004AE7'
              , 'System I/O': '#0094E7'
              , 'Scheduler': '#CCFFCC'
              , 'Queueing': '#C2B79B'
              , 'CPU': '#00CC00'}

    series = {k[1]: [] for k in sorted(set((item[3], item[2]) for item in r if item[0] == 1), key=lambda x: x[0])}
    p = deepcopy(app.config['CHART_CONFIG'])
    p['style'].colors = tuple(colors[wait_class] for wait_class in series.keys())
    p['height'] = 10 * 22
    session_count = max(tuple(item[4] for item in r if item[0] == 7) or (0,))
    session_limit = max(tuple(item[4] for item in r if item[0] == 9) or (0,))
    cpu_count = max(tuple(item[4] for item in r if item[0] == 8) or (0,))
    top_activity = StackedLine(**p
                               , legend_at_bottom=True
                               , legend_at_bottom_columns=len(series.keys())
                               , title=f'sessions(max): {session_count}, '
                                       f'sessions(limit): {session_limit}, '
                                       f'cpu cores: {cpu_count};')
    top_activity.fill = True
    top_activity.x_labels = sorted(set(item[1] for item in r if item[0] == 1))
    top_activity.x_labels_major_every = max(-(-len(top_activity.x_labels) // 20), 1)
    top_activity.truncate_label = 5
    top_activity.show_minor_x_labels = False
    for label in top_activity.x_labels:
        for serie in series.keys():
            v = tuple(item[4] for item in r if item[0] == 1 and item[1] == label and item[2] == serie)
            series[serie].append(v[0] if len(v) > 0 else 0)
    for serie in series.keys():
        top_activity.add(serie,  series[serie], show_dots=False)

    top_sql = HorizontalStackedBar(**p)
    top_sql.show_legend = False
    top_sql.width = 400
    top_sql.show_x_labels = False
    top_sql.x_labels = sorted(set(item[1] for item in r if item[0] == 2)
                              , key=lambda x: sum(tuple(item[4] for item in r if item[0] == 2 and item[1] == x)))
    top_sql.height = len(top_sql.x_labels) * 22
    series = {k[1]: [] for k in sorted(set((item[3], item[2]) for item in r if item[0] == 2), key=lambda x: x[0])}
    for label in top_sql.x_labels:
        for serie in series.keys():
            v = tuple(item[4] for item in r if item[0] == 2 and item[1] == label and item[2] == serie)
            series[serie].append(v[0] if len(v) > 0 else 0)
    for serie in series.keys():
        # todo https://github.com/Kozea/pygal/issues/18
        top_sql.add(serie,  [dict(value=item
                                  , color=colors[serie]
                                  , xlink=dict(href=url_for('get_query'
                                                            , target=target
                                                            , query=top_sql.x_labels[i]
                                                            , _external=True)
                                               , target='_blank')) for i, item in enumerate(series[serie])])

    top_objects = HorizontalStackedBar(**p)
    top_objects.show_legend = False
    top_objects.width = 400
    top_objects.show_x_labels = False
    top_objects.x_labels = sorted(set(item[1] for item in r if item[0] == 3)
                                  , key=lambda x: sum(tuple(item[4] for item in r if item[0] == 3 and item[1] == x)))
    series = {k[1]: [] for k in sorted(set((item[3], item[2]) for item in r if item[0] == 3), key=lambda x: x[0])}
    top_objects.height = len(top_objects.x_labels) * 22
    for label in top_objects.x_labels:
        for serie in series.keys():
            v = tuple(item[4] for item in r if item[0] == 3 and item[1] == label and item[2] == serie)
            series[serie].append(v[0] if len(v) > 0 else 0)
    for serie in series.keys():
        top_objects.add(serie,  [dict(value=item, color=colors[serie]) for item in series[serie]])

    pie_type = 5 if 'wait_class' in optional_values.keys() or 'event' in optional_values.keys() else 4
    top_waits = Pie(**p)
    top_waits.show_legend = False
    top_waits.width = 140
    top_waits.width = 140
    top_waits.inner_radius = 0.5
    labels = tuple(k[1] for k in sorted(set((item[3], item[2]) for item in r if item[0] == pie_type)
                                        , key=lambda x: x[0] if isinstance(x[0], int) else 0))
    for label in labels:
            top_waits.add(label, tuple(item[4] for item in r if item[0] == pie_type and item[2] == label)[0])

    top_sessions = HorizontalStackedBar(**p)
    top_sessions.show_legend = False
    top_sessions.width = 300
    top_sessions.show_x_labels = False
    top_sessions.x_labels = sorted(set(item[1] for item in r if item[0] == 6)
                                   , key=lambda x: sum(tuple(item[4] for item in r if item[0] == 6 and item[1] == x)))
    top_sessions.height = len(top_sessions.x_labels) * 22
    series = {k[1]: [] for k in sorted(set((item[3], item[2]) for item in r if item[0] == 6), key=lambda x: x[0])}
    for label in top_sessions.x_labels:
        for serie in series.keys():
            v = tuple(item[4] for item in r if item[0] == 6 and item[1] == label and item[2] == serie)
            series[serie].append(v[0] if len(v) > 0 else 0)
    for serie in series.keys():
        top_sessions.add(serie,  [dict(value=item
                                  , color=colors[serie]
                                  , xlink=dict(href=url_for('get_session'
                                                            , target=target
                                                            , sid=top_sessions.x_labels[i].split(':')[0]
                                                            , _external=True)
                                               , target='_blank')) for i, item in enumerate(series[serie])])

    return render_template('top_activity.html'
                           , top_activity=top_activity.render_data_uri()
                           , top_sql=top_sql.render_data_uri()
                           if 'sql_id' not in optional_values.keys() else None
                           , top_sessions=top_sessions.render_data_uri()
                           if 'session_id' not in optional_values.keys() else None
                           , top_objects=top_objects.render_data_uri()
                           , top_waits=top_waits.render_data_uri()
                           if labels else None
                           )
