import xml.etree.ElementTree as ElementTree

from flask import flash, render_template, request

from watch import app
from watch.utils.decorate_view import *
from watch.utils.oracle import execute
from watch.utils.parse_args import parse_parameters
from watch.utils.render_page import render_page


@app.route('/<target>/snapshot')
@title('Workload')
@template('list')
@columns({"snap_id": 'int'
         , "cast(begin_interval_time as date) begin_date": 'datetime'
         , "cast(end_interval_time as date) end_date": 'datetime'
         , "snap_flag": 'int'})
@select("dba_hist_snapshot")
@default_sort("snap_id")
@default_filters("begin_date > -1d")
def get_target_snapshot(target):
    return render_page()


# the only reason to parse the result is https://bugzilla.mozilla.org/show_bug.cgi?id=638598
@app.route('/<target>/awr')
@title('AWR report')
def get_awr_report(target):
    s = execute(target, "select snap_id"
                        ", to_char(begin_interval_time, 'dd.mm.yyyy hh24:mi:ss') begin_date"
                        ", to_char(end_interval_time, 'dd.mm.yyyy hh24:mi:ss') end_date"
                        " from dba_hist_snapshot"
                        " order by snap_id")
    if 'do' not in request.args:
        return render_template('awr.html', snapshots=s, data=None)
    if not request.args.get('sql_id', ''):
        r = execute(target, "select output"
                            " from table(dbms_workload_repository.awr_report_html("
                            "(select dbid from v$database)"
                            ", (select instance_number from v$instance)"
                            ", :bid, :eid, 8))"
                    , {'bid': request.args['bid'], 'eid': request.args['eid']}
                    , fetch_mode='many')
    else:
        r = execute(target, "select output"
                            " from table(dbms_workload_repository.awr_sql_report_html("
                            "(select dbid from v$database)"
                            ", (select instance_number from v$instance)"
                            ", :bid, :eid, :sql_id))"
                    , {'bid': request.args['bid'], 'eid': request.args['eid'], 'sql_id': request.args['sql_id']}
                    , fetch_mode='many')
    if not r:
        flash('Not found')
        return render_template('awr.html', snapshots=s, data=None)
    root = ElementTree.fromstring(''.join(item[0] for item in r if item[0])).find('./body')
    root.find('./h1').clear()
    for item in root:
        item.attrib.pop('border', None)
        item.attrib.pop('class', None)
        item.attrib.pop('summary', None)
    return render_template('awr.html'
                           , snapshots=s
                           , data=[ElementTree.tostring(item, method='html').decode('utf-8') for item in root])


@app.route('/<target>/ash')
@title('ASH report')
def get_ash_report(target):
    if 'do' not in request.args:
        return render_template('ash.html', data=None)
    source = {}
    required = {'l_btime': 'datetime', 'l_etime': 'datetime'}
    source['l_btime'] = request.args.get('l_btime', '')
    source['l_etime'] = request.args.get('l_etime', '')
    if request.args.get('l_sid', ''):
        source['l_sid'] = request.args['l_sid']
        required['l_sid'] = 'int'
    if request.args.get('l_sql_id', ''):
        source['l_sql_id'] = request.args['l_sql_id']
        required['l_sql_id'] = 'str'
    error, required_values = parse_parameters(source, required)
    if error:
        flash(f'Incorrect value: {error}')
        return render_template('ash.html', data=None)

    r = execute(target, "select output"
                        " from table(dbms_workload_repository.ash_report_html("
                        "l_dbid => (select dbid from v$database)"
                        ", l_inst_num => (select instance_number from v$instance)"
                        ", " + ", ".join(k + ' => :' + k for k in required_values.keys()) + "))"
                , required_values
                , fetch_mode='many')
    if not r:
        flash('Not found')
        return render_template('ash.html', data=None)
    root = ElementTree.fromstring(''.join(item[0].replace('<<', '&lt;&lt;') for item in r if item[0])).find('./body')
    root.find('./h1').clear()
    for item in root:
        item.attrib.pop('border', None)
        item.attrib.pop('class', None)
        item.attrib.pop('summary', None)
    return render_template('ash.html'
                           , data=[ElementTree.tostring(item, method='html').decode('utf-8') for item in root])


@app.route('/<target>/ad_tasks')
@title('Advisor tasks')
@template('list')
@columns({"task_id": 'int'
          , "execution_end": 'datetime'
          , "owner": 'str'
          , "task_name": 'str'
          , "advisor_name": 'str'
          , "description": 'str'})
@select("dba_advisor_tasks")
@default_sort("execution_end desc")
@default_filters("execution_end > -1d")
def get_advisor_tasks(target):
    return render_page()


@app.route('/<target>/A/<owner>/<task>')
@title('Advisor task report')
@template('single')
@content('text')
@function("dbms_advisor.get_task_report")
@parameters({'level': 'ALL'
             , 'owner_name': ':owner'
             , 'task_name': ':task'})
def get_advisor_task_report(target, owner, task):
    return render_page()


@app.route('/<target>/ad_findings')
@title('Advisor findings')
@template('list')
@columns({"e.execution_start": 'datetime'
          , "f.type as f_type": 'str'
          , "o.type as o_type": 'str'
          , "o.attr1": 'str'
          , "o.attr2": 'str'
          , "o.attr3": 'str'
          , "o.attr4": 'str'
          , "f.message": 'str'
          , "f.more_info": 'str'})
@select("dba_advisor_objects o"
        " join dba_advisor_executions e on e.task_id = o.task_id"
        " join dba_advisor_findings f on f.task_id = o.task_id and f.object_id = o.object_id"
        " where e.status = 'COMPLETED'")
@default_sort("execution_start desc")
@default_filters("execution_start > -1d")
def get_advisor_findings(target):
    return render_page()


@app.route('/<target>/alert_history')
@title('Alert history')
@auto()
@template('list')
@columns({"object_name": 'str'
          , "subobject_name": 'str'
          , "object_type": 'str'
          , "cast(creation_time as date) creation_time": 'datetime'
          , "cast(time_suggested as date) time_suggested": 'datetime'
          , "reason": 'str'
          , "suggested_action": 'str'})
@select("dba_alert_history")
def get_alert_history(target):
    return render_page()


@app.route('/<target>/outstanding_alerts')
@title('Outstanding alerts')
@auto()
@template('list')
@columns(get_alert_history.columns)
@select("dba_outstanding_alerts")
def get_outstanding_alerts(target):
    return render_page()
