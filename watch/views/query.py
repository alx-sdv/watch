import xml.etree.ElementTree as ElementTree

from flask import abort, redirect, render_template, url_for

from watch import app
from watch.utils.decorate_view import *
from watch.utils.render_page import render_page


@app.route('/<target>/Q/<query>')
@title('Query')
@template('single')
@columns({"parsing_schema_name": 'str'
          , "module": 'str'
          , "action": 'str'
          , "to_date(first_load_time, 'yyyy-mm-dd/hh24:mi:ss') first_load_time": 'datetime'
          , "to_date(last_load_time, 'yyyy-mm-dd/hh24:mi:ss') last_load_time": 'datetime'
          , "last_active_time": 'datetime'
          , "(select max(sid) keep (dense_rank last order by sql_exec_start)"
            " from v$session where v$session.sql_id = v$sql.sql_id) sid": 'int'})
@select("v$sql where sql_id = :query")
def get_query(target, query):
    """V$SQL lists statistics on shared SQL areas without the GROUP BY clause and contains one row for each """ \
       """child of the original SQL text entered. Statistics displayed in V$SQL are normally updated at the """ \
       """end of query execution. However, for long running queries, they are updated every 5 seconds. This """ \
       """makes it easy to see the impact of long running SQL statements while they are still in progress."""
    return render_page()


@app.route('/<target>/Q/<query>/report')
@title('Monitoring report')
@template('single')
@content('html')
@function("dbms_sqltune.report_sql_monitor")
@parameters({"sql_id": ':query'
            , "type": 'html'})
def get_query_report(target, query):
    result = render_page(True)
    if not result:
        abort(404)
    root = ElementTree.fromstring(result)
    sql_text = root.find('.body/p')
    if sql_text is not None:
        if len(sql_text.text) > 1000:
            sql_text.text = sql_text.text[:1000] + '...'
        excess_items = ['./head/title', './body/h1', './body/hr', './body/font/hr']
        for item in excess_items:
            for found_item in root.findall(item):
                found_item.clear()
    return render_template(get_query_report.template + '.html',
                           data=ElementTree.tostring(root, method='html').decode('utf-8'))


@app.route('/<target>/Q/<query>/plan')
@title('Cursor plan')
@template('single')
@content('text')
@columns({"plan_table_output": 'str'})
@select("table(dbms_xplan.display_cursor(sql_id => :query))")
def get_query_plan(target, query):
    return render_page()


@app.route('/<target>/Q/<query>/text')
@title('SQL text')
@template('single')
@content('text')
@columns({"case when length(sql_fulltext) <> 1000 then sql_fulltext"
          "  else (select sql_fulltext from v$sqlarea where sql_id = :query) end sql_text": 'clob'})  # 12c bug
@select("v$sqlstats where sql_id = :query")
def get_query_text(target, query):
    return render_page()


@app.route('/<target>/Q/<query>/waits')
@title('Top waits')
@template('list')
@columns({"event": 'str'
          , "object_name": 'str'
          , "sql_plan_line_id": 'int'
          , "sql_plan_operation": 'str'
          , "sql_plan_options": 'str'
          , "sum(ash.wait_time) wait_time": 'int'
          , "count(1) waits": 'int'})
@select("v$active_session_history ash left join all_objects o on o.object_id = ash.current_obj#"
        " where sql_id = :query and sample_time >= :sample_time"
        " group by event, object_name, sql_plan_line_id, sql_plan_operation, sql_plan_options")
@parameters({"sample_time": ' >= datetime'})
@default_sort("wait_time desc")
def get_query_waits(target, query):
    return render_page()


@app.route('/<target>/Q/<query>/long_ops')
@title('Long operations')
@template('list')
@auto()
@columns({"sid": 'int'
          , "round((sofar/nullif(totalwork, 0)) * 100) pct_remaining": 'int'
          , "start_time": 'datetime'
          , "last_update_time": 'datetime'
          , "elapsed_seconds elapsed": 'int'
          , "time_remaining remaining": 'int'
          , "sql_plan_operation || ' ' || sql_plan_options || '"
            " at line ' || to_char(sql_plan_line_id) operation": 'str'
          , "message": 'str'})
@select("v$session_longops where sql_id = :query")
@default_sort('start_time desc')
def get_query_long_ops(target, query):
    return render_page()


@app.route('/<target>/Q/<query>/plan_monitor')
@title('Plan monitor')
@template('list')
@auto()
@columns({"first_change_time": 'datetime'
          , "last_change_time": 'datetime'
          , "refresh_count": 'int'
          , "plan_line_id": 'int'
          , "plan_operation": 'str'
          , "plan_options": 'str'
          , "starts": 'int'
          , "output_rows": 'int'
          , "workarea_mem": 'int'
          , "workarea_max_mem": 'int'
          , "workarea_tempseg": 'int'
          , "workarea_max_tempseg": 'int'})
@select("v$sql_plan_monitor where sql_id = :query")
@default_sort("plan_line_id")
def get_query_plan_stats(target, query):
    return render_page()


@app.route('/<target>/Q/<query>/notify_if_done')
@title('Notify if done')
def notify_if_done(target, query):
    return redirect(url_for('wait_for_execution', target=target, sql_id=query))
