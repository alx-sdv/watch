from datetime import datetime

from flask import render_template, session

from watch import app, lock, task_pool
from watch.utils.decorate_view import *
from watch.utils.manage_message import t_link
from watch.utils.oracle import execute, get_tab_columns, ping
from watch.utils.parse_args import dlm_str_to_list, get_num_str, get_offset, upper_values


@app.route('/<target>/task')
@title('Tasks')
def get_task(target):
    with lock:
        task_count = tuple(v.user_name for v in task_pool.values() if v.target == target).count(session['user_name'])
        t = render_template('layout.html', text=f'{task_count} active tasks for {target}.')
    return t


@app.route('/<target>/wait_for_execution')
@title('SQL execution')
@template('task')
@parameters({'sql_id': ' = str'})
@period('1m')
@command('/wait')
def wait_for_execution(t):
    """Note that a query started manually from IDE will stay "executing" until you fetch all it's rows.""" \
       """ In such case "Wait for session" can be helpful."""
    if not t.data:
        r = execute(t.target, "select min(sql_exec_start) from v$sql_monitor"
                              " where sql_id = :sql_id and status = 'EXECUTING'"
                    , t.parameters
                    , 'one'
                    , False)
        if not r[0]:
            return t.abort(f"SQL {t.parameters['sql_id']} Not found")
        else:
            t.data = r[0]
            return
    r = execute(t.target
                , "select nvl(sum(case when status = 'EXECUTING' then 1 else 0 end), 0) e"
                  ", nvl(sum(case when status like 'DONE%' then 1 else 0 end), 0) d"
                  ", max(status) s"
                  " from v$sql_monitor where sql_id = :sql_id and sql_exec_start >= :start_time"
                , {'sql_id': t.parameters['sql_id'], 'start_time': t.data}
                , 'one'
                , False)
    if r[0] + r[1] == 0:
        return t.abort(f"SQL {t.parameters['sql_id']} Not found")
    if r[0] > 0:
        return
    if t.reply_to_message_id:
        return t.finish(r[2].lower())
    return t.finish('{} on {} is {}.'.format(t_link(f"{t.target}/Q/{t.parameters['sql_id']}", t.parameters['sql_id'])
                                             , t.target
                                             , r[2].lower()))


@app.route('/<target>/wait_for_status')
@title('Watch status')
@template('task')
@message_type('list')
@parameters({"owner": ' = str'
             , "table": ' = str'
             , "date_column": ' = str'
             , "status_column": ' = str'
             , "status_values": ' = s1;s2;sN str'
             , "info_column": ' = i1;i2;iN str'})
@optional({"filter_column": ' = str'
          , "filter_value": ' = str'})
@period('30m')
def wait_for_status(t):
    if not t.data:
        t.parameters = upper_values(t.parameters)
        t.parameters['status_values'] = dlm_str_to_list(t.parameters['status_values'])
        t.parameters['info_column'] = dlm_str_to_list(t.parameters['info_column'])
        table_columns = get_tab_columns(t.target, t.parameters['owner'], t.parameters['table'])
        for item in [t.parameters['date_column'], t.parameters['status_column']] + t.parameters['info_column']:
            if item not in table_columns.keys():
                return t.abort(f'{t.parameters["owner"]}.{t.parameters["table"]}.{item} not found.')
        if 'DATE' not in table_columns[t.parameters['date_column']]:
            return t.abort(f'{t.parameters["date_column"]} must be a date type.')

        status_type = table_columns[t.parameters['status_column']]
        if status_type != 'NUMBER' and 'CHAR' not in status_type:
            return t.abort(f'Unsupported type of {t.parameters["status_column"]} (neither number nor char).')
        if status_type == 'NUMBER':
            try:
                t.parameters['status_values'] = [int(v) for v in t.parameters['status_values']]
            except ValueError:
                return t.abort(f'All of status values ({t.parameters["status_values"]}) must be numbers.')

        t.parameters['info_column'] = {k: table_columns[k] for k in t.parameters['info_column']}

        filter_column_type = ''
        if t.optional.get('filter_column', False):
            if t.optional['filter_column'] not in table_columns.keys():
                return t.abort(f'{t.parameters["owner"]}.{t.parameters["table"]}'
                               f'.{t.optional["filter_column"]} not found.')
            filter_column_type = table_columns[t.optional['filter_column']]
            if filter_column_type != 'NUMBER' and 'CHAR' not in filter_column_type:
                return t.abort(f'Unsupported type of {t.optional["filter_column"]} (neither number nor char).')
            if not t.optional.get('filter_value', False):
                return t.abort('Filter value is not set.')
            if filter_column_type == 'NUMBER':
                try:
                    t.optional['filter_value'] = int(t.optional['filter_value'])
                except ValueError:
                    return t.abort(f'Filter value must be a number.')
        t.data = {'status_values': t.parameters['status_values']
                  , 'status_type': status_type
                  , 'start_date': t.create_date
                  , 'filter_column_type': filter_column_type}

    end_date = datetime.now()
    p = {str(k): v for k, v in enumerate(t.data['status_values'], start=1)}
    p['start_date'] = t.data['start_date']
    p['end_date'] = end_date
    p['filter_value'] = t.optional.get('filter_value', '1')
    info_column_list = []
    for c, ct in t.parameters['info_column'].items():
        if ct == 'CLOB':
            info_column_list.append(f"cast(dbms_lob.substr({c}, 255) as varchar2(255))")
        elif 'CHAR' in ct:
            info_column_list.append(f"substr(to_char({c}), 0, 255)")
        else:
            info_column_list.append(f"to_char({c})")
    info_column_sql_text = ' || \' \' || '.join(info_column_list)
    filter_column = t.optional.get('filter_column', '\'1\'')
    r = execute(t.target
                , f"select to_char({t.parameters['date_column']}, 'hh24:mi:ss')"
                  f", {info_column_sql_text}"
                  f", {t.parameters['status_column']}"
                  f" from {t.parameters['owner']}.{t.parameters['table']}"
                  f" where {t.parameters['date_column']} >= :start_date"
                  f" and {t.parameters['date_column']} < :end_date"
                  f" and {'upper' if t.data['status_type'] != 'NUMBER' else ''}"
                  f"({t.parameters['status_column']})"
                  f" in ({':' + ', :'.join(str(i) for i in range(1, len(t.data['status_values']) + 1))})"
                  f" and {filter_column} = :filter_value"
                , p
                , 'many'
                , False)
    t.data['start_date'] = end_date
    return t.get_message(r
                         , lambda o, i: f'{i[0]} {i[1]}'.replace('<', '&lt;').replace('>', '&gt;')
                         , lambda o: f'{o.parameters["table"]} ({o.target})')


@app.route('/<target>/wait_for_heavy')
@title('Heavy SQL')
@template('task')
@message_type('outstanding')
@parameters({'exec_time_min': ' >= int'
             , 'temp_usage_gb': ' >= int'})
@optional({'user_name': ' like str'
           , 'ignore_user': ' like str'})
@period('5m')
@command('/heavy')
def wait_for_heavy(t):
    r = execute(t.target
                , "select username, sql_id, exec_time_min, temp_usage_gb, exec_id, sid from"
                  " (select s.username, m.sql_id, to_char(round(elapsed_time / 60000000)) exec_time_min, s.sid,"
                  " m.sql_id || to_char(m.sql_exec_id) || to_char(m.sql_exec_start, 'yyyymmddhh24miss') exec_id,"
                  " rtrim(to_char(((nvl(sum(u.blocks), 0) * min(p.value)) / 1024 / 1024 / 1024), 'fm999990d99')"
                  ", to_char(0,'fmd'))  temp_usage_gb"
                  " from v$session s"
                  " left join v$sort_usage u on s.saddr = u.session_addr"
                  " join v$parameter p on p.name = 'db_block_size'"
                  " join v$sql_monitor m on m.sid = s.sid and m.session_serial# = s.serial#"
                  " where m.status = 'EXECUTING'{}{}"
                  " group by s.username, m.sql_id, round(elapsed_time / 60000000), s.sid,"
                  " m.sql_id || to_char(m.sql_exec_id) || to_char(m.sql_exec_start, 'yyyymmddhh24miss'))"
                  " where exec_time_min >= :exec_time_min or temp_usage_gb >= :temp_usage_gb"
                  .format(' and s.username like :user_name' if t.optional.get('user_name', None) else ''
                          , ' and s.username not like :ignore_user' if t.optional.get('ignore_user', None) else '')
                , {**t.parameters, **t.optional}
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: '{} ({}, {}) on {} is executing {} minutes and consumes {} Gb of temp space.'
                         .format(t_link(f'{o.target}/Q/{i[1]}', i[1]), i[5], i[0], o.target, i[2], i[3]), None, 4)


@app.route('/<target>/wait_for_temp')
@title('Temp usage')
@template('task')
@message_type('outstanding')
@parameters({'pct_used': ' >= int'})
@period('10m')
@command('/temp')
def wait_for_temp(t):
    """Notification will be sent again only when the threshold be crossed."""
    r = execute(t.target
                , "select tablespace_name, to_char(round((used_blocks / total_blocks) * 100)) pct_used"
                  " from v$sort_segment"
                  " where round((used_blocks / total_blocks) * 100) >= :pct_used"
                , t.parameters
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: f'Tablespace {i[0]} on {o.target} is {i[1]}% used.', None, 0)


@app.route('/<target>/wait_for_expiry')
@title('Expired users')
@template('task')
@message_type('outstanding')
@parameters({'expires_in_days': ' >= int'})
@period('1d')
@command('/exp')
def wait_for_expiry(t):
    r = execute(t.target
                , "select username, to_char(expiry_date, 'dd.mm.yyyy hh24:mi:ss') exp"
                  " from dba_users"
                  " where expiry_date between sysdate and sysdate + :expires_in_days"
                , t.parameters
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: f'User account {i[0]} on {o.target} expires at {i[1]}.', None, 0)


@app.route('/<target>/wait_for_uncommitted')
@title('Uncommitted trans')
@template('task')
@message_type('outstanding')
@parameters({'idle_time_minutes': ' >= int'})
@optional({'ignore_tables': ' like str'})
@period('1h')
@command('/uncommitted')
def wait_for_uncommitted(t):
    r = execute(t.target
                , "select distinct s.osuser, s.machine, l.name"
                  " from dba_dml_locks l"
                  " inner join v$session s on s.sid = l.session_id"
                  " where s.status != 'ACTIVE'"
                  " and l.name not like :ignore_tables"
                  " and round(last_call_et / 60) >= :idle_time_minutes"
                , {'idle_time_minutes': t.parameters['idle_time_minutes']
                    , 'ignore_tables': t.optional.get('ignore_tables', '-')}
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: f'It seems {i[0]} ({i[1]})'
                                         f' forgot to commit a transaction on {o.target} ({i[2]}).', None, 0)


@app.route('/<target>/wait_for_ts')
@title('Tabspace usage')
@template('task')
@message_type('outstanding')
@parameters({'pct_used': ' >= int'})
@optional({'tablespace_name': ' like str'})
@period('6h')
@command('/ts')
@snail()
def wait_for_ts(t):
    """Notification will be sent again only when the threshold be crossed."""
    r = execute(t.target
                , "select * from (select t.tablespace_name,"
                  " round((max_files_size - (files.free_files_space + free.free_space)) / 1024 / 1024 / 1024) used_gb,"
                  " round(files.max_files_size / 1024 / 1024 / 1024) allocated_gb,"
                  " round(((max_files_size - (files.free_files_space + free.free_space))"
                  " / max_files_size) * 100) pct_used"
                  " from dba_tablespaces t"
                  " left join (select tablespace_name,"
                  " sum(nvl(bytes,0)) free_space"
                  " from dba_free_space"
                  " group by tablespace_name) free on free.tablespace_name = t.tablespace_name"
                  " left join (select tablespace_name,"
                  " sum(decode(maxbytes, 0, bytes, maxbytes)) max_files_size,"
                  " sum(decode(maxbytes, 0, bytes, maxbytes)) - sum(bytes) free_files_space"
                  " from dba_data_files group by tablespace_name) files  on t.tablespace_name = files.tablespace_name)"
                  " where pct_used >= :pct_used and tablespace_name like :tablespace_name"
                , {'pct_used': t.parameters['pct_used'], 'tablespace_name': t.optional.get('tablespace_name', '%')}
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: f'Tablespace {i[0]} on {o.target} is {i[3]}%'
                                         f' used ({i[1]} of {i[2]} Gb).', None, 0)


@app.route('/<target>/wait_for_session')
@title('Session activity')
@template('task')
@parameters({'sid': ' = int'})
@period('1m')
@command('/waits')
def wait_for_session(t):
    """Be sure to choose the main session if your query started in parallel mode."""
    if not t.data:
        e = execute(t.target
                    , "select sid, status from v$session where sid = :sid"
                    , t.parameters
                    , 'one'
                    , False)
        if not e:
            return t.abort('Not found')
        t.data = {'sid': e[0]}
    r = execute(t.target
                , "select sid, status from v$session where sid = :sid"
                , t.data
                , 'one'
                , False)
    if not r:
        return t.finish(f"Session {t.data['sid']} is not found on {t.target}.")
    if r[1] != 'INACTIVE':
        return
    return t.finish(f'Session {r[0]} on {t.target} is {r[1].lower()}.')


@app.route('/<target>/wait_for_queued')
@title('Queued SQL')
@template('task')
@message_type('outstanding')
@parameters({'queued_time_sec': ' >= int'})
@optional({'ignore_event': ' like str'})
@period('5m')
def wait_for_queued(t):
    pt = t.period[-1:]
    pv = t.period[:-1]
    t.parameters['start_date'] = datetime.now() - get_offset(pv, pt)
    r = execute(t.target
                , "select nvl(sql_id, 'Unknown sql') || ' ' || event || ' ' || to_char(session_id), "
                  " nvl(sql_id, 'Unknown sql'), event, session_id, machine, count(1) waits"
                  " from v$active_session_history"
                  " where event like 'enq:%' and sample_time > :start_date"
                  " and event not like :ignore_event"
                  " group by nvl(sql_id, 'Unknown sql') || ' ' || event || ' ' || to_char(session_id),"
                  " sql_id, event, session_id, machine"
                  " having count(1) > :queued_time_sec"
                , {'start_date': t.parameters['start_date']
                    , 'queued_time_sec': t.parameters['queued_time_sec']
                    , 'ignore_event': t.optional.get('ignore_event', '---')}
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: '{} ({}, {}) on {} has been queued for {} seconds ({}).'
                         .format(t_link(f'{t.target}/Q/{i[1]}', i[1]), i[4], i[3], t.target, i[5], i[2]), None, 0)


@app.route('/<target>/wait_recycled')
@title('Recycled space')
@template('task')
@message_type('threshold')
@parameters({'space_gb': ' >= int'})
@period('1d')
def wait_for_recycled(t):
    r = execute(t.target
                , "select nvl(round(sum(r.space * p.value) / 1024 / 1024 / 1024), 0) space_gb"
                  " from dba_recyclebin r join v$parameter p on p.name = 'db_block_size'"
                  " where r.can_purge = 'YES' and nvl(r.space, 0) <> 0"
                , {}
                , 'one'
                , False)
    return t.get_message(r
                         , lambda o, i: f'{i} Gb can be purged from recycle bin on {o.target}.'
                         , None, t.parameters['space_gb'], 2)


@app.route('/<target>/check_size')
@title('Segment size')
@template('task')
@message_type('threshold')
@parameters({'owner': ' = str'
             , 'segment_name': ' = str'
             , 'size_mb': ' >= int'})
@period('1d')
def check_size(t):
    """Each occurrence increases the threshold to 2x."""
    r = execute(t.target
                , "select round(nvl(sum(bytes) / 1024 / 1024, 0)) size_mb"
                  " from dba_segments"
                  " where owner = :owner and segment_name = :segment_name"
                , {'owner': t.parameters['owner'], 'segment_name': t.parameters['segment_name']}
                , 'one'
                , False)
    if not r:
        return t.abort(f'Segment {t.parameters["owner"]}.{t.parameters["segment_name"]} not found.')
    return t.get_message(r
                         , lambda o, i: f'{o.parameters["owner"]}.{o.parameters["segment_name"]}'
                                        f' size reached {i} mb on {o.target}.', None, t.parameters['size_mb'], 2)


@app.route('/<target>/check_resource_usage')
@title('Resource usage')
@template('task')
@parameters({'pct_used': ' 0..100% >= int'})
@period('1h')
def check_resource_usage(t):
    r = execute(t.target
                , "select resource_name, to_char(current_utilization), trim(limit_value)"
                  ", round((current_utilization / to_number(limit_value)) * 100)"
                  " from v$resource_limit"
                  " where trim(limit_value) not in ('0', 'UNLIMITED')"
                  " and round((current_utilization / to_number(limit_value)) * 100) >= :pct_used"
                , t.parameters
                , 'many'
                , False)
    return '\n'.join(f'The resource {t.target}.{item[0]}'
                     f' is {item[3]}% used ({item[1]} of {item[2]}).' for item in r)


@app.route('/<target>/wait_for_sql_error')
@title('SQL error')
@template('task')
@message_type('list')
@optional({'ignore_user': ' like str'})
@period('5m')
def wait_for_sql_error(t):
    """This task shows sql errors, stored in sql_monitor cache. Errors, displaced from the cache, will be lost.""" \
       """ A good approach is creating a trigger "after servererror"."""
    if not t.data:
        t.data = {'start_date': t.create_date}
    end_date = datetime.now()
    r = execute(t.target
                , "select username, sql_id, sid, error_message"
                  " from v$sql_monitor"
                  " where status = 'DONE (ERROR)'"
                  " and error_number not in (1013, 28, 604, 24381)"  # cancel, kill, recursive, DML array
                  " and last_refresh_time between :start_date and :end_date"
                  " and (username not like :user_name or username is null)"
                , {'start_date': t.data['start_date']
                   , 'end_date': end_date
                   , 'user_name': t.optional.get('ignore_user', '---')}
                , 'many'
                , False)
    t.data['start_date'] = end_date
    return t.get_message(r, lambda o, i: '{} ({}, {}) on {} is failed ({}).'
                         .format(t_link(f'{o.target}/Q/{i[1]}'
                                        , i[1]), i[2], i[0], o.target, i[3].replace('\n', ' ')))


@app.route('/<target>/ping_target')
@title('Ping target')
@template('task')
@period('10m')
def ping_target(t):
    if ping(t.target) == -1 and t.data != -1:
        t.data = -1
        return f'Target {t.target} did not respond properly.'
    else:
        t.data = 0


@app.route('/<target>/check_redo_switches')
@title('Redo switches')
@template('task')
@parameters({'switches_per_interval': ' >= int'})
@period('1h')
def check_redo_switches(t):
    pt = t.period[-1:]
    pv = t.period[:-1]
    t.parameters['start_date'] = datetime.now() - get_offset(pv, pt)
    r = execute(t.target
                , "select count(1) switches_count from v$log_history"
                  " where first_time > :start_date having count(1) >= :switches_per_interval"
                , {'start_date': t.parameters['start_date']
                    , 'switches_per_interval': t.parameters['switches_per_interval']}
                , 'one'
                , False)
    return f'Redo logs on {t.target} have been switched {str(r[0])} times in the last {t.period}.' if r else None


@app.route('/<target>/check_logs_deletion')
@title('Logs moving')
@template('task')
@message_type('threshold')
@parameters({'size_gb': ' >= int'})
@period('1h')
def check_logs_deletion(t):
    """Each occurrence increases the threshold to 2x."""
    r = execute(t.target
                , "select round(nvl(sum(blocks * block_size) / 1024 / 1024 / 1024, 0)) size_gb"
                  " from v$archived_log where deleted = 'NO'"
                , {}
                , 'one'
                , False)
    return t.get_message(r, lambda o, i: f'{i} gb of archived logs on {o.target} are waiting to be deleted.'
                         , None, t.parameters['size_gb'], 2)


@app.route('/<target>/wait_for_zombie')
@title('Zombie sessions')
@template('task')
@message_type('outstanding')
@parameters({'last_call_minutes': ' >= int'})
@period('1h')
def wait_for_zombie(t):
    """User sessions could stay active and being waiting for an event that never comes.""" \
        """ They must be killed to free locked resources."""
    r = execute(t.target
                , "select sid, username from v$session where type = 'USER' and ("
                  "(sysdate - last_call_et / 86400 < sysdate - :last_call_minutes * 1 / 24 / 60 and status = 'ACTIVE')"
                  " or (event = 'SQL*Net break/reset to client' and status = 'INACTIVE'))"
                , {**t.parameters}
                , 'many'
                , False)
    return t.get_message(r
                         , lambda o, i: 'Session {} ({}) on {} seems to be a zombie.'
                         .format(t_link(f'{o.target}/S/{str(i[0])}', str(i[0])), i[1], t.target), None, 0)


@app.route('/<target>/check_job_status')
@title('Job health')
@template('task')
@message_type('outstanding')
@period('6h')
def check_job_status(t):
    r = execute(t.target
                , "select job, log_user, nvl(failures, 0) fails from dba_jobs  where broken = 'Y'"
                , {}
                , 'many'
                , False)
    return t.get_message(r, lambda o, i: f'Job {i[0]} ({i[1]}) on {o.target} is broken, {i[2]} failures.', None, 0)


@app.route('/<target>/check_src_structure')
@title('Compare structure')
@template('task')
@message_type('outstanding')
@parameters({'destination_schema': ' = str'
             , 'source_db': ' = str'
             , 'source_schema': ' = str'})
@optional({'by_target_prefix': ' = str'
           , 'by_target_postfix': ' = str'})
@period('3h')
def check_src_structure(t):
    """This task compares destination and source column types for all existing tables in specified schema."""
    if t.parameters['source_db'] not in app.config['USERS'][t.user_name][2]:
        return t.abort(f"Source target {t.parameters['source_db']} not exists or not allowed.")
    target_columns = execute(t.target
                             , "select c.table_name || '.' || c.column_name, c.data_type, c.data_length"
                               " from all_tab_columns c"
                               " join all_tables t on t.owner = c.owner and t.table_name = c.table_name"
                               " where c.owner = :destination_schema"
                               " and c.table_name like :by_target_prefix and c.table_name like :by_target_postfix"
                               " order by 1, 2"
                             , {'destination_schema': t.parameters['destination_schema']
                                , 'by_target_prefix': t.optional.get('by_target_prefix', '') + '%'
                                , 'by_target_postfix': '%' + t.optional.get('by_target_postfix', '')
                                }
                             , 'many'
                             , False)
    src_columns = execute(t.parameters['source_db']
                          , "select :prefix || c.table_name || :postfix || '.' || c.column_name,"
                            " c.data_type, c.data_length from all_tab_columns c"
                            " join all_tables t on t.owner = c.owner and t.table_name = c.table_name"
                            " where c.owner = :source_schema order by 1, 2"
                          , {'source_schema': t.parameters['source_schema']
                              , 'prefix': t.optional.get('by_target_prefix', '')
                              , 'postfix': t.optional.get('by_target_postfix', '')
                             }
                          , 'many'
                          , False)

    comparison_result = []
    src_columns = {item[0]: (item[1], item[2]) for item in src_columns}

    for target_column in target_columns:
        c = src_columns.get(target_column[0])
        if not c:
            continue
        if target_column[1] != c[0] or target_column[2] != c[1]:
            comparison_result.append((f'{target_column[0]}'
                                      f'\n  {target_column[1]}({target_column[2]}) → {c[0]}({c[1]})',))
    return t.get_message(comparison_result
                         , lambda o, i: i[0]
                         , lambda o: f"Some source tables"
                                     f" for {o.target}.{o.parameters['destination_schema']} has been changed", 0)


@app.route('/<target>/check_session_stats')
@title('Session stats')
@template('task')
@message_type('outstanding')
@parameters({'statistic_name': ' = str'
             , 'value': ' >= int'})
@period('30m')
def check_session_stats(t):
    """Please see "Activity -> Session monitor -> Session -> Session stats" to find all available statistic names."""
    r = execute(t.target
                , "select s.sid, n.name, s.value from v$sesstat s join v$statname n on s.statistic# = n.statistic#"
                  " where n.name = :statistic_name and s.value >= :value order by s.value desc"
                , {**t.parameters}
                , 'many'
                , False)
    return t.get_message(r
                         , lambda o, i: 'Session {} on {} has {} = {}.'
                         .format(t_link(f'{o.target}/S/{str(i[0])}', str(i[0])), o.target, i[1], get_num_str(i[2]))
                         , None, 0)


@app.route('/<target>/check_concurrency')
@title('SQL concurrency')
@template('task')
@message_type('threshold')
@parameters({'concurrency_pct': ' >= int'})
@period('1h')
def check_concurrency(t):
    """Each occurrence increases the threshold to 2x."""
    r = execute(t.target
                , "select nvl(round((sum(concurrency_wait_time) / nullif(sum(elapsed_time), 0)) * 100), 0) ct"
                  " from v$sql_monitor where status = 'EXECUTING'"
                  " and sid in (select sid from v$session where status = 'ACTIVE')"
                , {}
                , 'one'
                , False)
    return t.get_message(r
                         , lambda o, i: f'{o.target} has average concurrency rate = {i}%.'
                         , None, t.parameters['concurrency_pct'], 1.5)


@app.route('/<target>/check_frequent_sql')
@title('Frequent SQL')
@template('task')
@message_type('outstanding')
@parameters({'executions': ' >= int'})
@period('24h')
def check_frequent_sql(t):
    """This task based on v$sqlarea which accumulate statistics since SQL statement had been cached."""
    r = execute(t.target
                , "select sql_id, parsing_schema_name, executions,"
                  " to_char(to_date(first_load_time, 'yyyy-mm-dd/hh24:mi:ss'), 'dd.mm hh24:mi')"
                  " from v$sqlarea where parsing_schema_name not like '%SYS%'"
                  " and executions > :executions order by executions desc"
                , {**t.parameters}
                , 'many'
                , False)
    return t.get_message(r
                         , lambda o, i: '{} ({}) executed {} times since {}.'
                           .format(t_link(f'{o.target}/Q/{i[0]}', i[0]), i[1], get_num_str(i[2]), i[3])
                         , lambda o: f'Frequent executions on {o.target}' , 0)
