from watch import app
from watch.utils.decorate_view import *
from watch.utils.render_page import render_page


@app.route('/<target>')
@title('Activity')
@template('single')
@select("v$instance")
@columns({"instance_name": 'str'
          , "version": 'str'
          , "host_name": 'str'
          , "startup_time": 'datetime'
          , "user connected_as": 'str'})
def get_target(target):
    return render_page()


@app.route('/<target>/objects')
@title('Objects')
@template('list')
@snail()
@select("all_objects")
@columns({"owner": 'str'
         , "object_name": 'str'
         , "subobject_name": 'str'
         , "object_type": 'str'
         , "created": 'datetime'
         , "last_ddl_time": 'datetime'
         , "status": 'str'})
@default_filters("object_type = 'TABLE' and object_name like '%%'")
@default_sort("object_name, subobject_name")
def get_target_objects(target):
    return render_page()


@app.route('/<target>/sql_monitor')
@title('SQL monitor')
@template('list')
@auto()
@select("v$sql_monitor")
@columns({"sid": 'int'
         , "sql_id": 'str'
         , "px_maxdop px": 'int'
         , "status": 'str'
         , "username": 'str'
         , "module": 'str'
         # , "client_info": 'str'
         , "sql_exec_start": 'datetime'
         , "last_refresh_time": 'datetime'
         , "round(elapsed_time / 1000000) elapsed_secs": 'int'
         , "cpu_time": 'int'
          #  , "fetches": 'int'
         , "buffer_gets": 'int'
         , "disk_reads": 'int'
         , "direct_writes": 'int'
          # , "application_wait_time": 'int'
         , "round(concurrency_wait_time / 1000000) concurrency_secs": 'int'
          # , "cluster_wait_time": 'int'
         , "round(user_io_wait_time / 1000000) user_io_secs": 'int'
         , "lpad(sql_text, 32) sql_text": 'str'
         , "error_message msg": 'str'})
@default_filters("status = 'EXECUTING'", "msg is not null")
@default_sort("sql_exec_start desc")
def get_sql_monitor(target):
    return render_page()


@app.route('/<target>/session_monitor')
@title('Session monitor')
@template('list')
@auto()
@select("v$session s left join audit_actions a on a.action = s.command where type = 'USER'")
@columns({"sid": 'int'
         , "(select 'Y' from v$px_session ps where ps.qcsid = s.sid and ps.sid = s.sid) px": 'str'
         , "sql_id": 'str'
         , "a.name command": 'str'
         , "username": 'str'
         , "status": 'str'
         , "osuser": 'str'
         , "machine": 'str'
         , "program": 'str'
         , "logon_time": 'datetime'
         , "sysdate - last_call_et/86400 last_call": 'datetime'
         , "wait_class": 'str'
         , "event": 'str'})
@default_filters("status = 'ACTIVE'")
@default_sort("logon_time desc")
def get_session_monitor(target):
    """Px means the session is parallel run coordinator."""
    return render_page()


@app.route('/<target>/long_ops')
@title('Long operations')
@template('list')
@columns({"sid": 'int'
          , "sql_id": 'str'
          , "to_char(round((sofar/nullif(totalwork, 0)) * 100)) || '%' complete": 'str'
          , "start_time": 'datetime'
          , "last_update_time": 'datetime'
          , "elapsed_seconds elapsed": 'int'
          , "time_remaining remaining": 'int'
          , "sql_plan_operation || ' ' || sql_plan_options || '"
            " at line ' || to_char(sql_plan_line_id) operation": 'str'
          , "message": 'str'})
@select("v$session_longops")
@default_filters("remaining > 0")
@default_sort("start_time desc")
def get_target_long_ops(target):
    return render_page()


@app.route('/<target>/waits')
@title('Top object waits')
@template('list')
@columns({"event": 'str'
          , "object_name": 'str'
          , "sum(ash.wait_time) wait_time": 'int'
          , "count(1) waits": 'int'})
@select("v$active_session_history ash"
        " inner join all_objects o on o.object_id = ash.current_obj# and ash.CURRENT_OBJ# <> -1"
        " where sample_time >= :sample_time"
        " group by event, object_name")
@parameters({"sample_time": ' >= datetime'})
@default_filters("rownum <= 10")
@default_sort("wait_time desc")
def get_target_waits(target):
    return render_page()


@app.route('/<target>/users')
@title('Users')
@template('list')
@columns({"user_id": 'int'
         , "username": 'str'
         , "account_status": 'str'
         , "lock_date": 'datetime'
         , "expiry_date": 'datetime'
         , "default_tablespace": 'str'
         , "temporary_tablespace": 'str'})
@select("dba_users")
@default_filters("account_status = 'OPEN'")
@default_sort("expiry_date desc")
def get_users(target):
    return render_page()


@app.route('/<target>/table_stats')
@title('Table stats')
@template('list')
@snail()
@columns({"s.owner": 'str'
         , "object_type": 'str'
         , "s.table_name": 'str'
         , "s.partition_name": 'str'
         , "s.subpartition_name": 'str'
         , "s.num_rows": 'int'
         , "round((s.blocks * p.value) / 1024 / 1024) size_mb": 'int'
         , "round((((s.blocks * p.value) - (num_rows * avg_row_len))"
           " / nullif((s.blocks * p.value), 0)) * 100) pct_wasted": 'int'
         , "s.last_analyzed": 'datetime'
         , "s.stale_stats": 'str'})
@select("all_tab_statistics s join v$parameter p on p.name  = 'db_block_size'")
@default_filters("owner not like 'SYS%' and stale_stats = 'YES'", "object_type = 'TABLE'")
@default_sort("last_analyzed")
def get_table_stats(target):
    """Pct wasted is a very approximate parameter, based on average row length."""
    return render_page()


@app.route('/<target>/segments')
@title('Segments')
@template('list')
@snail()
@columns({"tablespace_name": 'str'
         , "owner": 'str'
         , "segment_name": 'str'
         , "segment_type": 'str'
         , "round(nvl(sum(bytes) / 1024 / 1024, 0)) size_mb": 'int'})
@select("dba_segments group by tablespace_name, owner, segment_name, segment_type")
@default_filters("size_mb > 0", "tablespace_name like '%%'")
@default_sort("size_mb desc")
def get_segments(target):
    return render_page()


@app.route('/<target>/tablespace_usage')
@title('Tablespace usage')
@template('list')
@snail()
@columns({"t.tablespace_name": 'str'
         , "files.datafiles": 'int'
         , "t.segment_space_management": 'str'
         , "round(((files.max_files_size - (files.free_files_space + free.free_space))"
           " / files.max_files_size) * 100) pct_used": 'int'
         , "round(files.max_files_size / 1024 / 1024 / 1024) allocated_gb": 'int'
         , "round((files.max_files_size - (files.free_files_space + free.free_space))"
           " / 1024 / 1024 / 1024) used_gb": 'int'
         , "round((files.free_files_space + free.free_space) / 1024 / 1024 / 1024) free_gb": 'int'})
@select("dba_tablespaces t"
        " left join (select tablespace_name, sum(nvl(bytes,0)) free_space"
        " from dba_free_space group by tablespace_name) free"
        " on t.tablespace_name = free.tablespace_name"
        " left join (select tablespace_name, count(1) datafiles,"
        " sum(decode(maxbytes, 0, bytes, maxbytes)) - sum(bytes) free_files_space,"
        " sum(decode(maxbytes, 0, bytes, maxbytes)) max_files_size"
        " from dba_data_files group by tablespace_name) files"
        " on t.tablespace_name = files.tablespace_name")
@default_sort("pct_used desc")
def get_tablespace_usage(target):
    return render_page()


@app.route('/<target>/temp_usage')
@title('Temp usage')
@template('list')
@auto()
@columns({"tablespace": 'str'
         , "total_mb": 'int'
         , "total_used_mb": 'int'
         , "total_free_mb": 'int'
         , "username": 'str'
         , "sid": 'int'
         , "sql_id": 'str'
         , "pct_sql_used": 'int'
         , "sql_used_mb": 'int'
         , "segtype": 'str'})
@select("(select u.tablespace, u.segtype, s.username, s.sid, s.sql_id"
        " , round(((min(t.total_blocks) * min(p.value)) / 1024 / 1024)) total_mb"
        " , round(((min(t.used_blocks) * min(p.value)) / 1024 / 1024)) total_used_mb"
        " , round(((min(t.free_blocks) * min(p.value)) / 1024 / 1024)) total_free_mb"
        " , round(((sum(u.blocks) / min(t.total_blocks)) * 100)) pct_sql_used"
        " , round(((sum(u.blocks) * min(p.value)) / 1024 / 1024)) sql_used_mb"
        " from v$sort_usage u"
        " join v$parameter p on p.name  = 'db_block_size'"
        " join v$sort_segment t on t.tablespace_name = u.tablespace"
        " join v$session s on s.saddr = u.session_addr"
        " group by u.tablespace, u.segtype, s.username, s.sid, s.sql_id)")
@default_sort("tablespace, sql_used_mb desc")
def get_temp_usage(target):
    return render_page()


@app.route('/<target>/plan')
@title('Plans cache')
@template('list')
@snail()
@columns({"timestamp": 'datetime'
         , "sql_id": 'str'
         , "operation": 'str'
         , "options": 'str'
         , "object_name": 'str'
         , "access_predicates": 'str'
         , "filter_predicates": 'str'
         , "cost": 'int'
         , "cardinality": 'int'
         , "projection": 'str'})
@select("v$sql_plan")
@default_sort("timestamp desc")
@default_filters("operation = 'PARTITION RANGE' and options = 'ALL'", "options = 'CARTESIAN'")
def get_plans_cache(target):
    return render_page()


@app.route('/<target>/sql_area')
@title('SQL area')
@template('list')
@snail()
@columns({"parsing_schema_name psn": 'str'
          , "sql_id": 'str'
          , "last_load_time": 'datetime'
          , "last_active_time": 'datetime'
          , "module": 'str'
          , "sharable_mem": 'int'
          , "persistent_mem": 'int'
          , "runtime_mem": 'int'
          , "disk_reads": 'int'
          , "direct_writes": 'int'
          , "buffer_gets": 'int'
          , "concurrency_wait_time concurrency": 'int'
          , "user_io_wait_time user_io": 'int'
          , "rows_processed": 'int'
          , "cpu_time": 'int'
          , "elapsed_time": 'int'})
@select("v$sqlarea")
@default_sort("last_active_time desc")
@default_filters("last_active_time > -1d")
def get_sql_area(target):
    """docs.oracle.com: V$SQLAREA displays statistics on shared SQL areas and contains one row per SQL string. """ \
        """It provides statistics on SQL statements that are in memory, parsed, and ready for execution. """ \
        """V$SQL lists statistics on shared SQL areas without the GROUP BY clause and contains one row for each """ \
        """child of the original SQL text entered. Statistics displayed in V$SQL are normally updated at the end """ \
        """of query execution. However, for long running queries, they are updated every 5 seconds. This makes """ \
        """it easy to see the impact of long running SQL statements while they are still in progress. """ \
        """asktom.oracle.com: ...v$sqlarea is a aggregate of v$sql."""
    return render_page()


@app.route('/<target>/sql_stats')
@title('SQL stats')
@template('list')
@columns({"sql_id": 'str'
          , "last_active_time": 'datetime'
          , "disk_reads": 'int'
          , "direct_writes": 'int'
          , "rows_processed": 'int'
          , "cpu_time": 'int'
          , "user_io_wait_time user_io": 'int'
          , "executions": 'int'})
@select("v$sqlstats")
@default_sort("last_active_time desc")
@default_filters("last_active_time > -1d")
def get_sql_stats(target):
    """docs.oracle.com: V$SQLSTATS displays basic performance statistics for SQL cursors and contains one row per """ \
        """SQL statement (that is, one row per unique value of SQL_ID). The column definitions for columns in """ \
        """V$SQLSTATS are identical to those in the V$SQL and V$SQLAREA views. However, the V$SQLSTATS view """ \
        """differs from V$SQL and V$SQLAREA in that it is faster, more scalable, and has a greater data retention (""" \
        """the statistics may still appear in this view, even after the cursor has been aged out of the shared pool)."""
    return render_page()


@app.route('/<target>/index_stats')
@title('Index stats')
@template('list')
@snail()
@columns({"owner": 'str'
         , "object_type": 'str'
         , "index_name": 'str'
         , "table_name": 'str'
         , "partition_name": 'str'
         , "subpartition_name": 'str'
         , "leaf_blocks": 'int'
         , "distinct_keys": 'int'
         , "avg_leaf_blocks_per_key": 'int'
         , "avg_data_blocks_per_key": 'int'
         , "clustering_factor": 'int'
         , "num_rows": 'int'
         , "last_analyzed": 'datetime'
         , "stale_stats": 'str'})
@select("all_ind_statistics")
@default_sort("last_analyzed")
def get_index_stats(target):
    return render_page()


@app.route('/<target>/privileges')
@title('Privileges')
@template('list')
@columns({"grantee": 'str'
          , "owner": 'str'
          , "table_name": 'str'
          , "grantor": 'str'
          , "privilege": 'str'
          , "grantable": 'str'
          , "hierarchy": 'str'})
@select("dba_tab_privs")
@default_sort("table_name")
def get_privileges(target):
    return render_page()


@app.route('/<target>/rman')
@title('Rman status')
@template('list')
@columns({"recid": 'int'
          , "row_type": 'str'
          , "operation": 'str'
          , "status": 'str'
          , "start_time": 'datetime'
          , "end_time": 'datetime'
          , "object_type": 'str'})
@select("v$rman_status")
@default_sort("end_time desc")
@default_filters("end_time > -1d")
def get_rman_status(target):
    return render_page()


@app.route('/<target>/dml_locks')
@title('DML locks')
@template('list')
@auto()
@columns({"session_id": 'int'
          , "owner": 'str'
          , "name": 'str'
          , "mode_held": 'str'
          , "last_convert": 'int'
          , "blocking_others": 'str'})
@select("dba_dml_locks")
def get_dml_locks(target):
    return render_page()


@app.route('/<target>/tab_partitions')
@title('Tab partitions count')
@template('list')
@snail()
@columns({"table_owner": 'str'
          , "table_name": 'str'
          , "count(partition_name) part_count": 'int'
          , "sum(subpartition_count) subpart_count": 'int'})
@select("all_tab_partitions group by table_owner, table_name")
@default_sort("part_count desc")
@default_filters("part_count > 1000 or subpart_count > 1000")
def get_tab_partitions_count(target):
    return render_page()


@app.route('/<target>/ind_partitions')
@title('Ind partitions count')
@template('list')
@snail()
@columns({"index_owner": 'str'
          , "index_name": 'str'
          , "count(partition_name) part_count": 'int'
          , "sum(subpartition_count) subpart_count": 'int'})
@select("all_ind_partitions group by index_owner, index_name")
@default_sort("part_count desc")
@default_filters("part_count > 1000 or subpart_count > 1000")
def get_ind_partitions_count(target):
    return render_page()


@app.route('/<target>/modifications')
@title('Modifications')
@template('list')
@columns({"table_owner": 'str'
         , "table_name": 'str'
         , "partition_name": 'str'
         , "subpartition_name": 'str'
         , "inserts": 'int'
         , "updates": 'int'
         , "deletes": 'int'
         , "timestamp": 'datetime'
         , "truncated": 'str'
         , "drop_segments": 'int'})
@select("all_tab_modifications")
@default_sort("timestamp desc")
@default_filters("timestamp > -1d")
def get_modifications(target):
    return render_page()


@app.route('/<target>/ts_fragmentation')
@title('Tabspace fragmentation')
@template('list')
@snail()
@columns({"t.tablespace_name": 'str'
         , "f.fc free_blocks_count": 'int'
         , "u.uc used_blocks_count": 'int'
         , "round((f.fc / (f.fc + u.uc)) * 100) pct_fragmented": 'int'})
@select("dba_tablespaces t"
        " inner join (select tablespace_name, sum(blocks) fc from dba_free_space group by tablespace_name) f"
        " on f.tablespace_name = t.tablespace_name"
        " inner join (select tablespace_name, sum(blocks) uc from dba_segments group by tablespace_name) u"
        " on u.tablespace_name = t.tablespace_name"
        " where t.contents = 'PERMANENT'")
@default_sort("pct_fragmented desc")
@default_filters("used_blocks_count >= 1000 and pct_fragmented > 30")
def get_ts_fragmentation(target):
    return render_page()


@app.route('/<target>/undo_usage')
@title('Undo usage')
@template('list')
@auto()
@select("v$session a inner join v$transaction b on a.saddr = b.ses_addr")
@columns({"a.sid": 'int'
         , "a.username": 'str'
         , "b.used_urec": 'int'
         , "b.used_ublk": 'int'})
@default_sort("used_ublk desc")
def get_undo_usage(target):
    return render_page()


@app.route('/<target>/synonyms')
@title('Synonyms')
@template('list')
@select("dba_synonyms")
@columns({"owner": 'str'
         , "synonym_name": 'str'
         , "table_owner": 'str'
         , "table_name": 'str'
         , "db_link": 'str'})
@default_filters("table_owner not like '%SYS%'")
def get_synonyms(target):
    return render_page()


@app.route('/<target>/segment_usage')
@title('Segment usage')
@template('list')
@select("v$segment_statistics")
@columns({"owner": 'str'
         , "object_name": 'str'
         , "subobject_name": 'str'
         , "tablespace_name": 'str'
         , "object_type": 'str'
         , "statistic_name": 'str'
         , "value": 'int'})
@default_filters(""
                 , "statistic_name = 'segment scans'"
                 , "statistic_name = 'row lock waits'"
                 , "statistic_name like '%read%'"
                 , "statistic_name like '%write%'")
def get_segment_usage(target):
    return render_page()
