from time import time

from flask import abort, flash, render_template, request

from watch import app
from watch.utils.decorate_view import *
from watch.utils.oracle import execute
from watch.utils.parse_args import parse_parameters
from watch.utils.render_page import render_page


@app.route('/<target>/T/<owner>/<table>')
@title('Table')
@template('single')
@columns({"num_rows": 'int'
          , "last_analyzed": 'datetime'
          , "partitioned": 'str'
          , "compression": 'str'
          , "compress_for": 'str'})
@select("all_tables where owner = :owner and table_name = :p_table")
def get_table(target, owner, table):
    return render_page()


@app.route('/<target>/T/<owner>/<table>/ddl')
@title('Get DDL')
@template('single')
@content('text')
@function("dbms_metadata.get_ddl")
@parameters({"object_type": 'TABLE'
             , "name": ':table'
             , "schema": ':owner'})
def get_table_ddl(target, owner, table):
    return render_page()


@app.route('/<target>/T/<owner>/<table>/row_count')
@title('Row count')
@columns({'date_column': 'datetime'
          , 'row_count': 'int'})
def get_row_count(target, owner, table):
    date_columns = execute(target, "select column_name from all_tab_columns"
                                   " where owner = :o and table_name = :t and data_type = 'DATE'"
                                   " order by column_name", {'o': owner, 't': table})
    if 'do' not in request.args:
        return render_template('row_count.html', date_columns=date_columns, data=None)

    check_for_column = execute(target, "select owner, table_name, column_name from all_tab_columns"
                                       " where owner = :o and table_name = :t and data_type = 'DATE'"
                                       " and column_name = :c"
                               , {'o': owner, 't': table, 'c': request.args.get('column_name', '')}
                               , fetch_mode='one')
    if not check_for_column:
        flash('No such column')
        return render_template('row_count.html', date_columns=date_columns, data=None)

    rr, required_values = parse_parameters(request.args, {'date_from': 'datetime'})
    if rr:
        flash(f'Incorrect value for required parameter: {rr}')
        return render_template('row_count.html', date_columns=date_columns, data=None)
    data = execute(target, f"select trunc({check_for_column[2]}) date_column, count({check_for_column[2]}) row_count"
                           f" from {check_for_column[0]}.{check_for_column[1]}"
                           f" where {check_for_column[2]} >= :date_from"
                           f" group by trunc({check_for_column[2]})"
                           f" order by trunc({check_for_column[2]})", required_values)
    if not data:
        flash('No rows found for this period')
    return render_template('row_count.html', date_columns=date_columns, data=data)


@app.route('/<target>/T/<owner>/<table>/columns')
@title('Columns')
@template('list')
@auto()
@columns({"c.column_id": 'int'
          , "c.column_name": 'str'
          , "c.data_type": 'str'
          , "c.data_length": 'int'
          , "c.data_precision": 'int'
          , "c.data_scale": 'int'
          , "c.nullable": 'str'
          , "c.data_default": 'str'
          , "c.num_distinct": 'int'
          , "c.num_nulls": 'int'
          , "c.last_analyzed": 'datetime'
          , "c.histogram": 'str'
          , "p.column_position part_pos": 'int'
          , "sp.column_position subpart_pos": 'int'})
@select("all_tab_columns c"
        " left join all_part_key_columns p"
        " on p.owner = c.owner and p.name = c.table_name"
        " and p.object_type = 'TABLE' and p.column_name = c.column_name"
        " left join all_subpart_key_columns sp"
        " on sp.owner = c.owner and sp.name = c.table_name"
        " and sp.object_type = 'TABLE' and sp.column_name = c.column_name"
        " where c.owner = :owner and c.table_name = :p_table")
@default_sort("column_id")
def get_table_columns(target, owner, table):
    return render_page()


@app.route('/<target>/T/<owner>/<table>/indexes')
@title('Indexes')
@template('list')
@auto()
@columns({"i.index_name": 'str'
          , "i.index_type": 'str'
          , "i.partitioned": 'str'
          , "i.uniqueness": 'str'
          , "i.distinct_keys": 'int'
          , "i.clustering_factor": 'int'
          , "i.status": 'str'
          , "i.num_rows": 'int'
          , "i.last_analyzed": 'datetime'
          , "i.degree": 'str'
          , "i.join_index": 'str'
          , "i.visibility": 'str'
          , "i.logging": 'str'
          , "c.columns": 'str'})
@select("all_indexes i"
        " join (select index_owner, index_name, listagg(column_name, ', ')"
        " within group (order by column_position) columns"
        " from all_ind_columns group by index_owner, index_name) c"
        " on c.index_owner = i.owner and c.index_name = i.index_name"
        " where i.owner = :owner and i.table_name = :p_table")
def get_table_indexes(target, owner, table):
    return render_page()


@app.route('/<target>/T/<owner>/<table>/partitions')
@title('Partitions')
@template('list')
@auto()
@columns({"tablespace_name": 'str'
          , "partition_name": 'str'
          , "subpartition_count": 'int'
          , "high_value": 'str'
          , "num_rows": 'int'
          , "last_analyzed": 'datetime'
          , "compression": 'str'
          , "compress_for": 'str'})
@select("all_tab_partitions"
        " where table_owner = :owner and table_name = :p_table")
def get_table_partitions(target, owner, table):
    return render_page()


@app.route('/<target>/T/<owner>/<table>/insert_from_select')
@title('Insert from select')
def get_insert_from_select(target, owner, table):
    params = {'owner': owner, 'p_table': table}
    r = execute(target
                , "select count(table_name) from all_tables"
                  " where owner = :owner and table_name = :p_table"
                , params
                , 'one')
    if r[0] != 1:
        abort(404)
    column_list = execute(target
                          , "select column_name from all_tab_cols"
                            " where owner = :owner and table_name = :p_table and virtual_column = 'NO'"
                            " order by column_id"
                          , params)
    column_string_list = '\n     , '.join([i[0] for i in column_list])
    return render_template('layout.html', formatted_text=f"INSERT /*+ APPEND */ INTO {owner}.{table}\n"
                                                         f"      ({column_string_list})\n"
                                                         f"SELECT {column_string_list}\n"
                                                         f"FROM ???.{table};\n"
                                                         f"COMMIT;")


@app.route('/<target>/T/<owner>/<table>/scan')
@title('Test scan speed')
@columns({'name': 'str'
          , 'scan_time,_sec': 'int'
          , 'row_count': 'int'
          , 'row/sec': 'int'
          , 'size,_mb': 'int'
          , 'mb/sec': 'int'})
def get_scan_speed(target, owner, table):
    r = execute(target
                , "select owner, table_name from all_tables"
                  " where owner = :owner and table_name = :p_table"
                , {'owner': owner, 'p_table': table}
                , 'one')
    if not r:
        abort(404)
    owner_name, table_name = r
    part_list = execute(target
                        , "select partition_name from all_tab_partitions"
                          " where table_owner = :owner and table_name = :p_table"
                          " order by partition_name"
                        , {'owner': owner_name, 'p_table': table_name})
    start_table_scan_time = time()
    scan_results = []
    if part_list:
        r = execute(target
                    , "select  nvl(sp.partition_name, s.partition_name) partition_name"
                      " , round(nvl(sum(bytes) / 1024 / 1024, 0)) size_mb"
                      " from dba_segments s left join all_tab_subpartitions sp"
                      " on sp.table_owner = s.owner and sp.table_name = s.segment_name"
                      " and s.partition_name = sp.subpartition_name"
                      " where s.owner = :owner and s.segment_name = :p_table"
                      " group by nvl(sp.partition_name, s.partition_name) order by 1"
                    , {'owner': owner_name, 'p_table': table_name})
        part_size = {item[0]: item[1] for item in r}
        for partition in part_list:
            start_part_scan_time = time()
            r = execute(target
                        , f"select /*+ no_index(t) */ count(*)"
                          f" from {owner_name}.{table_name} partition ({partition[0]}) t", {}, 'one')
            finish_part_scan_time = time()
            scan_results.append((partition[0], r[0], round(finish_part_scan_time - start_part_scan_time)
                                 , part_size.get(partition[0], 0)))
    else:
        r = execute(target
                    , f"select /*+ no_index(t) */ count(*) from {owner_name}.{table_name} t", {}, 'one')
        table_size = execute(target
                             , "select round(nvl(sum(bytes) / 1024 / 1024, 0)) size_mb"
                               " from dba_segments where owner = :owner and segment_name = :p_table"
                             , {'owner': owner_name, 'p_table': table_name}, 'one')
        scan_results.append((table_name, r[0], round(time() - start_table_scan_time), table_size[0]))

    finish_table_scan_time = time()
    output_data = []
    for item in scan_results:
        output_data.append((item[0], item[2], item[1], item[1] if item[2] == 0 else round(item[1] / item[2])
                            , item[3], item[3] if item[2] == 0 else round(item[3] / item[2])))
    if part_list:
        total_row_count = sum(list(zip(*output_data))[2])
        total_scan_time = round(finish_table_scan_time - start_table_scan_time)
        total_size = sum(part_size.values())
        output_data.append(('TOTAL:'
                            , total_scan_time
                            , total_row_count
                            , total_row_count if total_scan_time == 0 else round(total_row_count / total_scan_time)
                            , total_size
                            , total_size if total_scan_time == 0 else round(total_size / total_scan_time)))

    return render_template('static_list.html'
                           , text=f'Completed in {((finish_table_scan_time - start_table_scan_time) / 60):.1f} minutes.'
                           , data=output_data)
