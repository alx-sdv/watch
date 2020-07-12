from cx_Oracle import DatabaseError, OperationalError
from flask import flash, g, render_template, request

from watch import app
from .oracle import execute


def render_page(prepare_only=False):
    data = None
    statement = None
    parameters = {}
    endpoint = app.view_functions[request.endpoint]

    if hasattr(endpoint, 'select'):
        statement = "select {} from {}".format(", ".join(endpoint.columns.keys()), endpoint.select)
        parameters.update({k: v for k, v in request.view_args.items()
                           if endpoint.select.find(":" + k) > -1})
        parameters.update({"p_" + k: v for k, v in request.view_args.items()
                           if endpoint.select.find(":p_" + k) > -1})  # to avoid ORA-01036

    if endpoint.template == 'single':
        if hasattr(endpoint, 'columns') and list(endpoint.columns.values())[0] == 'clob':
            fetch_mode = 'clob'
        elif hasattr(endpoint, 'columns') and len(endpoint.columns.keys()) == 1:
            fetch_mode = 'many'
        else:
            fetch_mode = 'one'

        if hasattr(endpoint, 'parameters'):
            parameters.update({k: request.view_args[v[1:]] if str(v).startswith(':') else v
                               for k, v in endpoint.parameters.items()})
        if hasattr(endpoint, 'function'):
            fetch_mode = 'func'
            statement = endpoint.function
        try:
            data = execute(request.view_args['target'], statement, parameters, fetch_mode)
            if not data:
                flash('No result found')
        except (DatabaseError, OperationalError) as e:
            flash(e.args[0].message)

    if endpoint.template == 'list':
        fetch_mode = 'many'
        if hasattr(endpoint, 'parameters'):
            parameters.update(g.required_values)
        statement = "select * from ({})".format(statement)
        if g.get('filter_expr'):
            statement += " where {}".format(g.filter_expr)
        if g.get('sort_expr'):
            statement += " order by {}".format(g.sort_expr)
        parameters.update({str(k): v for k, v in enumerate(g.get('filter_values', []), start=1)})
        try:
            data = execute(request.view_args['target'], statement, parameters, fetch_mode)
            if not data:
                flash('No rows fetched')
        except (DatabaseError, OperationalError) as e:
            flash(e.args[0].message)

    if prepare_only:
        return data
    else:
        return render_template(endpoint.template + '.html', data=data, statement=statement)
