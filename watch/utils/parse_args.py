from datetime import datetime, timedelta

from watch import app


def get_offset(pv, pt):
    i_pv = int(pv)
    return timedelta(weeks=i_pv if pt == 'w' else 0
                     , days=i_pv if pt == 'd' else 0
                     , hours=i_pv if pt == 'h' else 0
                     , minutes=i_pv if pt == 'm' else 0
                     , seconds=i_pv if pt == 's' else 0)


def parse_filter_expr(source, columns):
    comparisons = ('>=', '<=', '<>', '!=', '>', '<', '=', 'is not null', 'is null', 'not like', 'like')
    booleans = ('and', 'or')
    directives = ('w', 'd', 'h', 'm', 's')
    filter_expr = ''
    values = []

    filters = {key.strip().split(' ')[-1].split('.')[-1]: value for key, value in columns.items()}
    filters['rownum'] = 'int'
    filters_keys = sorted(filters.keys(), key=len, reverse=True)
    current_filter_pos = 1
    source = source.strip().rstrip(';')
    source_len = len(source)

    while len(source) > 0:
        column = ''
        comparison = ''
        value = ''

        for item in filters_keys:
            if source.upper().startswith(item.upper()):
                source = source[len(item):].lstrip()
                column = str(item)
                break
        if not column:
            return source_len - len(source) + 1, None, None
        filter_expr += column + ' '

        for item in comparisons:
            if source.upper().startswith(item.upper()):
                source = source[len(item):].lstrip()
                comparison = item
                break
        if not comparison:
            return source_len - len(source) + 1, None, None
        filter_expr += comparison + ' '

        if comparison not in ('is not null', 'is null'):
            if filters[column] == 'int':
                value = source.split(' ')[0]
                try:
                    int(value)
                except ValueError:
                    return source_len - len(source) + 1, None, None
                source = source[len(value) + 1:].lstrip()
                values.append(int(value))

            elif filters[column] == 'datetime' and source.startswith('-'):
                value = source.split(' ')[0]
                source = source[len(value) + 1:].lstrip()
                directive = ''
                for item in directives:
                    if value.lower().endswith(item):
                        directive = item
                        break
                if not directive:
                    return source_len - len(source) + 1, None, None
                value = value[1:-len(directive)]
                if not value.isdigit():
                    return source_len - len(source) + 1, None, None
                try:
                    values.append(datetime.now() - get_offset(value, directive))
                except (TypeError, ValueError, OverflowError):
                    return source_len - len(source) + 1, None, None

            elif filters[column] in ('str', 'datetime'):
                if not source.startswith('\''):
                    return source_len - len(source) + 1, None, None
                else:
                    source = source[1:]
                    qm = False
                for i in range(len(source)):
                    value_len = i
                    if source[i] == '\'' and not qm:
                        qm = True
                    elif source[i] == '\'' and qm:
                        qm = False
                        value += source[i]
                    else:
                        value += source[i]
                    if qm and (source + ' ')[i+1] != '\'':
                        source = source[value_len+1:].lstrip()
                        break
                if not qm:
                    return source_len - len(source) + 1, None, None
                if filters[column] == 'str':
                    values.append(str(value))
                if filters[column] == 'datetime':
                    try:
                        values.append(datetime.strptime(value, app.config['DATETIME_FORMAT']))
                    except ValueError:
                        return source_len - len(source) + 1, None, None
            filter_expr += ':' + str(current_filter_pos) + ' '
            current_filter_pos += 1

        boolean = ''
        for item in booleans:
            if source.upper().startswith(item.upper() + ' '):
                boolean = item
                filter_expr += item + ' '
                source = source[len(item):].lstrip()
                break
        if not boolean and len(source):
            return source_len - len(source) + 1, None, None

    return 0, filter_expr, values


def parse_parameters(source, required, optional=False):
    required_values = {}
    directives = ('w', 'd', 'h', 'm', 's')

    for k, v in required.items():
        v_type = v.split(' ')[-1]
        value = source[k].strip('\'')
        if optional and not value:
            continue
        if v_type == 'int':
            try:
                int(source[k])
            except ValueError:
                return k, None
            required_values[k] = int(value)
        elif v_type == 'str':
            if len(value) == 0:
                return k, None
            required_values[k] = str(value)
        elif v_type == 'datetime':
            try:
                required_values[k] = datetime.strptime(value, app.config['DATETIME_FORMAT'])
            except ValueError:
                if value.startswith('-'):
                    directive = ''
                    for item in directives:
                        if value.lower().endswith(item):
                            directive = item
                            break
                    if not directive:
                        return k, None
                    value = value[1:-len(directive)]
                    if not value.isdigit():
                        return k, None
                    try:
                        required_values[k] = datetime.now() - get_offset(value, directive)
                    except (TypeError, ValueError, OverflowError):
                        return k, None
                else:
                    return k, None
        else:
            return k, None
    return None, required_values


def parse_sort(source, columns):
    filters = sorted([key.strip().split(' ')[-1].split('.')[-1] for key in columns.keys()], key=len, reverse=True)
    source = source.strip().rstrip(';')
    source_len = len(source)
    directions = ('asc', 'desc')
    sort_expr = ''

    while len(source) > 0:
        column = ''
        for item in filters:
            if source.upper().startswith(item.upper()):
                column = str(item)
                source = source[len(item):].lstrip()
                break
        if not column:
            return source_len - len(source) + 1, None
        sort_expr += column

        for item in directions:
            if source.upper().startswith(item.upper()):
                sort_expr += ' ' + item
                source = source[len(item):].lstrip()
                break

        if len(source) > 0:
            if not source.startswith(','):
                return source_len - len(source) + 1, None
            else:
                source = source[1:].lstrip()
                sort_expr += ', '
    return 0, sort_expr


def parse_command(source):
    source_parts = source.split()
    if not source_parts:
        return '<empty>', None, None, {}
    f = None
    endpoint = ''
    for k in app.view_functions.keys():
        if getattr(app.view_functions[k], 'command', '') == source_parts[0]:
            f = app.view_functions[k]
            endpoint = k
            break
    if not f:
        return source_parts[0], None, None, {}
    target = ''
    for rule in app.url_map.iter_rules(endpoint):
        if '<target>' in rule.rule:
            if len(source_parts) == 1:
                return '<target>', None, None, {}
            target = source_parts[1].upper()
    parsed_values = {}
    parameters = getattr(f, 'parameters', {})
    if parameters:
        if not (len(source_parts) == len(parameters) + 1 + (1 if target else 0)):
            return '<parameters>', None, None, {}
        source_values = dict(zip(parameters.keys(), source_parts[1 + (1 if target else 0):]))
        rr, parsed_values = parse_parameters(source_values, parameters)
        if rr:
            return f'<{rr}>', None, None, {}
    return '', endpoint, target, parsed_values


def parse_period(source):
    value = source.lower().strip()
    if value[-1:] not in 'wdhms':
        return 'Incorrect period type.', None
    try:
        if int(value[:-1]) <= 0:
            return 'Period value must be positive.', None
    except ValueError:
            return 'Incorrect period value.', None
    return None, value


def dlm_str_to_list(s):
    return s.strip(' ;').split(';')


def upper_values(d):
    return {k: v.strip().upper() if isinstance(v, str) else v for k, v in d.items()}


def get_num_str(n):
    return "{:,}".format(n).replace(',', ' ')
