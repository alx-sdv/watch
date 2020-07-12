def title(view_title):
    def decorator(f):
        f.title = view_title
        return f
    return decorator


def columns(view_columns):
    def decorator(f):
        f.columns = view_columns
        return f
    return decorator


def select(view_select):
    def decorator(f):
        f.select = view_select
        return f
    return decorator


def default_filters(*view_default_filters):
    def decorator(f):
        f.default_filters = tuple(view_default_filters)
        return f
    return decorator


def default_sort(view_default_sort):
    def decorator(f):
        f.default_sort = view_default_sort
        return f
    return decorator


def template(view_template):
    def decorator(f):
        f.template = view_template
        return f
    return decorator


def content(view_content):
    def decorator(f):
        f.content = view_content
        return f
    return decorator


def function(view_function):
    def decorator(f):
        f.function = view_function
        return f
    return decorator


def parameters(view_parameters):
    def decorator(f):
        f.parameters = view_parameters
        return f
    return decorator


def snail():
    def decorator(f):
        f.snail = True
        return f
    return decorator


def auto():
    def decorator(f):
        f.auto = True
        return f
    return decorator


def period(task_period):
    def decorator(f):
        f.period = task_period
        return f
    return decorator


def command(task_command):
    def decorator(f):
        f.command = task_command
        return f
    return decorator


def optional(optional_parameters):
    def decorator(f):
        f.optional = optional_parameters
        return f
    return decorator


def message_type(task_message_type):
    def decorator(f):
        f.message_type = task_message_type
        return f
    return decorator
