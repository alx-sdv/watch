from watch import app
from watch.utils.decorate_view import *
from watch.utils.render_page import render_page


@app.route('/<target>/V/<owner>/<view>')
@title('View')
@template('single')
@columns({"text_length": 'int'
          , "editioning_view": 'str'})
@select("all_views where owner = :owner and view_name = :p_view")
def get_view(target, owner, view):
    return render_page()


@app.route('/<target>/V/<owner>/<view>/columns')
@title('Columns')
@template('list')
@auto()
@columns({"column_id": 'int'
          , "column_name": 'str'
          , "data_type": 'str'
          , "data_length": 'int'
          , "data_precision": 'int'
          , "data_scale": 'int'
          , "nullable": 'str'})
@select("all_tab_columns where owner = :owner and table_name = :p_view")
@default_sort("column_id")
def get_view_columns(target, owner, view):
    return render_page()


@app.route('/<target>/V/<owner>/<view>/text')
@title('Script')
@template('single')
@content('text')
@columns({"text": 'str'})
@select("all_views where owner = :owner and view_name = :p_view")
def get_view_text(target, owner, view):
    return render_page()
