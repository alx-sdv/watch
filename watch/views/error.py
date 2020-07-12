from flask import render_template

from watch import app


@app.errorhandler(500)
def server_error(e):
    return render_template('error.html', e=e, title='Oh, Shiii!')


@app.errorhandler(429)
def too_many_requests(e):
    return render_template('error.html', e=e, title='Too many requests')


@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', e=e, title='Page not found')


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html', e=e, title='Forbidden')


@app.errorhandler(400)
def bad_request(e):
    return render_template('error.html', e=e, title='Bad request')