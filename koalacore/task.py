import os
import webapp2
from google.appengine.api import users
from koalacore.api import parse_api_path


def task_runner(request, *args, **kwargs):
    if not users.get_current_user():
        webapp2.abort(code=401)

    if not users.is_current_user_admin():
        webapp2.abort(code=403)
    # api_method = parse_api_path(api=test_api, path=request.get('api_method'))
    # Must enforce admin check because the unittest runner won't
    # TODO: authenticate as admin
    # TODO: take the requested api method and execute it with kwargs
    return webapp2.Response('Task ran successfully')


class TaskHandler(webapp2.WSGIApplication):
    def __init__(self, debug=None, webapp2_config=None):
        if debug is None:
            try:
                debug = os.environ['SERVER_SOFTWARE'].startswith('Dev')
            except KeyError:
                debug = True

        super(TaskHandler, self).__init__(debug=debug, config=webapp2_config)

        self.router.add(webapp2.Route(template='/_taskhandler', name='default', handler=task_runner, methods=['POST']))
