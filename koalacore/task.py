import os
import webapp2


def task_runner(request, *args, **kwargs):
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

        self.router.add(webapp2.Route(template='/_taskhandler.*', name='default', handler=task_runner))
