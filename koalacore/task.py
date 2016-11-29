import os
import webapp2
import logging
from webob import exc
from google.appengine.api import users
from koalacore.api import parse_api_path


class TaskRunnerAdapter(webapp2.BaseHandlerAdapter):
    """An adapter for dispatching queued tasks to a koala api instance.

    We need to pass the api instance to the handler so that it can invoke the necessary methods. By passing instance
    variables at run time we can have multiple task handlers for different api instances.
    """
    def __call__(self, request, api_instance):
        return self.handler(request=request, api_instance=api_instance)


def task_runner_adapter(router, handler):
    return TaskRunnerAdapter(handler)


def task_runner_dispatcher(router, request, response, api_instance):
    """Dispatches a handler. Slightly modified from the webapp2 default in that we don't bother checking the handler
    type, and we pass the api instance to the handler.

    :param request:
        A :class:`Request` instance.
    :param response:
        A :class:`Response` instance.
    :raises:
        ``exc.HTTPNotFound`` if no route matched or
        ``exc.HTTPMethodNotAllowed`` if a route matched but the HTTP
        method was not allowed.
    :returns:
        The returned value from the handler.
    """
    route, args, kwargs = rv = router.match(request)
    request.route, request.route_args, request.route_kwargs = rv

    if route.handler_adapter is None:
        router.handler_adapter = router.adapt(route.handler)

    return router.handler_adapter(request=request, api_instance=api_instance)


def filter_unwanted_params(request_params, unwanted=None):
    if not unwanted:
        unwanted = []

    keys_to_keep = set(request_params) - set(unwanted)
    return{k: v for k, v in request_params.iteritems() if k in keys_to_keep}


def task_runner(request, api_instance):
    if not users.get_current_user():
        webapp2.abort(code=401)

    if not users.is_current_user_admin():
        webapp2.abort(code=403)

    method_params = filter_unwanted_params(request_params=request.params, unwanted=['identity_uid'])

    api_method_path = parse_api_path(api=api_instance, path=request.get('api_method_path'))

    # TODO: authenticate as admin and add identity_uid to method_params
    method_params['identity_uid'] = 'thisisobviouslyatest'

    result_future = api_method_path(**method_params)
    result = result_future.get_result()

    logging.debug(u"`{}` called with: \n{}\nResult: {}".format(request.get('api_method_path'), method_params, result))

    return webapp2.Response('Task ran successfully')


class TaskHandler(webapp2.WSGIApplication):
    def __init__(self, api_instance, task_handler_path='/_taskhandler', debug=None, webapp2_config=None):
        if debug is None:
            try:
                debug = os.environ['SERVER_SOFTWARE'].startswith('Dev')
            except KeyError:
                debug = True

        self.api_instance = api_instance

        super(TaskHandler, self).__init__(debug=debug, config=webapp2_config)

        self.router.set_dispatcher(task_runner_dispatcher)
        self.router.set_adapter(task_runner_adapter)

        self.router.add(webapp2.Route(template=task_handler_path, name='default', handler=task_runner, methods=['POST']))

    def __call__(self, environ, start_response):
        """Called by WSGI when a request comes in.

        Modified version of the default webapp2 method. We send the api instance to the
        dispatch method so the appropriate methods can be invoked.

        :param environ:
            A WSGI environment.
        :param start_response:
            A callable accepting a status code, a list of headers and an
            optional exception context to start the response.
        :returns:
            An iterable with the response to return to the client.
        """
        with self.request_context_class(self, environ) as (request, response):
            try:
                if request.method not in self.allowed_methods:
                    # 501 Not Implemented.
                    raise exc.HTTPNotImplemented()

                rv = self.router.dispatch(request, response, api_instance=self.api_instance)
                if rv is not None:
                    response = rv
            except Exception, e:
                try:
                    # Try to handle it with a custom error handler.
                    rv = self.handle_exception(request, response, e)
                    if rv is not None:
                        response = rv
                except webapp2.HTTPException, e:
                    # Use the HTTP exception as response.
                    response = e
                except Exception, e:
                    # Error wasn't handled so we have nothing else to do.
                    response = self._internal_error(e)

            try:
                return response(environ, start_response)
            except Exception, e:
                return self._internal_error(e)(environ, start_response)
