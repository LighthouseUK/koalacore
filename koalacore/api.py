# -*- coding: utf-8 -*-
"""
    koalacore.api
    ~~~~~~~~~~~~~~~~~~

    Contains base implementations for building an internal project API
    
    Copyright 2016 Lighthouse

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import functools
import pickle
from blinker import signal
from .resource import ResourceMock
import google.appengine.ext.ndb as ndb
from google.appengine.api import taskqueue

__author__ = 'Matt Badger'

async = ndb.tasklet
sync = ndb.synctasklet


SECURITY_API_CONFIG = {
    'sub_apis': {
        'inode': {
        },
        'identity': {
        },
    }
}


def _parse_component_config(config, default_config):
    try:
        default_config.update(config)
    except (KeyError, TypeError, AttributeError):
        # Missing key or explicitly set to none
        pass


def _build_component(config):
    component_type = config['type']
    del config['type']
    return component_type(**config)


def _init_component(config, default_config):
    _parse_component_config(config=config, default_config=default_config)
    return _build_component(config=default_config)


class TopLevelAPI(object):
    def __init__(self, children=None):
        self._path = ''
        self.map = None
        if children is None:
            self.children = []
        else:
            self.children = children


class Component(object):
    def __init__(self, code_name, parent, **kwargs):
        self.code_name = code_name
        self._parent = parent
        self._path = '{}.{}'.format(self._parent._path, code_name)
        self._full_name = '{}{}'.format(self.code_name, self._parent._path.replace('.', '_'))


class Method(Component):
    def __init__(self, **kwargs):
        super(Method, self).__init__(**kwargs)
        self._internal_op_async = async(self._internal_op)
        self._create_signals()

    def _create_signals(self):
        self.pre_name = 'pre_{}'.format(self._full_name)
        self.post_name = 'post_{}'.format(self._full_name)

        self.pre_signal = signal(self.pre_name)
        self.post_signal = signal(self.post_name)

    @async
    def _trigger_signal(self, signal_to_trigger, sender, **kwargs):
        if bool(signal_to_trigger.receivers):
            kwargs['hook_name'] = signal_to_trigger.name
            kwargs['action'] = self._full_name
            for receiver in signal_to_trigger.receivers_for(sender=sender):
                yield receiver(sender, **kwargs)

    def _internal_op(self, **kwargs):
        """
        Note the lack of async decorator. This is intentional because the __call__ method may wish to use a different
         decorator at runtime (based on request variables).
        :param kwargs:
        :return:
        """
        results = yield self._trigger_signal(signal_to_trigger=signal(self._full_name), sender=self._parent, **kwargs)
        # Result could be a list, depending on how many hooks you have setup. These will all be passed to the post hook
        # so that you can do further processing. However, callers of the API do not need these extra return values. We
        # simply return the first value in the list.
        if results is not None and results:
            main_result = results[0]
        else:
            main_result = None
        raise ndb.Return(main_result)

    @async
    def __call__(self, **kwargs):
        """
        Methods are very simple. They simply emit signal which you can receive and act upon, both before and after the
        internal op is executed.

        :param kwargs:
        :return:
        """
        yield self._trigger_signal(signal_to_trigger=self.pre_signal, sender=self._parent, **kwargs)
        result = yield self._internal_op_async(**kwargs)
        yield self._trigger_signal(signal_to_trigger=self.post_signal, sender=self._parent, op_result=result, **kwargs)
        raise ndb.Return(result)


class RPCMethod(Method):
    @async
    def __call__(self, **kwargs):
        """
        This is basically the same as the parent class except we use self as the sender instead of the parent object

        :param kwargs:
        :return:
        """
        yield self._trigger_signal(signal_to_trigger=self.pre_signal, sender=self, **kwargs)
        result = yield self._internal_op_async(**kwargs)
        yield self._trigger_signal(signal_to_trigger=self.post_signal, sender=self, op_result=result, **kwargs)
        raise ndb.Return(result)


class BaseAPI(Component):
    def __init__(self, resource_model, methods=None, children=None, method_class=Method, task_queue_path='/_taskhandler', default_task_queue='deferredwork', **kwargs):
        """
        code_name is the name of the API, taken from the api config.

        methods is a list of strings representing the desired methods.

        :param code_name:
        :param resource_model:
        :param methods:
        :param children:
        """
        super(BaseAPI, self).__init__(**kwargs)
        self.resource_model = resource_model
        self.methods = methods
        self.task_queue_path = task_queue_path
        self.default_task_queue = default_task_queue

        if children is None:
            self.children = []
        else:
            self.children = children

        self._create_methods(method_class=method_class)

    def _create_methods(self, method_class):
        if self.methods is not None:
            for method in self.methods:
                setattr(self, method, method_class(code_name=method, parent=self))

    def defer(self, payload, queue_name=None, **kwargs):
        """
        Kwargs can be any additional arguments that you would normally pass to taskqueue.add()

        Payload must contain 'api_method_path' of the form `company.get`. This will be parsed by the deferred task
        handler.

        :param payload:
        :param queue_name:
        :param kwargs:
        :return:
        """
        if queue_name is None:
            queue_name = self.default_task_queue

        pickled_payload = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)

        taskqueue.add(url=self.task_queue_path, payload=pickled_payload, queue_name=queue_name, **kwargs)

    def _parse_defer_payload(self):
        pass

    def defer_lambda(self, api_method_path, **kwargs):
        payload = {
            'api_method_path': api_method_path
        }

        payload.update(kwargs)
        self.defer(payload=payload)


class RPCClient(BaseAPI):
    def __init__(self, method_class=RPCMethod, **kwargs):
        super(RPCClient, self).__init__(method_class=method_class, **kwargs)


class ResourceApi(BaseAPI):
    pass


class GaeMethod(Method):
    def __init__(self, rpc_client, **kwargs):
        self.rpc_client = rpc_client
        super(GaeMethod, self).__init__(**kwargs)

    def _internal_op(self, **kwargs):
        rpc_method = getattr(self.rpc_client, self.code_name)
        result = yield rpc_method(**kwargs)
        raise ndb.Return(result)


class GaeAPI(ResourceApi):
    def __init__(self, datastore_config=None, search_config=None, resource_update_queue='resource-update',
                 search_update_queue='search-index-update', **kwargs):
        """
        The only difference from the parent class is that we automatically create ndb methods and setup search index
        updating.

        :param kwargs:
        """
        # TODO: move this to another module to keep things neat
        default_datastore_config = {
            'type': RPCClient,
            'resource_model': kwargs['resource_model'],
            'parent': self,
            'code_name': 'ndb_client',
        }

        default_search_config = {
            'type': RPCClient,
            'resource_model': kwargs['resource_model'],
            'parent': self,
            'code_name': 'search_client',
        }

        self.resource_update_queue = resource_update_queue
        self.search_update_queue = search_update_queue

        _parse_component_config(config=datastore_config, default_config=default_datastore_config)
        self.datastore_client_config = default_datastore_config

        _parse_component_config(config=search_config, default_config=default_search_config)
        self.search_client_config = default_search_config

        super(GaeAPI, self).__init__(**kwargs)
        signal(self.insert.post_name).connect(lambda s, **k: self.defer_lambda(api_method_path='companies._update_search_index', **k), sender=self, weak=False)
        signal(self.update.post_name).connect(lambda s, **k: self.defer_lambda(api_method_path='companies._update_search_index', **k), sender=self, weak=False)
        signal(self.delete.post_name).connect(lambda s, **k: self.defer_lambda(api_method_path='companies._delete_search_index', **k), sender=self, weak=False)

    def _create_methods(self, method_class):
        self.datastore_client = _build_component(config=self.datastore_client_config)
        self.search_client = _build_component(config=self.search_client_config)

        self.insert = GaeMethod(code_name='insert', parent=self, rpc_client=self.datastore_client)
        self.get = GaeMethod(code_name='get', parent=self, rpc_client=self.datastore_client)
        self.update = GaeMethod(code_name='update', parent=self, rpc_client=self.datastore_client)
        self.delete = GaeMethod(code_name='delete', parent=self, rpc_client=self.datastore_client)
        self.query = GaeMethod(code_name='query', parent=self, rpc_client=self.datastore_client)
        self.search = GaeMethod(code_name='search', parent=self, rpc_client=self.search_client)

    def test_hook(self, *args, **kwargs):
        pass

    def _update_search_index(self, result, **kwargs):
        resource = self.get(resource_uid=result).get_result()
        self.search_client.insert(resource=resource.to_search_doc(), identity_uid='systemidentitykey', **kwargs)

    def _delete_search_index(self, result, **kwargs):
        self.search_client.delete(resource_uid=result, identity_uid='systemidentitykey', **kwargs)


class Security(ResourceApi):
    def __init__(self, valid_actions, pre_hook_names, **kwargs):
        # Init the inode and securityid apis. The default values will be ok here. We need to rely on the NDB datastore
        # because of the implementation. It's ok if the resources themselves are kept elsewhere.

        # TODO: for each op, take the resource uid. Should be string. Try to parse key. If fail then build one what we
        # can use as a parent key in the datastore

        # TODO: auto add receivers for create and delete methods so that we can generate inodes for each resource. Do
        # in transaction?

        super(Security, self).__init__(**kwargs)
        self.valid_actions = valid_actions
        self.pre_hook_names = pre_hook_names

        for pre_hook in pre_hook_names:
            signal(pre_hook).connect(self.get_uid_and_check_permissions)

    def chmod(self):
        # Need to check that the user is authorized to perform this op
        pass

    def chflags(self):
        # Need to check that the user is authorized to perform this op
        pass

    @async
    def authorize(self, identity_uid, resource_uid, action, **kwargs):
        """
        identity_uid will generally be the uid for a user, but it could also apply to other 'users' such as client
        credentials in an OAuth2 authentication flow.

        resource_uid is the uid of the resource that the action is to be performed on.

        action is the name of the action that is to be performed on the resource. Might change this to permission.

        :param identity_uid:
        :param resource_uid:
        :param action:
        :return:
        """
        pass

    @async
    def get_uid_and_check_permissions(self, sender, identity_uid, resource_object=None, resource_uid=None, **kwargs):
        if resource_object:
            if resource_object.uid:
                resource_uid = resource_object.uid

        yield self.authorize(identity_uid=identity_uid, resource_uid=resource_uid, **kwargs)

sentinel = object()


def rgetattr(obj, attr, default=sentinel):
    """
    Credit to unutbu on SO: http://stackoverflow.com/a/31174427/4596718
    :param obj:
    :param attr:
    :param default:
    :return:
    """
    if default is sentinel:
        _getattr = getattr
    else:
        def _getattr(obj, name):
            return getattr(obj, name, default)
    return functools.reduce(_getattr, [obj]+attr.split('.'))


def parse_api_path(api, path):
    """
    path should be a period delimited string e.g. .company.get
    :param api:
    :param path:
    :return:
    """
    if path.startswith('.'):
        path = path.lstrip('.')

    return rgetattr(obj=api, attr=path)


BASE_METHODS = ['insert', 'update', 'get', 'delete']
NDB_METHODS = BASE_METHODS + ['query']
SEARCH_METHODS = BASE_METHODS + ['search']
GAE_METHODS = BASE_METHODS + ['query', 'search']


def init_api(api_name, api_def, parent=None, default_api=ResourceApi, default_methods=GAE_METHODS,
             resource_mock=ResourceMock, default_task_queue_path='/_taskhandler', default_task_queue='deferredwork'):
    try:
        # sub apis should not be passed to the api constructor.
        sub_api_defs = api_def['sub_apis']
    except KeyError:
        sub_api_defs = None

    default_api_config = {
        'code_name': api_name,
        'parent': parent,
        'type': default_api,
        'methods': default_methods,
        'resource_model': resource_mock,
        'task_queue_path': default_task_queue_path,
        'default_task_queue': default_task_queue,
    }

    # This could raise a number of exceptions. Rather than swallow them we will let them bubble to the top;
    # fail fast
    default_api_config.update(api_def)

    # Create the new api
    new_api_type = default_api_config['type']
    del default_api_config['type']

    if sub_api_defs is not None:
        # We shouldn't pass the sub_apis to the api constructor.
        del default_api_config['sub_apis']

    new_api = new_api_type(**default_api_config)

    if sub_api_defs is not None:
        # recursively add the sub apis to this newly created api
        for sub_api_name, sub_api_def in sub_api_defs.iteritems():
            init_api(api_name=sub_api_name,
                     api_def=sub_api_def,
                     parent=new_api,
                     default_api=default_api)
            new_api.children.append(sub_api_name)

    if parent:
        setattr(parent, api_name, new_api)


def walk_the_api(api, api_map):
    try:
        api_map['methods'] = {}
        for method in api.methods:
            api_map['methods'][method] = {}
            api_method = getattr(api, method)
            api_map['methods'][method]['pre_hook'] = api_method.pre_name
            api_map['methods'][method]['post_hook'] = api_method.post_name
            api_map['methods'][method]['full_name'] = api_method._full_name
    except AttributeError:
        pass

    try:
        api_map['children'] = {}
        for child in api.children:
            api_map['children'][child] = {}
            walk_the_api(api=getattr(api, child), api_map=api_map['children'][child])
    except AttributeError:
        pass


def compile_security_config(api_map, actions, pre_hook_names):
    try:
        for method, method_details in api_map['methods'].iteritems():
            actions.add(method_details['full_name'])
            pre_hook_names.add(method_details['pre_hook'])
    except AttributeError:
        pass

    try:
        for child, child_map in api_map['children'].iteritems():
            compile_security_config(api_map=child_map, actions=actions, pre_hook_names=pre_hook_names)
    except AttributeError:
        pass


def parse_api_config(api_definition, default_api=ResourceApi, default_methods=GAE_METHODS, koala_security=True,
                     resource_mock=ResourceMock, default_task_queue_path='/_taskhandler',
                     default_task_queue='deferredwork'):
    api = TopLevelAPI()

    for api_name, api_def in api_definition.iteritems():
        init_api(api_name=api_name,
                 api_def=api_def,
                 parent=api,
                 default_api=default_api,
                 default_methods=default_methods,
                 resource_mock=resource_mock,
                 default_task_queue_path=default_task_queue_path,
                 default_task_queue=default_task_queue)
        api.children.append(api_name)

    api_map = {}
    walk_the_api(api=api, api_map=api_map)
    api.map = api_map

    if koala_security:
        actions = set()
        pre_hook_names = set()
        compile_security_config(api_map=api_map, actions=actions, pre_hook_names=pre_hook_names)

        security_def = {
            'type': Security,
            'spi': {
                'type': None,
            },
            'valid_actions': actions,
            'pre_hook_names': pre_hook_names,
            'sub_apis': {
                'inode': {
                },
                'identity': {
                },
            }
        }

        init_api(api_name='security', api_def=security_def, parent=api, default_methods=None)

    return api
