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

from blinker import signal
from google.appengine.ext import deferred
from .resource import ResourceMock
import google.appengine.ext.ndb as ndb

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
        return default_config.update(config)
    except (KeyError, TypeError, AttributeError):
        # Missing key or explicitly set to none
        return None


def _init_component(config, default_config):
    try:
        _parse_component_config(config=config, default_config=default_config)
    except (KeyError, TypeError, AttributeError):
        # Missing key or explicitly set to none
        return None
    else:
        component_type = default_config['type']
        del default_config['type']

        try:
            component = component_type(**default_config)
        except TypeError:
            # The supplied type is not instantiable
            return None
        else:
            return component


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


class Method(Component):
    def __init__(self, **kwargs):
        super(Method, self).__init__(**kwargs)
        self.action_name = '{}{}'.format(self.code_name, self._parent._path.replace('.', '_'))
        self._create_signals()

    def _create_signals(self):
        self.pre_name = 'pre_{}'.format(self.action_name)
        self.post_name = 'post_{}'.format(self.action_name)

        self.pre_signal = signal(self.pre_name)
        self.post_signal = signal(self.post_name)

    @async
    def _trigger_signal(self, signal_to_trigger, sender, **kwargs):
        if bool(signal_to_trigger.receivers):
            kwargs['hook_name'] = signal_to_trigger.name
            kwargs['action'] = self.action_name
            for receiver in signal_to_trigger.receivers_for(sender=sender):
                yield receiver(self, **kwargs)
    @async
    def _internal_op(self, **kwargs):
        results = yield self._trigger_signal(signal_to_trigger=signal(self.action_name), sender=self._parent, **kwargs)
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
        result = yield self._internal_op(**kwargs)
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
        result = yield self._internal_op(**kwargs)
        yield self._trigger_signal(signal_to_trigger=self.post_signal, sender=self, op_result=result, **kwargs)
        raise ndb.Return(result)


class BaseRPCClient(Component):
    def __init__(self, resource_model, **kwargs):
        super(BaseRPCClient, self).__init__(**kwargs)
        self.resource_model = resource_model
        self.resource_name = resource_model.__name__


class RPCClientMock(BaseRPCClient):
    def __init__(self, methods=None, **kwargs):
        super(RPCClientMock, self).__init__(**kwargs)
        if methods is not None:
            self._create_methods(methods=methods, resource_name=self.resource_name, **kwargs)

    def _create_methods(self, methods, **kwargs):
        for method in methods:
            setattr(self, method, RPCMethod(code_name=method, **kwargs))


class BaseAPI(Component):
    def __init__(self, resource_model, methods=None, children=None, **kwargs):
        """
        code_name is the name of the API, taken from the api config.

        spi is the service interface that the api can use. This is available in each method.

        methods is a list of strings representing the desired methods.

        :param code_name:
        :param resource_model:
        :param base_path:
        :param spi_config:
        :param methods:
        :param children:
        :param default_spi:
        :param default_resource_update_queue:
        :param default_search_update_queue:
        """
        super(BaseAPI, self).__init__(**kwargs)
        self.resource_model = resource_model
        self.methods = methods

        if children is None:
            self.children = []
        else:
            self.children = children


class ResourceApi(BaseAPI):
    def __init__(self, default_api_method_class=Method, **kwargs):
        """
        The only difference from the base class is that we automatically create async api methods based on the provided
        list of methods. The methods are set as attributes.

        :param kwargs:
        """
        super(ResourceApi, self).__init__(**kwargs)

        if self.methods is not None:
            for method in self.methods:
                setattr(self, method, default_api_method_class(code_name=method, parent=self))


@async
def _insert(sender, api, **kwargs):
    result = yield api.datastore.insert(**kwargs)
    raise ndb.Return(result)


@async
def _get(sender, api, **kwargs):
    result = yield api.datastore.get(**kwargs)
    raise ndb.Return(result)


@async
def _update(sender, api, **kwargs):
    result = yield api.datastore.update(**kwargs)
    raise ndb.Return(result)


@async
def _delete(sender, api, **kwargs):
    result = yield api.datastore.delete(**kwargs)
    raise ndb.Return(result)


@async
def _query(sender, api, **kwargs):
    result = yield api.datastore.query(**kwargs)
    raise ndb.Return(result)


@async
def _search(sender, api, **kwargs):
    result = yield api.search_index.search(**kwargs)
    raise ndb.Return(result)


class GaeApi(ResourceApi):
    def __init__(self, datastore_config=None, search_config=None, resource_update_queue='resource-update',
                 search_update_queue='search-index-update', **kwargs):
        """
        The only difference from the parent class is that we automatically create ndb methods and setup search index
        updating.

        :param kwargs:
        """
        # TODO: move this to another module to keep things neat
        default_datastore_config = {
            'type': RPCClientMock,
            'resource_model': kwargs['resource_model'],
        }

        default_search_config = {
            'type': RPCClientMock,
            'resource_model': kwargs['resource_model'],
        }

        self.resource_update_queue = resource_update_queue
        self.search_update_queue = search_update_queue

        self.datastore = _init_component(config=datastore_config, default_config=default_datastore_config)
        self.search_index = _init_component(config=search_config, default_config=default_search_config)

        super(GaeApi, self).__init__(**kwargs)
        # Attach hooks to the methods
        signal('insert').connect(_insert, sender=self)
        signal('get').connect(_get, sender=self)
        signal('update').connect(_update, sender=self)
        signal('delete').connect(_delete, sender=self)
        signal('query').connect(_query, sender=self)
        signal('search').connect(_search, sender=self)
        # Add hooks to update search index
        # TODO: can't pickle the spi classes because of instance methods. Possibly add __getstate__ and __setstate__
        # TODO: possibly use copy_reg to define methods used for pickling API/SPI methods
        # the API methods work because they use signals to connect the internal op methods instead of being customized
        # at run time. Potentially solve everything by converting the SPI methods to do the same thing? They could all
        # share a base class? They are fundamentally similar; we're just providing an abstraction over the internal
        # workings.
        signal('post_insert').connect(self._test_hook, sender=self)
        signal('post_update').connect(lambda **k: deferred.defer(self._update_search_index, _queue=self.search_update_queue, **k), sender=self)
        signal('post_delete').connect(lambda **k: deferred.defer(self._delete_search_index, _queue=self.search_update_queue, **k), sender=self)

    def _test_hook(self, sender, result, **kwargs):
        deferred.defer(self._update_search_index, _queue=self.search_update_queue, result=result)

    def _update_search_index(self, result, **kwargs):
        resource = self.get(resource_uid=result).get_result()
        self.search_index.insert(resource=resource.to_search_doc(), identity_uid='systemidentitykey', **kwargs)

    def _delete_search_index(self, result, **kwargs):
        self.search_index.delete(resource_uid=result, identity_uid='systemidentitykey', **kwargs)


class Security(GaeApi):
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


BASE_METHODS = ['insert', 'update', 'get', 'delete']
NDB_METHODS = BASE_METHODS + ['query']
SEARCH_METHODS = BASE_METHODS + ['search']
GAE_METHODS = BASE_METHODS + ['query', 'search']


def init_api(api_name, api_def, parent=None, default_api=ResourceApi, default_methods=GAE_METHODS,
             resource_mock=ResourceMock):
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
            api_map['methods'][method]['action'] = api_method.action_name
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
            actions.add(method_details['action'])
            pre_hook_names.add(method_details['pre_hook'])
    except AttributeError:
        pass

    try:
        for child, child_map in api_map['children'].iteritems():
            compile_security_config(api_map=child_map, actions=actions, pre_hook_names=pre_hook_names)
    except AttributeError:
        pass


def parse_api_config(api_definition, default_api=ResourceApi, default_methods=GAE_METHODS, koala_security=True):
    api = TopLevelAPI()

    for api_name, api_def in api_definition.iteritems():
        init_api(api_name=api_name,
                 api_def=api_def,
                 parent=api,
                 default_api=default_api,
                 default_methods=default_methods)
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
