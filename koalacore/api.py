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
from .spi import DatastoreMock, SearchMock, GAESPI
from .resource import Resource, ResourceMock
from .ramdisk import RamDisk, _hash
import google.appengine.ext.ndb as ndb

__author__ = 'Matt Badger'

async = ndb.tasklet
sync = ndb.synctasklet
# TODO: remove the deferred library dependency; extend the BaseAPI in an App Engine specific module to include deferred.

# TODO: it is possible that these methods will fail and thus their result will be None. Passing this in a signal may
# cause other functions to throw exceptions. Check the return value before processing the post_ signals?


SECURITY_API_CONFIG = {
    'sub_apis': {
        'inode': {
        },
        'identity': {
        },
    }
}


class TopLevelAPI(object):
    def __init__(self, children=None):
        self.map = None
        if children is None:
            self.children = []
        else:
            self.children = children


def apimethodwrapper(f):
    """
        Wraps api methods with signal sending; removes boilerplate code.

        NOTE: you must use kwargs with this decorator. You should only ever be passing named arguments to api methods.
        This allows us to more easily evaluate the arguments with the signals, but it also eliminates bugs due to
        argument positioning.
    """

    def _w(*args, **kwargs):
        pre_hook_name = 'pre_{}'.format(f.func_name)
        pre_hook = signal(pre_hook_name)
        post_hook_name = 'post_{}'.format(f.func_name)
        post_hook = signal(post_hook_name)

        if bool(pre_hook.receivers):
            for receiver in pre_hook.receivers_for(args[0]):
                receiver(args[0], **kwargs)

        result = yield f(*args, **kwargs)

        if bool(post_hook.receivers):
            for receiver in post_hook.receivers_for(args[0]):
                receiver(args[0], result=result, **kwargs)

        raise ndb.Return(result)

    return _w


"""
We turn the apimethodwrapper into an ndb tasklet to facilitate async signal sending (by default the signals are
blocking). That's not to say that they can't still be blocking if you write blocking code. Any connected signals should
not return a value, and should avoid blocking code at all costs (think making remote api calls without yielding).
"""
apimethod = ndb.tasklet(apimethodwrapper)


def cache_result(ttl=0, noc=0):
    """
        Wraps an api method and caches the result

        NOTE: you must use kwargs with this decorator. You should only ever be passing named arguments to api methods.
        This allows us to more easily evaluate the arguments with the signals, but it also eliminates bugs due to
        argument positioning.
        You must also have set `create_cache` to True in the API config.
    """

    def result_cacher(func):

        def cacher(*args, **kwargs):
            cache = _hash(func, list(args), ttl, **kwargs)
            result = args[0]._cache.get_data(cache, ttl=ttl, noc=noc)

            if result is not None:
                return result
            else:
                result = func(*args, **kwargs)
                args[0]._cache.store_data(cache, result, ttl=ttl, noc=noc, ncalls=0)
            return result

        return cacher

    return result_cacher


class AsyncAPIMethod(object):
    def __init__(self, code_name, parent_api):
        self.code_name = code_name
        self.parent_api = parent_api

        try:
            self.spi = self.parent_api.spi
        except AttributeError:
            self.spi = None

        self.action_name = '{}_{}'.format(self.code_name, self.parent_api.code_name)
        self.pre_name = 'pre_{}'.format(self.code_name)
        self.hook_name = self.code_name
        self.post_name = 'post_{}'.format(self.code_name)

    @async
    def _trigger_hook(self, hook_name, **kwargs):
        hook = signal(hook_name)
        if bool(hook.receivers):
            kwargs['hook_name'] = hook_name
            kwargs['action'] = self.action_name
            for receiver in hook.receivers_for(self):
                yield receiver(self, **kwargs)

    @async
    def __call__(self, **kwargs):
        """
        API methods are very simple. They simply emit signal which you can receive and act upon. By default there are
        three signals: pre, execute, and post.

        You can connect to them by specifying the sender as a parent api instance. Be careful though, if you
        don't specify a sender you will be acting upon *every* api method!

        :param kwargs:
        :return:
        """
        yield self._trigger_hook(hook_name=self.pre_name, **kwargs)
        result = yield self._trigger_hook(hook_name=self.hook_name, spi=self.spi, **kwargs)
        yield self._trigger_hook(hook_name=self.post_name, result=result, **kwargs)

        raise ndb.Return(result)


class BaseAPI(object):
    def __init__(self, code_name, spi=None, methods=None, children=None, **kwargs):
        """
        code_name is the name of the API, taken from the api config.

        spi is the service interface that the api can use. This is available in each method.

        methods is a list of strings representing the desired methods.

        :param code_name:
        :param spi:
        :param children:
        :param methods:
        """
        self.code_name = code_name
        self.methods = methods
        self.spi = spi

        if children is None:
            self.children = []
        else:
            self.children = children


class AsyncResourceApi(BaseAPI):
    def __init__(self, default_api_method_class=AsyncAPIMethod, **kwargs):
        """
        The only difference from the base class is that we automatically create async api methods based on the provided
        list of methods. The methods are set as attributes.

        :param kwargs:
        """
        super(AsyncResourceApi, self).__init__(**kwargs)

        if self.methods is not None:
            for method in self.methods:
                setattr(self, method, default_api_method_class(code_name=method, parent_api=self))


class GaeApi(AsyncResourceApi):
    def __init__(self, **kwargs):
        """
        The only difference from the parent class is that we automatically create ndb methods and setup search index
        updating.

        :param kwargs:
        """
        super(GaeApi, self).__init__(**kwargs)
        # Attach hooks to the methods
        signal('insert').connect(self._insert, sender=self)
        signal('get').connect(self._get, sender=self)
        signal('update').connect(self._update, sender=self)
        signal('delete').connect(self._delete, sender=self)
        signal('query').connect(self._query, sender=self)
        signal('search').connect(self._search, sender=self)
        # Add hooks to update search index
        search_queue = self.spi.search_index.search_index_update_queue
        signal('post_insert').connect(lambda *a, **k: deferred.defer(self._update_search_index, _queue=search_queue, **k), sender=self)
        signal('post_update').connect(lambda *a, **k: deferred.defer(self._update_search_index, _queue=search_queue, **k), sender=self)
        signal('post_delete').connect(lambda *a, **k: deferred.defer(self._delete_search_index, _queue=search_queue, **k), sender=self)

    @staticmethod
    def _insert(spi, **kwargs):
        return spi.datastore.insert(**kwargs)

    @staticmethod
    def _get(spi, **kwargs):
        return spi.datastore.get(**kwargs)

    @staticmethod
    def _update(spi, **kwargs):
        return spi.datastore.update(**kwargs)

    @staticmethod
    def _delete(spi, **kwargs):
        return spi.datastore.delete(**kwargs)

    @staticmethod
    def _query(spi, **kwargs):
        return spi.datastore.query(**kwargs)

    @staticmethod
    def _search(spi, **kwargs):
        return spi.search_index.search(**kwargs)

    def _update_search_index(self, result, **kwargs):
        resource = self.get(resource_uid=result).get_result()
        self.spi.search_index.insert(search_doc=resource.to_search_doc(), **kwargs)

    def _delete_search_index(self, result, **kwargs):
        self.spi.search_index.delete(search_doc_uid=result, **kwargs)


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
        if resource_object is not None and resource_uid is not None:
            raise ValueError('You must supply either a resource object or a resource uid')

        if resource_object:
            if not resource_object.uid:
                raise ValueError('Resource object does not have a valid UID')

            resource_uid = resource_object.uid

        yield self.authorize(identity_uid=identity_uid, resource_uid=resource_uid, **kwargs)


BASE_METHODS = ['insert', 'update', 'get', 'delete']
NDB_METHODS = BASE_METHODS + ['query']
SEARCH_METHODS = BASE_METHODS + ['search']
GAE_METHODS = BASE_METHODS + ['query', 'search']


def init_api(api_name, api_def, parent=None, default_api=GaeApi, default_methods=GAE_METHODS,
             default_spi=GAESPI, resource_mock=ResourceMock, default_resource_update_queue='resource-update',
             default_search_update_queue='search-index-update'):
    try:
        # sub apis should not be passed to the api constructor.
        sub_api_defs = api_def['sub_apis']
    except KeyError:
        sub_api_defs = None
    try:
        # sub apis should not be passed to the api constructor.
        resource_model = api_def['resource_model']
    except KeyError:
        resource_model = resource_mock

    default_spi_config = {
        'type': default_spi,
        'resource_model': resource_model,
        'resource_update_queue': default_resource_update_queue,
        'search_index_update_queue': default_search_update_queue,
    }

    try:
        default_spi_config.update(api_def['spi'])
    except (KeyError, TypeError):
        # Missing key or explicitly set to none
        pass

    spi_type = default_spi_config['type']
    del default_spi_config['type']

    default_api_config = {
        'code_name': api_name,
        'type': default_api,
        'methods': default_methods,
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

    try:
        default_api_config['spi'] = spi_type(**default_spi_config)
    except TypeError:
        # The supplied type is not instantiable
        pass

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
            api_map['methods'][method]['hooks'] = [api_method.pre_name, api_method.hook_name, api_method.post_name]
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
            pre_hook_names.add(method_details['hooks'][0])
    except AttributeError:
        pass

    try:
        for child, child_map in api_map['children'].iteritems():
            compile_security_config(api_map=child_map, actions=actions, pre_hook_names=pre_hook_names)
    except AttributeError:
        pass


def parse_api_config(api_definition, default_api=GaeApi, default_methods=GAE_METHODS, koala_security=True):
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
