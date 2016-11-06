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
from .datastore import DatastoreMock
from .search import SearchMock
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


class API(object):
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


class AsyncResourceAPI(BaseAPI):
    def __init__(self, **kwargs):
        """
        The only difference from the base class is that we automatically create async api methods based on the provided
        list of methods. The methods are set as attributes.

        :param kwargs:
        """
        super(AsyncResourceAPI, self).__init__(**kwargs)

        if self.methods is not None:
            for method in self.methods:
                setattr(self, method, AsyncAPIMethod(code_name=method, parent_api=self))


class BaseSPI(object):
    pass


class GAESPI(BaseSPI):
    def __init__(self, datastore_config=None, search_config=None):
        default_datastore_config = {
            'type': DatastoreMock,
            'resource_model': Resource,
            'unwanted_resource_kwargs': None,
        }

        try:
            default_datastore_config.update(datastore_config)
        except (KeyError, TypeError):
            pass

        new_datastore_type = default_datastore_config['type']
        del default_datastore_config['type']
        self.datastore = new_datastore_type(**default_datastore_config)

        # Update the default search config with the user supplied values and set them in the def
        default_search_config = {
            'type': SearchMock,
            'update_queue': 'search-index-update',
            'index_name': None,
            'result': None,
            'search_result': None,
            'check_duplicates': False,
        }

        try:
            default_search_config.update(search_config)
        except (KeyError, TypeError):
            pass

        new_search_index_type = default_search_config['type']
        del default_search_config['type']
        self.search_index = new_search_index_type(**default_search_config)


class Security(AsyncResourceAPI):
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


def init_api(api_name, api_def, parent=None, default_api=AsyncResourceAPI, default_methods=GAE_METHODS,
             default_spi=GAESPI):
    try:
        # sub apis should not be passed to the api constructor.
        sub_api_defs = api_def['sub_apis']
    except KeyError:
        sub_api_defs = None

    default_spi_config = {
        'type': default_spi
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


def parse_api_config(api_definition, default_api=AsyncResourceAPI, default_methods=GAE_METHODS, koala_security=True):
    api = API()

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


# class BaseAPI(object):
#     _api_name = ''
#     _api_model = None
#     _datastore_interface = None
#     _search_interface = None
#
#     @classmethod
#     def new(cls, **kwargs):
#         return cls._api_model(**kwargs)
#
#     @classmethod
#     def insert(cls, resource_object, **kwargs):
#         if signal('pre_insert').has_receivers_for(cls):
#             signal('pre_insert').send(cls, resource_object=resource_object, **kwargs)
#
#         resource_uid = cls._datastore_interface.insert(resource_object=resource_object, **kwargs)
#         deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')
#
#         if signal('post_insert').has_receivers_for(cls):
#             signal('post_insert').send(cls, result=resource_uid, resource_uid=resource_uid, resource_object=resource_object, **kwargs)
#         return resource_uid
#
#     @classmethod
#     def get(cls, resource_uid, **kwargs):
#         if signal('pre_get').has_receivers_for(cls):
#             signal('pre_get').send(cls, resource_uid=resource_uid, **kwargs)
#
#         resource = cls._datastore_interface.get(resource_uid=resource_uid)
#
#         if signal('post_get').has_receivers_for(cls):
#             signal('post_get').send(cls, result=resource, resource_uid=resource_uid, **kwargs)
#
#         return resource
#
#     @classmethod
#     def update(cls, resource_object, **kwargs):
#         if signal('pre_update').has_receivers_for(cls):
#             signal('pre_update').send(cls, resource_object=resource_object, **kwargs)
#
#         resource_uid = cls._datastore_interface.update(resource_object=resource_object, **kwargs)
#         deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')
#
#         if signal('post_update').has_receivers_for(cls):
#             signal('post_update').send(cls, result=resource_uid, resource_uid=resource_uid, resource_object=resource_object, **kwargs)
#
#         return resource_uid
#
#     @classmethod
#     def patch(cls, resource_uid, delta_update, **kwargs):
#         if signal('pre_patch').has_receivers_for(cls):
#             signal('pre_patch').send(cls, resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#
#         resource_uid = cls._datastore_interface.patch(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#         deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')
#
#         if signal('post_patch').has_receivers_for(cls):
#             signal('post_patch').send(cls, result=resource_uid, resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#
#         return resource_uid
#
#     @classmethod
#     def delete(cls, resource_uid, **kwargs):
#         if signal('pre_delete').has_receivers_for(cls):
#             signal('pre_delete').send(cls, resource_uid=resource_uid, **kwargs)
#
#         cls._datastore_interface.delete(resource_uid=resource_uid, **kwargs)
#         deferred.defer(cls._delete_search_index, resource_uid=resource_uid, _queue='search-index-update')
#
#         if signal('post_delete').has_receivers_for(cls):
#             signal('post_delete').send(cls, result=None, resource_uid=resource_uid, **kwargs)
#
#     @classmethod
#     def search(cls, query_string, **kwargs):
#         if signal('pre_search').has_receivers_for(cls):
#             signal('pre_search').send(cls, query_string=query_string, **kwargs)
#
#         search_result = cls._search_interface.search(query_string=query_string, **kwargs)
#
#         if signal('post_search').has_receivers_for(cls):
#             signal('post_search').send(cls, result=search_result, query_string=query_string, **kwargs)
#
#         return search_result
#
#     @classmethod
#     def _update_search_index(cls, resource_uid, **kwargs):
#         resource = cls.get(resource_uid=resource_uid)
#         cls._search_interface.insert(resource_object=resource, **kwargs)
#
#     @classmethod
#     def _delete_search_index(cls, resource_uid, **kwargs):
#         cls._search_interface.delete(resource_object_uid=resource_uid, **kwargs)
#
#
# class BaseSubAPI(object):
#     _api_name = ''
#     _parent_api = None
#     _allowed_patch_keys = set()
#
#     @classmethod
#     def _parse_patch_keys(cls, delta_update):
#         delta_keys = set(delta_update.keys())
#         unauthorized_keys = delta_keys - cls._allowed_patch_keys
#         if unauthorized_keys:
#             raise ValueError(u'Cannot perform patch as "{}" are unauthorized keys'.format(unauthorized_keys))
#
#     @classmethod
#     def patch(cls, resource_uid, delta_update, **kwargs):
#         cls._parse_patch_keys(delta_update=delta_update)
#
#         if signal('pre_patch').has_receivers_for(cls):
#             signal('pre_patch').send(cls, resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#
#         resource_uid = cls._parent_api._datastore_interface.patch(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#         deferred.defer(cls._parent_api._update_search_index, resource_uid=resource_uid, _queue='search-index-update')
#
#         if signal('post_patch').has_receivers_for(cls):
#             signal('post_patch').send(cls, result=resource_uid, resource_uid=resource_uid, delta_update=delta_update, **kwargs)
#
#         return resource_uid


class BaseResourceProperty(object):
    """A data descriptor that sets and returns values normally but also includes a title attribute and assorted filters.
        You can inherit from this class to create custom property types
    """
    _name = None
    _default = None
    title = None

    _attributes = ['_name', '_default', 'title']
    _positional = 1  # Only name is a positional argument.

    def __init__(self, name=None, default=None, title=u''):
        self._name = name  # name should conform to python class attribute naming conventions
        self._default = default
        self.title = title

    def __repr__(self):
        """Return a compact unambiguous string representation of a property."""
        args = []
        cls = self.__class__
        for i, attr in enumerate(self._attributes):
            val = getattr(self, attr)
            if val is not getattr(cls, attr):
                if isinstance(val, type):
                    s = val.__name__
                else:
                    s = repr(val)
                if i >= cls._positional:
                    if attr.startswith('_'):
                        attr = attr[1:]
                    s = '%s=%s' % (attr, s)
                args.append(s)
        s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
        return s

    def __get__(self, entity, unused_cls=None):
        """Descriptor protocol: get the value from the entity."""
        if entity is None:
            return self  # __get__ called on class
        return entity._values.get(self._name, self._default)

    def __set__(self, entity, value):
        """Descriptor protocol: set the value on the entity."""
        entity._values[self._name] = value

    def _fix_up(self, cls, code_name):
        """Internal helper called to tell the property its name.

        This is called by _fix_up_properties() which is called by
        MetaModel when finishing the construction of a Model subclass.
        The name passed in is the name of the class attribute to which the
        Property is assigned (a.k.a. the code name).  Note that this means
        that each Property instance must be assigned to (at most) one
        class attribute.  E.g. to declare three strings, you must call
        StringProperty() three times, you cannot write

          foo = bar = baz = StringProperty()
        """
        if self._name is None:
            self._name = code_name

    def _has_value(self, entity, unused_rest=None):
        """Internal helper to ask if the entity has a value for this Property."""
        return self._name in entity._values


class ResourceProperty(BaseResourceProperty):
    _attributes = BaseResourceProperty._attributes + ['_immutable', '_unique', '_strip', '_lower']

    def __init__(self, immutable=False, unique=False, track_revisions=True, strip_whitespace=True,
                 force_lowercase=False, **kwargs):
        super(ResourceProperty, self).__init__(**kwargs)
        self._immutable = immutable
        self._unique = unique
        self._track_revisions = track_revisions
        self._strip = strip_whitespace
        self._lower = force_lowercase

    def __set__(self, entity, value):
        """Descriptor protocol: set the value on the entity."""

        if entity._init_complete:
            if self._immutable:
                raise AssertionError('"{}" is immutable.'.format(self._name))

        if self._strip:
            if value is not None:
                if hasattr(value, 'strip'):
                    value = value.strip()
                elif isinstance(value, list):
                    try:
                        value = [item.strip() for item in value]
                    except AttributeError:
                        # The value cannot simply be stripped. Custom formatting should be used in a dedicated method.
                        pass
                elif isinstance(value, set):
                    value_list = list(value)
                    try:
                        value = set([item.strip() for item in value_list])
                    except AttributeError:
                        # The value cannot simply be stripped. Custom formatting should be used in a dedicated method.
                        pass
        if self._lower:
            if value is not None:
                if hasattr(value, 'lower'):
                    value = value.lower()
                elif isinstance(value, list):
                    try:
                        value = [item.lower() for item in value]
                    except AttributeError:
                        # The value cannot simply be lowered. Custom formatting should be used in a dedicated method.
                        pass

        if entity._init_complete:
            if self._unique:
                entity._uniques_modified.append(self._name)
            if self._track_revisions:
                if self._name in entity._history:
                    entity._history[self._name] = (entity._history[self._name][0], value)
                else:
                    entity._history[self._name] = (getattr(entity, self._name, None), value)

        super(ResourceProperty, self).__set__(entity=entity, value=value)


class ComputedResourceProperty(BaseResourceProperty):
    _attributes = BaseResourceProperty._attributes + ['_compute_function']

    def __init__(self, compute_function, **kwargs):
        super(ComputedResourceProperty, self).__init__(**kwargs)
        self._compute_function = compute_function

    def __get__(self, entity, unused_cls=None):
        """Descriptor protocol: get the value from the entity."""
        if entity is None:
            return self  # __get__ called on class
        return self._compute_function(entity)


class MetaModel(type):
    """Metaclass for Model.

    This exists to fix up the properties -- they need to know their name.
    This is accomplished by calling the class's _fix_properties() method.

    Note: This class is derived from Google's NDB MetaModel (line 2838 in model.py)
    """

    def __init__(cls, name, bases, classdict):
        super(MetaModel, cls).__init__(name, bases, classdict)
        cls._fix_up_properties()

    def __repr__(cls):
        props = []
        for _, prop in sorted(cls._properties.iteritems()):
            props.append('%s=%r' % (prop._code_name, prop))
        return '%s<%s>' % (cls.__name__, ', '.join(props))


class BaseResource(object):
    """
    Base resource object. You have to implement some of the functionality yourself.

    You must call super(Resource, self).__init__() first in your init method.

    Immutable properties must be set within init otherwise it makes it impossible to set initial values.
    If a property is required then make sure that you check it during init and throw an exception.

    """
    # TODO: add a method to apply delta update. This can be used by forms etc as well

    __metaclass__ = MetaModel

    _properties = None
    _uniques = None

    def __init__(self, **kwargs):
        self._init_complete = False
        self._values = {}
        self._uniques_modified = []
        self._history = {}
        self._set_attributes(kwargs)
        self._init_complete = True

    def _set_attributes(self, kwds):
        """Internal helper to set attributes from keyword arguments.

        Expando overrides this.
        """
        cls = self.__class__
        for name, value in kwds.iteritems():
            prop = getattr(cls, name)  # Raises AttributeError for unknown properties.
            if not isinstance(prop, BaseResourceProperty):
                raise TypeError('Cannot set non-property %s' % name)
            prop.__set__(self, value)

    def __repr__(self):
        """Return an unambiguous string representation of an entity."""
        args = []
        for prop in self._properties.itervalues():
            if prop._has_value(self):
                val = prop.__get__(self)
                if val is None:
                    rep = 'None'
                else:
                    rep = val
                args.append('%s=%s' % (prop._name, rep))
        args.sort()
        s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
        return s

    def _as_dict(self):
        """Return a dict containing the entity's property values.
        """
        return self._values.copy()

    as_dict = _as_dict

    @classmethod
    def _fix_up_properties(cls):
        """Fix up the properties by calling their _fix_up() method.

        Note: This is called by MetaModel, but may also be called manually
        after dynamically updating a model class.
        """
        cls._properties = {}  # Map of {name: Property}
        cls._uniques = []  # Map of {name: Property}

        if cls.__module__ == __name__:  # Skip the classes in *this* file.
            return
        for name in set(dir(cls)):
            attr = getattr(cls, name, None)
            if isinstance(attr, BaseResourceProperty):
                if name.startswith('_'):
                    raise TypeError('ModelAttribute %s cannot begin with an underscore '
                                    'character. _ prefixed attributes are reserved for '
                                    'temporary Model instance values.' % name)
                attr._fix_up(cls, name)
                cls._properties[attr._name] = attr
                try:
                    if attr._unique:
                        cls._uniques.append(attr._name)
                except AttributeError:
                    pass


class Resource(BaseResource):
    """
    Default implementation of a resource. It handles uid, created and updated properties. The latter two are simply
    timestamps.

    Due to the way these objects are used, the properties cannot be mandatory. For example, the uid may be set by the
    datastore on insert. Same goes for the timestamps.

    """
    # name=None, default=None, title='', immutable=False, unique=False, track_revisions=True, strip_whitespace=True, force_lowercase=False
    uid = ResourceProperty(title=u'UID', immutable=True, track_revisions=False)
    created = ResourceProperty(title=u'Created', immutable=True, track_revisions=False)
    updated = ResourceProperty(title=u'Updated', immutable=True, track_revisions=False)


class SearchResultProperty(BaseResourceProperty):
    """
    We don't need any functionality that the BaseResourceProperty doesn't already provide. We extend here to avoid
     ambiguity between ResourceProperties and SearchResultProperties.
    """
    pass


class BaseSearchResult(object):
    """
    Base search result object. This is a stripped down version of a resource object. It doesn't include any of the fancy
    features, such as history tracking, but it does still use the concept of properties. This allows you to set titles
    for properties, which could make UI development easier.

    You must call super(SearchResult, self).__init__() first in your init method.

    """

    __metaclass__ = MetaModel

    _properties = None

    def __init__(self, **kwargs):
        self._init_complete = False
        self._values = {}
        self._set_attributes(kwargs)
        self._init_complete = True

    def _set_attributes(self, kwds):
        """Internal helper to set attributes from keyword arguments.

        Expando overrides this.
        """
        cls = self.__class__
        for name, value in kwds.iteritems():
            prop = getattr(cls, name)  # Raises AttributeError for unknown properties.
            if not isinstance(prop, BaseResourceProperty):
                raise TypeError('Cannot set non-property %s' % name)
            prop.__set__(self, value)

    def __repr__(self):
        """Return an unambiguous string representation of an entity."""
        args = []
        for prop in self._properties.itervalues():
            if prop._has_value(self):
                val = prop.__get__(self)
                if val is None:
                    rep = 'None'
                else:
                    rep = val
                args.append('%s=%s' % (prop._name, rep))
        args.sort()
        s = '%s(%s)' % (self.__class__.__name__, ', '.join(args))
        return s

    def _as_dict(self):
        """Return a dict containing the entity's property values.
        """
        return self._values.copy()

    as_dict = _as_dict

    @classmethod
    def _fix_up_properties(cls):
        """Fix up the properties by calling their _fix_up() method.

        Note: This is called by MetaModel, but may also be called manually
        after dynamically updating a model class.
        """
        cls._properties = {}  # Map of {name: Property}
        cls._uniques = []  # Map of {name: Property}

        if cls.__module__ == __name__:  # Skip the classes in *this* file.
            return
        for name in set(dir(cls)):
            attr = getattr(cls, name, None)
            if isinstance(attr, BaseResourceProperty):
                if name.startswith('_'):
                    raise TypeError('ModelAttribute %s cannot begin with an underscore '
                                    'character. _ prefixed attributes are reserved for '
                                    'temporary Model instance values.' % name)
                attr._fix_up(cls, name)
                cls._properties[attr._name] = attr


class SearchResult(BaseResource):
    """
    Default implementation of a search result. The UID property is created by default but it is up to you to set it.

    These objects should be treated as read-only.
    """
    uid = SearchResultProperty(title=u'UID')
