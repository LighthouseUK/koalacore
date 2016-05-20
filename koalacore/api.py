# -*- coding: utf-8 -*-
"""
    koala.api
    ~~~~~~~~~~~~~~~~~~

    Contains base implementations for building an internal project API
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""

from blinker import signal
from google.appengine.ext import deferred

__author__ = 'Matt Badger'

# TODO: remove the deferred library dependency; extend the BaseAPI in an App Engine specific module to include deferred.

# TODO: it is possible that these methods will fail and thus their result will be None. Passing this in a signal may
# cause other functions to throw exceptions. Check the return value before processing the post_ signals?


# Result should always be the first argument to the post_ signals. That way the receivers can check the value before
# continuing execution.
class BaseAPI(object):
    _api_name = ''
    _api_model = None
    _datastore_interface = None
    _search_interface = None

    @classmethod
    def new(cls, **kwargs):
        return cls._api_model(**kwargs)

    @classmethod
    def insert(cls, resource_object, **kwargs):
        if signal('pre_insert').has_receivers_for(cls):
            signal('pre_insert').send(cls, resource_object=resource_object, **kwargs)

        resource_uid = cls._datastore_interface.insert(resource_object=resource_object, **kwargs)
        deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')

        if signal('post_insert').has_receivers_for(cls):
            signal('post_insert').send(cls, result=resource_uid, resource_uid=resource_uid, resource_object=resource_object, **kwargs)
        return resource_uid

    @classmethod
    def get(cls, resource_uid, **kwargs):
        if signal('pre_get').has_receivers_for(cls):
            signal('pre_get').send(cls, resource_uid=resource_uid, **kwargs)

        resource = cls._datastore_interface.get(resource_uid=resource_uid)

        if signal('post_get').has_receivers_for(cls):
            signal('post_get').send(cls, result=resource, resource_uid=resource_uid, **kwargs)

        return resource

    @classmethod
    def update(cls, resource_object, **kwargs):
        if signal('pre_update').has_receivers_for(cls):
            signal('pre_update').send(cls, resource_object=resource_object, **kwargs)

        resource_uid = cls._datastore_interface.update(resource_object=resource_object, **kwargs)
        deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')

        if signal('post_update').has_receivers_for(cls):
            signal('post_update').send(cls, result=resource_uid, resource_uid=resource_uid, resource_object=resource_object, **kwargs)

        return resource_uid

    @classmethod
    def patch(cls, resource_uid, delta_update, **kwargs):
        if signal('pre_patch').has_receivers_for(cls):
            signal('pre_patch').send(cls, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

        resource_uid = cls._datastore_interface.patch(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
        deferred.defer(cls._update_search_index, resource_uid=resource_uid, _queue='search-index-update')

        if signal('post_patch').has_receivers_for(cls):
            signal('post_patch').send(cls, result=resource_uid, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

        return resource_uid

    @classmethod
    def delete(cls, resource_uid, **kwargs):
        if signal('pre_delete').has_receivers_for(cls):
            signal('pre_delete').send(cls, resource_uid=resource_uid, **kwargs)

        cls._datastore_interface.delete(resource_uid=resource_uid, **kwargs)
        deferred.defer(cls._delete_search_index, resource_uid=resource_uid, _queue='search-index-update')

        if signal('post_delete').has_receivers_for(cls):
            signal('post_delete').send(cls, result=None, resource_uid=resource_uid, **kwargs)

    @classmethod
    def search(cls, query_string, **kwargs):
        if signal('pre_search').has_receivers_for(cls):
            signal('pre_search').send(cls, query_string=query_string, **kwargs)

        search_result = cls._search_interface.search(query_string=query_string, **kwargs)

        if signal('post_search').has_receivers_for(cls):
            signal('post_search').send(cls, result=search_result, query_string=query_string, **kwargs)

        return search_result

    @classmethod
    def _update_search_index(cls, resource_uid, **kwargs):
        resource = cls.get(resource_uid=resource_uid)
        cls._search_interface.insert(resource_object=resource, **kwargs)

    @classmethod
    def _delete_search_index(cls, resource_uid, **kwargs):
        cls._search_interface.delete(resource_object_uid=resource_uid, **kwargs)


class BaseSubAPI(object):
    _api_name = ''
    _parent_api = None
    _allowed_patch_keys = set()

    @classmethod
    def _parse_patch_keys(cls, delta_update):
        delta_keys = set(delta_update.keys())
        unauthorized_keys = delta_keys - cls._allowed_patch_keys
        if unauthorized_keys:
            raise ValueError(u'Cannot perform patch as "{}" are unauthorized keys'.format(unauthorized_keys))

    @classmethod
    def patch(cls, resource_uid, delta_update, **kwargs):
        cls._parse_patch_keys(delta_update=delta_update)

        if signal('pre_patch').has_receivers_for(cls):
            signal('pre_patch').send(cls, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

        resource_uid = cls._parent_api._datastore_interface.patch(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
        deferred.defer(cls._parent_api._update_search_index, resource_uid=resource_uid, _queue='search-index-update')

        if signal('post_patch').has_receivers_for(cls):
            signal('post_patch').send(cls, result=resource_uid, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

        return resource_uid


class BaseResourceProperty(object):
    """A data descriptor that sets and returns values normally but also includes a title attribute and assorted filters.
        You can inherit from this class to create custom property types
    """
    _name = None
    _default = None
    title = None

    _attributes = ['_name', '_default', 'title']
    _positional = 1  # Only name is a positional argument.

    def __init__(self, name=None, default=None, title=''):
        self._name = name   # name should conform to python class attribute naming conventions
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

    def __init__(self, immutable=False, unique=False, track_revisions=True, strip_whitespace=True, force_lowercase=False, **kwargs):
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
