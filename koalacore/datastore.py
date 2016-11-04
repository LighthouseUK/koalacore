# -*- coding: utf-8 -*-
"""
    koala.datastore
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import logging
import datetime
import zlib
import copy
import pickle
from blinker import signal
import google.appengine.ext.ndb as ndb
from google.appengine.api import datastore_errors, datastore_types, users
from google.appengine.datastore import entity_pb
from google.appengine.ext.ndb.google_imports import ProtocolBuffer
from .tools import DictDiffer
from .exceptions import KoalaException


__author__ = 'Matt Badger'


class ResourceNotFound(KoalaException):
    """
    Raised when a datastore method that requires a resource cannot find said resource. Usually because the supplied uid
    does not exist.
    """
    pass


class ResourceException(KoalaException):
    """
    Used when there was a problem persisting changes to a resource. Generally this is the base exception; more granular
    exceptions would be useful, but it provides a catch all fallback.
    """
    pass


class UniqueValueRequired(ResourceException, ValueError):
    """
    Raised during the insert, update operations in the datastore interfaces. If a lock on the unique value cannot be
    obtained then this exception is raised. It should detail the reason for failure by listing the values that locks
    could not be obtained for.
    """

    def __init__(self, errors, message=u'Unique resource values already exist in the datastore'):
        super(UniqueValueRequired, self).__init__(message)
        self.errors = errors


class DatastoreMock(object):
    def __init__(self, methods, resource_model, *args, **kwargs):
        # TODO: accept list of method names and generate them
        for method in methods:
            setattr(self, method, NDBMethod(code_name=method, resource_model=resource_model))

    def __getattr__(self, name):
        if not name.startswith('__') and not name.endswith('__'):
            raise ndb.Return("'%s' was called" % name)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, name))


class BaseDatastoreInterface(object):
    def __init__(self, datastore_model, resource_model):
        self._datastore_model = datastore_model
        self._resource_model = resource_model
        # TODO: accept a list of method names and generate method stubs


class ResourceProperty(ndb.Property):
    _positional = 1  # Only name is a positional argument.
    _attributes = ndb.Property._attributes + ['_immutable', '_unique']
    
    @ndb.utils.positional(1 + _positional)  # Add 1 for self.
    def __init__(self, immutable=False, unique=False, track_revisions=True, **kwargs):
        super(ResourceProperty, self).__init__(**kwargs)
        self._immutable = immutable
        self._unique = unique
        self._track_revisions = track_revisions

    def __set__(self, entity, value):
        """Descriptor protocol: set the value on the entity."""

        if entity._init_complete:
            if self._immutable:
                raise AssertionError('"{}" is immutable.'.format(self._name))

        if entity._init_complete:
            if self._unique:
                entity._uniques_modified.append(self._name)
            if self._track_revisions:
                if self._name in entity._history:
                    entity._history[self._name] = (entity._history[self._name][0], value)
                else:
                    entity._history[self._name] = (getattr(entity, self._name, None), value)

        super(ResourceProperty, self).__set__(entity=entity, value=value)

"""
Because we have changed the base Property class, the subclasses properties need to be modified. Now, this could be done
with some manipulation of the __bases__ attribute, but there are all sorts of issues with that. Simpler to redeclare
the subclassed properties with the new base class.
"""


class IntegerProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if not isinstance(value, (int, long)):
            raise datastore_errors.BadValueError('Expected integer, got %r' %
                                                 (value,))
        return int(value)

    def _db_set_value(self, v, unused_p, value):
        if not isinstance(value, (bool, int, long)):
            raise TypeError('IntegerProperty %s can only be set to integer values; '
                            'received %r' % (self._name, value))
        v.set_int64value(value)

    def _db_get_value(self, v, unused_p):
        if not v.has_int64value():
            return None
        return int(v.int64value())


class FloatProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class.

    Note: int, long and bool are also allowed.
    """

    def _validate(self, value):
        if not isinstance(value, (int, long, float)):
            raise datastore_errors.BadValueError('Expected float, got %r' %
                                                 (value,))
        return float(value)

    def _db_set_value(self, v, unused_p, value):
        if not isinstance(value, (bool, int, long, float)):
            raise TypeError('FloatProperty %s can only be set to integer or float '
                            'values; received %r' % (self._name, value))
        v.set_doublevalue(float(value))

    def _db_get_value(self, v, unused_p):
        if not v.has_doublevalue():
            return None
        return v.doublevalue()


class BooleanProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    # TODO: Allow int/long values equal to 0 or 1?

    def _validate(self, value):
        if not isinstance(value, bool):
            raise datastore_errors.BadValueError('Expected bool, got %r' %
                                                 (value,))
        return value

    def _db_set_value(self, v, unused_p, value):
        if not isinstance(value, bool):
            raise TypeError('BooleanProperty %s can only be set to bool values; '
                            'received %r' % (self._name, value))
        v.set_booleanvalue(value)

    def _db_get_value(self, v, unused_p):
        if not v.has_booleanvalue():
            return None
        # The booleanvalue field is an int32, so booleanvalue() returns an
        # int, hence the conversion.
        return bool(v.booleanvalue())


_MAX_STRING_LENGTH = ndb.model._MAX_STRING_LENGTH
_MEANING_URI_COMPRESSED = ndb.model._MEANING_URI_COMPRESSED
_CompressedValue = ndb.model._CompressedValue


class BlobProperty(ResourceProperty):
    """A Property whose value is a byte string.  It may be compressed."""

    _indexed = False
    _compressed = False

    _attributes = ResourceProperty._attributes + ['_compressed']

    @ndb.utils.positional(1 + ResourceProperty._positional)
    def __init__(self, name=None, compressed=False, **kwds):
        super(BlobProperty, self).__init__(name=name, **kwds)
        self._compressed = compressed
        if compressed and self._indexed:
            # TODO: Allow this, but only allow == and IN comparisons?
            raise NotImplementedError('BlobProperty %s cannot be compressed and '
                                      'indexed at the same time.' % self._name)

    def _value_to_repr(self, value):
        long_repr = super(BlobProperty, self)._value_to_repr(value)
        # Note that we may truncate even if the value is shorter than
        # _MAX_STRING_LENGTH; e.g. if it contains many \xXX or \uUUUU
        # escapes.
        if len(long_repr) > _MAX_STRING_LENGTH + 4:
            # Truncate, assuming the final character is the closing quote.
            long_repr = long_repr[:_MAX_STRING_LENGTH] + '...' + long_repr[-1]
        return long_repr

    def _validate(self, value):
        if not isinstance(value, str):
            raise datastore_errors.BadValueError('Expected str, got %r' %
                                                 (value,))
        if (self._indexed and
                not isinstance(self, TextProperty) and
                    len(value) > _MAX_STRING_LENGTH):
            raise datastore_errors.BadValueError(
                'Indexed value %s must be at most %d bytes' %
                (self._name, _MAX_STRING_LENGTH))

    def _to_base_type(self, value):
        if self._compressed:
            return _CompressedValue(zlib.compress(value))

    def _from_base_type(self, value):
        if isinstance(value, _CompressedValue):
            return zlib.decompress(value.z_val)

    def _datastore_type(self, value):
        # Since this is only used for queries, and queries imply an
        # indexed property, always use ByteString.
        return datastore_types.ByteString(value)

    def _db_set_value(self, v, p, value):
        if isinstance(value, _CompressedValue):
            self._db_set_compressed_meaning(p)
            value = value.z_val
        else:
            self._db_set_uncompressed_meaning(p)
        v.set_stringvalue(value)

    def _db_set_compressed_meaning(self, p):
        # Use meaning_uri because setting meaning to something else that is not
        # BLOB or BYTESTRING will cause the value to be decoded from utf-8 in
        # datastore_types.FromPropertyPb. That would break the compressed string.
        p.set_meaning_uri(_MEANING_URI_COMPRESSED)
        p.set_meaning(entity_pb.Property.BLOB)

    def _db_set_uncompressed_meaning(self, p):
        if self._indexed:
            p.set_meaning(entity_pb.Property.BYTESTRING)
        else:
            p.set_meaning(entity_pb.Property.BLOB)

    def _db_get_value(self, v, p):
        if not v.has_stringvalue():
            return None
        value = v.stringvalue()
        if p.meaning_uri() == _MEANING_URI_COMPRESSED:
            value = _CompressedValue(value)
        return value


class TextProperty(BlobProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if isinstance(value, str):
            # Decode from UTF-8 -- if this fails, we can't write it.
            try:
                length = len(value)
                value = value.decode('utf-8')
            except UnicodeError:
                raise datastore_errors.BadValueError('Expected valid UTF-8, got %r' %
                                                     (value,))
        elif isinstance(value, unicode):
            length = len(value.encode('utf-8'))
        else:
            raise datastore_errors.BadValueError('Expected string, got %r' %
                                                 (value,))
        if self._indexed and length > _MAX_STRING_LENGTH:
            raise datastore_errors.BadValueError(
                'Indexed value %s must be at most %d bytes' %
                (self._name, _MAX_STRING_LENGTH))

    def _to_base_type(self, value):
        if isinstance(value, unicode):
            return value.encode('utf-8')

    def _from_base_type(self, value):
        if isinstance(value, str):
            try:
                return unicode(value, 'utf-8')
            except UnicodeDecodeError:
                # Since older versions of NDB could write non-UTF-8 TEXT
                # properties, we can't just reject these.  But _validate() now
                # rejects these, so you can't write new non-UTF-8 TEXT
                # properties.
                # TODO: Eventually we should close this hole.
                pass


class StringProperty(TextProperty):
    _indexed = True
    _attributes = ResourceProperty._attributes + ['_strip', '_lower']

    @ndb.utils.positional(1 + ResourceProperty._positional)  # Add 1 for self.
    def __init__(self, strip_whitespace=True, force_lowercase=False, **kwargs):
        super(ResourceProperty, self).__init__(**kwargs)
        self._strip = strip_whitespace
        self._lower = force_lowercase

    def __set__(self, entity, value):
        """
        If you look at the documentation for the validation attribute of a property you will see that they allow you to
        coerce values inside a validation function. I find this to be somewhat unintuitive and prefer the validators
        to not modify the submitted values. Hence the addition of the `strip_whitespace` and `force_lowercase` init
        args.
        """

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

        super(ResourceProperty, self).__set__(entity=entity, value=value)


_EPOCH = ndb.model._EPOCH


class DateTimeProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _attributes = ResourceProperty._attributes + ['_auto_now', '_auto_now_add']

    _auto_now = False
    _auto_now_add = False

    @ndb.utils.positional(1 + ResourceProperty._positional)
    def __init__(self, name=None, auto_now=False, auto_now_add=False, **kwds):
        super(DateTimeProperty, self).__init__(name=name, **kwds)
        # TODO: Disallow combining auto_now* and default?
        if self._repeated:
            if auto_now:
                raise ValueError('DateTimeProperty %s could use auto_now and be '
                                 'repeated, but there would be no point.' % self._name)
            elif auto_now_add:
                raise ValueError('DateTimeProperty %s could use auto_now_add and be '
                                 'repeated, but there would be no point.' % self._name)
        self._auto_now = auto_now
        self._auto_now_add = auto_now_add

    def _validate(self, value):
        if not isinstance(value, datetime.datetime):
            raise datastore_errors.BadValueError('Expected datetime, got %r' %
                                                 (value,))

    def _now(self):
        return datetime.datetime.utcnow()

    def _prepare_for_put(self, entity):
        if (self._auto_now or
                (self._auto_now_add and not self._has_value(entity))):
            value = self._now()
            self._store_value(entity, value)

    def _db_set_value(self, v, p, value):
        if not isinstance(value, datetime.datetime):
            raise TypeError('DatetimeProperty %s can only be set to datetime values; '
                            'received %r' % (self._name, value))
        if value.tzinfo is not None:
            raise NotImplementedError('DatetimeProperty %s can only support UTC. '
                                      'Please derive a new Property to support '
                                      'alternative timezones.' % self._name)
        dt = value - _EPOCH
        ival = dt.microseconds + 1000000 * (dt.seconds + 24 * 3600 * dt.days)
        v.set_int64value(ival)
        p.set_meaning(entity_pb.Property.GD_WHEN)

    def _db_get_value(self, v, unused_p):
        if not v.has_int64value():
            return None
        ival = v.int64value()
        return _EPOCH + datetime.timedelta(microseconds=ival)


_date_to_datetime = ndb.model._date_to_datetime
_time_to_datetime = ndb.model._time_to_datetime


class DateProperty(DateTimeProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if not isinstance(value, datetime.date):
            raise datastore_errors.BadValueError('Expected date, got %r' %
                                                 (value,))

    def _to_base_type(self, value):
        assert isinstance(value, datetime.date), repr(value)
        return _date_to_datetime(value)

    def _from_base_type(self, value):
        assert isinstance(value, datetime.datetime), repr(value)
        return value.date()

    def _now(self):
        return datetime.datetime.utcnow().date()


class TimeProperty(DateTimeProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if not isinstance(value, datetime.time):
            raise datastore_errors.BadValueError('Expected time, got %r' %
                                                 (value,))

    def _to_base_type(self, value):
        assert isinstance(value, datetime.time), repr(value)
        return _time_to_datetime(value)

    def _from_base_type(self, value):
        assert isinstance(value, datetime.datetime), repr(value)
        return value.time()

    def _now(self):
        return datetime.datetime.utcnow().time()


BlobKey = ndb.model.BlobKey
Expando = ndb.model.Expando
Model = ndb.model.Model
Key = ndb.model.Key
GeoPt = ndb.model.GeoPt
_unpack_user = ndb.model._unpack_user
_MAX_LONG = ndb.model._MAX_LONG


class GenericProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _compressed = False

    _attributes = ResourceProperty._attributes + ['_compressed']

    @ndb.utils.positional(1 + ResourceProperty._positional)
    def __init__(self, name=None, compressed=False, **kwds):
        if compressed:  # Compressed implies unindexed.
            kwds.setdefault('indexed', False)
        super(GenericProperty, self).__init__(name=name, **kwds)
        self._compressed = compressed
        if compressed and self._indexed:
            # TODO: Allow this, but only allow == and IN comparisons?
            raise NotImplementedError('GenericProperty %s cannot be compressed and '
                                      'indexed at the same time.' % self._name)

    def _to_base_type(self, value):
        if self._compressed and isinstance(value, str):
            return _CompressedValue(zlib.compress(value))

    def _from_base_type(self, value):
        if isinstance(value, _CompressedValue):
            return zlib.decompress(value.z_val)

    def _validate(self, value):
        if self._indexed:
            if isinstance(value, unicode):
                value = value.encode('utf-8')
            if isinstance(value, basestring) and len(value) > _MAX_STRING_LENGTH:
                raise datastore_errors.BadValueError(
                    'Indexed value %s must be at most %d bytes' %
                    (self._name, _MAX_STRING_LENGTH))

    def _db_get_value(self, v, p):
        # This is awkward but there seems to be no faster way to inspect
        # what union member is present.  datastore_types.FromPropertyPb(),
        # the undisputed authority, has the same series of if-elif blocks.
        # (We don't even want to think about multiple members... :-)
        if v.has_stringvalue():
            sval = v.stringvalue()
            meaning = p.meaning()
            if meaning == entity_pb.Property.BLOBKEY:
                sval = BlobKey(sval)
            elif meaning == entity_pb.Property.BLOB:
                if p.meaning_uri() == _MEANING_URI_COMPRESSED:
                    sval = _CompressedValue(sval)
            elif meaning == entity_pb.Property.ENTITY_PROTO:
                # NOTE: This is only used for uncompressed LocalStructuredProperties.
                pb = entity_pb.EntityProto()
                pb.MergePartialFromString(sval)
                modelclass = Expando
                if pb.key().path().element_size():
                    kind = pb.key().path().element(-1).type()
                    modelclass = Model._kind_map.get(kind, modelclass)
                sval = modelclass._from_pb(pb)
            elif meaning != entity_pb.Property.BYTESTRING:
                try:
                    sval.decode('ascii')
                    # If this passes, don't return unicode.
                except UnicodeDecodeError:
                    try:
                        sval = unicode(sval.decode('utf-8'))
                    except UnicodeDecodeError:
                        pass
            return sval
        elif v.has_int64value():
            ival = v.int64value()
            if p.meaning() == entity_pb.Property.GD_WHEN:
                return _EPOCH + datetime.timedelta(microseconds=ival)
            return ival
        elif v.has_booleanvalue():
            # The booleanvalue field is an int32, so booleanvalue() returns
            # an int, hence the conversion.
            return bool(v.booleanvalue())
        elif v.has_doublevalue():
            return v.doublevalue()
        elif v.has_referencevalue():
            rv = v.referencevalue()
            app = rv.app()
            namespace = rv.name_space()
            pairs = [(elem.type(), elem.id() or elem.name())
                     for elem in rv.pathelement_list()]
            return Key(pairs=pairs, app=app, namespace=namespace)
        elif v.has_pointvalue():
            pv = v.pointvalue()
            return GeoPt(pv.x(), pv.y())
        elif v.has_uservalue():
            return _unpack_user(v)
        else:
            # A missing value implies null.
            return None

    def _db_set_value(self, v, p, value):
        # TODO: use a dict mapping types to functions
        if isinstance(value, str):
            v.set_stringvalue(value)
            # TODO: Set meaning to BLOB or BYTESTRING if it's not UTF-8?
            # (Or TEXT if unindexed.)
        elif isinstance(value, unicode):
            v.set_stringvalue(value.encode('utf8'))
            if not self._indexed:
                p.set_meaning(entity_pb.Property.TEXT)
        elif isinstance(value, bool):  # Must test before int!
            v.set_booleanvalue(value)
        elif isinstance(value, (int, long)):
            # pylint: disable=superfluous-parens
            if not (-_MAX_LONG <= value < _MAX_LONG):
                raise TypeError('Property %s can only accept 64-bit integers; '
                                'received %s' % (self._name, value))
            v.set_int64value(value)
        elif isinstance(value, float):
            v.set_doublevalue(value)
        elif isinstance(value, Key):
            # See datastore_types.PackKey
            ref = value.reference()
            rv = v.mutable_referencevalue()  # A Reference
            rv.set_app(ref.app())
            if ref.has_name_space():
                rv.set_name_space(ref.name_space())
            for elem in ref.path().element_list():
                rv.add_pathelement().CopyFrom(elem)
        elif isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                raise NotImplementedError('Property %s can only support the UTC. '
                                          'Please derive a new Property to support '
                                          'alternative timezones.' % self._name)
            dt = value - _EPOCH
            ival = dt.microseconds + 1000000 * (dt.seconds + 24 * 3600 * dt.days)
            v.set_int64value(ival)
            p.set_meaning(entity_pb.Property.GD_WHEN)
        elif isinstance(value, GeoPt):
            p.set_meaning(entity_pb.Property.GEORSS_POINT)
            pv = v.mutable_pointvalue()
            pv.set_x(value.lat)
            pv.set_y(value.lon)
        elif isinstance(value, users.User):
            datastore_types.PackUser(p.name(), value, v)
        elif isinstance(value, BlobKey):
            v.set_stringvalue(str(value))
            p.set_meaning(entity_pb.Property.BLOBKEY)
        elif isinstance(value, Model):
            set_key = value._key is not None
            pb = value._to_pb(set_key=set_key)
            value = pb.SerializePartialToString()
            v.set_stringvalue(value)
            p.set_meaning(entity_pb.Property.ENTITY_PROTO)
        elif isinstance(value, _CompressedValue):
            value = value.z_val
            v.set_stringvalue(value)
            p.set_meaning_uri(_MEANING_URI_COMPRESSED)
            p.set_meaning(entity_pb.Property.BLOB)
        else:
            raise NotImplementedError('Property %s does not support %s types.' %
                                      (self._name, type(value)))


class _StructuredGetForDictMixin(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _get_for_dict(self, entity):
        value = self._get_value(entity)
        if self._repeated:
            value = [v._to_dict() for v in value]
        elif value is not None:
            value = value._to_dict()
        return value


_BaseValue = ndb.model._BaseValue
_NestedCounter = ndb.model._NestedCounter
InvalidPropertyError = ndb.model.InvalidPropertyError


class StructuredProperty(_StructuredGetForDictMixin):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _modelclass = None

    _attributes = ['_modelclass'] + ResourceProperty._attributes
    _positional = 1 + ResourceProperty._positional  # Add modelclass as positional arg.

    @ndb.utils.positional(1 + _positional)
    def __init__(self, modelclass, name=None, **kwds):
        super(StructuredProperty, self).__init__(name=name, **kwds)
        if self._repeated:
            if modelclass._has_repeated:
                raise TypeError('This StructuredProperty cannot use repeated=True '
                                'because its model class (%s) contains repeated '
                                'properties (directly or indirectly).' %
                                modelclass.__name__)
        self._modelclass = modelclass

    def _get_value(self, entity):
        """Override _get_value() to *not* raise UnprojectedPropertyError."""
        value = self._get_user_value(entity)
        if value is None and entity._projection:
            # Invoke super _get_value() to raise the proper exception.
            return super(StructuredProperty, self)._get_value(entity)
        return value

    def __getattr__(self, attrname):
        """Dynamically get a subproperty."""
        # Optimistically try to use the dict key.
        prop = self._modelclass._properties.get(attrname)
        # We're done if we have a hit and _code_name matches.
        if prop is None or prop._code_name != attrname:
            # Otherwise, use linear search looking for a matching _code_name.
            for prop in self._modelclass._properties.values():
                if prop._code_name == attrname:
                    break
            else:
                # This is executed when we never execute the above break.
                prop = None
        if prop is None:
            raise AttributeError('Model subclass %s has no attribute %s' %
                                 (self._modelclass.__name__, attrname))
        prop_copy = copy.copy(prop)
        prop_copy._name = self._name + '.' + prop_copy._name
        # Cache the outcome, so subsequent requests for the same attribute
        # name will get the copied property directly rather than going
        # through the above motions all over again.
        setattr(self, attrname, prop_copy)
        return prop_copy

    def _comparison(self, op, value):
        if op != '=':
            raise datastore_errors.BadFilterError(
                'StructuredProperty filter can only use ==')
        if not self._indexed:
            raise datastore_errors.BadFilterError(
                'Cannot query for unindexed StructuredProperty %s' % self._name)
        # Import late to avoid circular imports.
        from google.appengine.ext.ndb.query import ConjunctionNode, PostFilterNode
        from google.appengine.ext.ndb.query import RepeatedStructuredPropertyPredicate
        if value is None:
            from google.appengine.ext.ndb.query import FilterNode  # Import late to avoid circular imports.
            return FilterNode(self._name, op, value)
        value = self._do_validate(value)
        value = self._call_to_base_type(value)
        filters = []
        match_keys = []
        # TODO: Why not just iterate over value._values?
        for prop in self._modelclass._properties.itervalues():
            vals = prop._get_base_value_unwrapped_as_list(value)
            if prop._repeated:
                if vals:
                    raise datastore_errors.BadFilterError(
                        'Cannot query for non-empty repeated property %s' % prop._name)
                continue
            assert isinstance(vals, list) and len(vals) == 1, repr(vals)
            val = vals[0]
            if val is not None:
                altprop = getattr(self, prop._code_name)
                filt = altprop._comparison(op, val)
                filters.append(filt)
                match_keys.append(altprop._name)
        if not filters:
            raise datastore_errors.BadFilterError(
                'StructuredProperty filter without any values')
        if len(filters) == 1:
            return filters[0]
        if self._repeated:
            pb = value._to_pb(allow_partial=True)
            pred = RepeatedStructuredPropertyPredicate(match_keys, pb,
                                                       self._name + '.')
            filters.append(PostFilterNode(pred))
        return ConjunctionNode(*filters)

    def _IN(self, value):
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise datastore_errors.BadArgumentError(
                'Expected list, tuple or set, got %r' % (value,))
        from google.appengine.ext.ndb.query import DisjunctionNode, FalseNode
        # Expand to a series of == filters.
        filters = [self._comparison('=', val) for val in value]
        if not filters:
            # DisjunctionNode doesn't like an empty list of filters.
            # Running the query will still fail, but this matches the
            # behavior of IN for regular properties.
            return FalseNode()
        else:
            return DisjunctionNode(*filters)

    IN = _IN

    def _validate(self, value):
        if isinstance(value, dict):
            # A dict is assumed to be the result of a _to_dict() call.
            return self._modelclass(**value)
        if not isinstance(value, self._modelclass):
            raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                                 (self._modelclass.__name__, value))

    def _has_value(self, entity, rest=None):
        # rest: optional list of attribute names to check in addition.
        # Basically, prop._has_value(self, ent, ['x', 'y']) is similar to
        #   (prop._has_value(ent) and
        #    prop.x._has_value(ent.x) and
        #    prop.x.y._has_value(ent.x.y))
        # assuming prop.x and prop.x.y exist.
        # NOTE: This is not particularly efficient if len(rest) > 1,
        # but that seems a rare case, so for now I don't care.
        ok = super(StructuredProperty, self)._has_value(entity)
        if ok and rest:
            lst = self._get_base_value_unwrapped_as_list(entity)
            if len(lst) != 1:
                raise RuntimeError('Failed to retrieve sub-entity of StructuredProperty'
                                   ' %s' % self._name)
            subent = lst[0]
            if subent is None:
                return True
            subprop = subent._properties.get(rest[0])
            if subprop is None:
                ok = False
            else:
                ok = subprop._has_value(subent, rest[1:])
        return ok

    def _serialize(self, entity, pb, prefix='', parent_repeated=False,
                   projection=None):
        # entity -> pb; pb is an EntityProto message
        values = self._get_base_value_unwrapped_as_list(entity)
        for value in values:
            if value is not None:
                # TODO: Avoid re-sorting for repeated values.
                for unused_name, prop in sorted(value._properties.iteritems()):
                    prop._serialize(value, pb, prefix + self._name + '.',
                                    self._repeated or parent_repeated,
                                    projection=projection)
            else:
                # Serialize a single None
                super(StructuredProperty, self)._serialize(
                    entity, pb, prefix=prefix, parent_repeated=parent_repeated,
                    projection=projection)

    def _deserialize(self, entity, p, depth=1):
        if not self._repeated:
            subentity = self._retrieve_value(entity)
            if subentity is None:
                subentity = self._modelclass()
                self._store_value(entity, _BaseValue(subentity))
            cls = self._modelclass
            if isinstance(subentity, _BaseValue):
                # NOTE: It may not be a _BaseValue when we're deserializing a
                # repeated structured property.
                subentity = subentity.b_val
            if not isinstance(subentity, cls):
                raise RuntimeError('Cannot deserialize StructuredProperty %s; value '
                                   'retrieved not a %s instance %r' %
                                   (self._name, cls.__name__, subentity))
            # _GenericProperty tries to keep compressed values as unindexed, but
            # won't override a set argument. We need to force it at this level.
            # TODO(pcostello): Remove this hack by passing indexed to _deserialize.
            # This cannot happen until we version the API.
            indexed = p.meaning_uri() != _MEANING_URI_COMPRESSED
            prop = subentity._get_property_for(p, depth=depth, indexed=indexed)
            if prop is None:
                # Special case: kill subentity after all.
                self._store_value(entity, None)
                return
            prop._deserialize(subentity, p, depth + 1)
            return

        # The repeated case is more complicated.
        # TODO: Prove we won't get here for orphans.
        name = p.name()
        parts = name.split('.')
        if len(parts) <= depth:
            raise RuntimeError('StructuredProperty %s expected to find properties '
                               'separated by periods at a depth of %i; received %r' %
                               (self._name, depth, parts))
        next = parts[depth]
        rest = parts[depth + 1:]
        prop = self._modelclass._properties.get(next)
        prop_is_fake = False
        if prop is None:
            # Synthesize a fake property.  (We can't use Model._fake_property()
            # because we need the property before we can determine the subentity.)
            if rest:
                # TODO: Handle this case, too.
                logging.warn('Skipping unknown structured subproperty (%s) '
                             'in repeated structured property (%s of %s)',
                             name, self._name, entity.__class__.__name__)
                return
            # TODO: Figure out the value for indexed.  Unfortunately we'd
            # need this passed in from _from_pb(), which would mean a
            # signature change for _deserialize(), which might break valid
            # end-user code that overrides it.
            compressed = p.meaning_uri() == _MEANING_URI_COMPRESSED
            prop = GenericProperty(next, compressed=compressed)
            prop._code_name = next
            prop_is_fake = True

        # Find the first subentity that doesn't have a value for this
        # property yet.
        if not hasattr(entity, '_subentity_counter'):
            entity._subentity_counter = _NestedCounter()
        counter = entity._subentity_counter
        counter_path = parts[depth - 1:]
        next_index = counter.get(counter_path)
        subentity = None
        if self._has_value(entity):
            # If an entire subentity has been set to None, we have to loop
            # to advance until we find the next partial entity.
            while next_index < self._get_value_size(entity):
                subentity = self._get_base_value_at_index(entity, next_index)
                if not isinstance(subentity, self._modelclass):
                    raise TypeError('sub-entities must be instances '
                                    'of their Model class.')
                if not prop._has_value(subentity, rest):
                    break
                next_index = counter.increment(counter_path)
            else:
                subentity = None
        # The current property is going to be populated, so advance the counter.
        counter.increment(counter_path)
        if not subentity:
            # We didn't find one.  Add a new one to the underlying list of
            # values.
            subentity = self._modelclass()
            values = self._retrieve_value(entity, self._default)
            if values is None:
                self._store_value(entity, [])
                values = self._retrieve_value(entity, self._default)
            values.append(_BaseValue(subentity))
        if prop_is_fake:
            # Add the synthetic property to the subentity's _properties
            # dict, so that it will be correctly deserialized.
            # (See Model._fake_property() for comparison.)
            subentity._clone_properties()
            subentity._properties[prop._name] = prop
        prop._deserialize(subentity, p, depth + 1)

    def _prepare_for_put(self, entity):
        values = self._get_base_value_unwrapped_as_list(entity)
        for value in values:
            if value is not None:
                value._prepare_for_put()

    def _check_property(self, rest=None, require_indexed=True):
        """Override for Property._check_property().

        Raises:
          InvalidPropertyError if no subproperty is specified or if something
          is wrong with the subproperty.
        """
        if not rest:
            raise InvalidPropertyError('Structured property %s requires a subproperty' % self._name)
        self._modelclass._check_properties([rest], require_indexed=require_indexed)

    def _get_base_value_at_index(self, entity, index):
        assert self._repeated
        value = self._retrieve_value(entity, self._default)
        value[index] = self._opt_call_to_base_type(value[index])
        return value[index].b_val

    def _get_value_size(self, entity):
        values = self._retrieve_value(entity, self._default)
        if values is None:
            return 0
        return len(values)


class GeoPtProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if not isinstance(value, GeoPt):
            raise datastore_errors.BadValueError('Expected GeoPt, got %r' %
                                                 (value,))

    def _db_set_value(self, v, p, value):
        if not isinstance(value, GeoPt):
            raise TypeError('GeoPtProperty %s can only be set to GeoPt values; '
                            'received %r' % (self._name, value))
        p.set_meaning(entity_pb.Property.GEORSS_POINT)
        pv = v.mutable_pointvalue()
        pv.set_x(value.lat)
        pv.set_y(value.lon)

    def _db_get_value(self, v, unused_p):
        if not v.has_pointvalue():
            return None
        pv = v.pointvalue()
        return GeoPt(pv.x(), pv.y())


class PickleProperty(BlobProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _to_base_type(self, value):
        return pickle.dumps(value, pickle.HIGHEST_PROTOCOL)

    def _from_base_type(self, value):
        return pickle.loads(value)


class JsonProperty(BlobProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _json_type = None

    @ndb.utils.positional(1 + BlobProperty._positional)
    def __init__(self, name=None, compressed=False, json_type=None, **kwds):
        super(JsonProperty, self).__init__(name=name, compressed=compressed, **kwds)
        self._json_type = json_type

    def _validate(self, value):
        if self._json_type is not None and not isinstance(value, self._json_type):
            raise TypeError('JSON property must be a %s' % self._json_type)

    # Use late import so the dependency is optional.

    def _to_base_type(self, value):
        try:
            import json
        except ImportError:
            import simplejson as json
        return json.dumps(value)

    def _from_base_type(self, value):
        try:
            import json
        except ImportError:
            import simplejson as json
        return json.loads(value)


class UserProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _attributes = ResourceProperty._attributes + ['_auto_current_user', '_auto_current_user_add']

    _auto_current_user = False
    _auto_current_user_add = False

    @ndb.utils.positional(1 + ResourceProperty._positional)
    def __init__(self, name=None, auto_current_user=False,
                 auto_current_user_add=False, **kwds):
        super(UserProperty, self).__init__(name=name, **kwds)
        # TODO: Disallow combining auto_current_user* and default?
        if self._repeated:
            if auto_current_user:
                raise ValueError('UserProperty could use auto_current_user and be '
                                 'repeated, but there would be no point.')
            elif auto_current_user_add:
                raise ValueError('UserProperty could use auto_current_user_add and be '
                                 'repeated, but there would be no point.')
        self._auto_current_user = auto_current_user
        self._auto_current_user_add = auto_current_user_add

    def _validate(self, value):
        if not isinstance(value, users.User):
            raise datastore_errors.BadValueError('Expected User, got %r' %
                                                 (value,))

    def _prepare_for_put(self, entity):
        if (self._auto_current_user or
                (self._auto_current_user_add and not self._has_value(entity))):
            value = users.get_current_user()
            if value is not None:
                self._store_value(entity, value)

    def _db_set_value(self, v, p, value):
        datastore_types.PackUser(p.name(), value, v)

    def _db_get_value(self, v, unused_p):
        if not v.has_uservalue():
            return None
        return _unpack_user(v)


class KeyProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _attributes = ResourceProperty._attributes + ['_kind']

    _kind = None

    @ndb.utils.positional(2 + ResourceProperty._positional)
    def __init__(self, *args, **kwds):
        # Support several positional signatures:
        # ()  =>  name=None, kind from kwds
        # (None)  =>  name=None, kind from kwds
        # (name)  =>  name=arg 0, kind from kwds
        # (kind)  =>  name=None, kind=arg 0
        # (name, kind)  => name=arg 0, kind=arg 1
        # (kind, name)  => name=arg 1, kind=arg 0
        # The positional kind must be a Model subclass; it cannot be a string.
        name = kind = None

        for arg in args:
            if isinstance(arg, basestring):
                if name is not None:
                    raise TypeError('You can only specify one name')
                name = arg
            elif isinstance(arg, type) and issubclass(arg, Model):
                if kind is not None:
                    raise TypeError('You can only specify one kind')
                kind = arg
            elif arg is not None:
                raise TypeError('Unexpected positional argument: %r' % (arg,))

        if name is None:
            name = kwds.pop('name', None)
        elif 'name' in kwds:
            raise TypeError('You can only specify name once')

        if kind is None:
            kind = kwds.pop('kind', None)
        elif 'kind' in kwds:
            raise TypeError('You can only specify kind once')

        if kind is not None:
            if isinstance(kind, type) and issubclass(kind, Model):
                kind = kind._get_kind()
            if isinstance(kind, unicode):
                kind = kind.encode('utf-8')
            if not isinstance(kind, str):
                raise TypeError('kind must be a Model class or a string')

        super(KeyProperty, self).__init__(name, **kwds)

        self._kind = kind

    def _datastore_type(self, value):
        return datastore_types.Key(value.urlsafe())

    def _validate(self, value):
        if not isinstance(value, Key):
            raise datastore_errors.BadValueError('Expected Key, got %r' % (value,))
        # Reject incomplete keys.
        if not value.id():
            raise datastore_errors.BadValueError('Expected complete Key, got %r' %
                                                 (value,))
        if self._kind is not None:
            if value.kind() != self._kind:
                raise datastore_errors.BadValueError(
                    'Expected Key with kind=%r, got %r' % (self._kind, value))

    def _db_set_value(self, v, unused_p, value):
        if not isinstance(value, Key):
            raise TypeError('KeyProperty %s can only be set to Key values; '
                            'received %r' % (self._name, value))
        # See datastore_types.PackKey
        ref = value.reference()
        rv = v.mutable_referencevalue()  # A Reference
        rv.set_app(ref.app())
        if ref.has_name_space():
            rv.set_name_space(ref.name_space())
        for elem in ref.path().element_list():
            rv.add_pathelement().CopyFrom(elem)

    def _db_get_value(self, v, unused_p):
        if not v.has_referencevalue():
            return None
        ref = entity_pb.Reference()
        rv = v.referencevalue()
        if rv.has_app():
            ref.set_app(rv.app())
        if rv.has_name_space():
            ref.set_name_space(rv.name_space())
        path = ref.mutable_path()
        for elem in rv.pathelement_list():
            path.add_element().CopyFrom(elem)
        return Key(reference=ref)


class BlobKeyProperty(ResourceProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def _validate(self, value):
        if not isinstance(value, datastore_types.BlobKey):
            raise datastore_errors.BadValueError('Expected BlobKey, got %r' %
                                                 (value,))

    def _db_set_value(self, v, p, value):
        if not isinstance(value, datastore_types.BlobKey):
            raise TypeError('BlobKeyProperty %s can only be set to BlobKey values; '
                            'received %r' % (self._name, value))
        p.set_meaning(entity_pb.Property.BLOBKEY)
        v.set_stringvalue(str(value))

    def _db_get_value(self, v, unused_p):
        if not v.has_stringvalue():
            return None
        return datastore_types.BlobKey(v.stringvalue())


ComputedPropertyError = ndb.model.ComputedPropertyError


class ComputedProperty(GenericProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    def __init__(self, func, name=None, indexed=None,
                 repeated=None, verbose_name=None):
        """Constructor.

        Args:
          func: A function that takes one argument, the model instance, and returns
                a calculated value.
        """
        super(ComputedProperty, self).__init__(name=name, indexed=indexed,
                                               repeated=repeated,
                                               verbose_name=verbose_name)
        self._func = func

    def _set_value(self, entity, value):
        raise ComputedPropertyError("Cannot assign to a ComputedProperty")

    def _delete_value(self, entity):
        raise ComputedPropertyError("Cannot delete a ComputedProperty")

    def _get_value(self, entity):
        # About projections and computed properties: if the computed
        # property itself is in the projection, don't recompute it; this
        # prevents raising UnprojectedPropertyError if one of the
        # dependents is not in the projection.  However, if the computed
        # property is not in the projection, compute it normally -- its
        # dependents may all be in the projection, and it may be useful to
        # access the computed value without having it in the projection.
        # In this case, if any of the dependents is not in the projection,
        # accessing it in the computation function will raise
        # UnprojectedPropertyError which will just bubble up.
        if entity._projection and self._name in entity._projection:
            return super(ComputedProperty, self)._get_value(entity)
        value = self._func(entity)
        self._store_value(entity, value)
        return value

    def _prepare_for_put(self, entity):
        self._get_value(entity)  # For its side effects.


class LocalStructuredProperty(_StructuredGetForDictMixin, BlobProperty):
    """This is exactly the same implementation as the NDB SDK, we just need to inherit from a different base class."""

    _indexed = False
    _modelclass = None
    _keep_keys = False

    _attributes = ['_modelclass'] + BlobProperty._attributes + ['_keep_keys']
    _positional = 1 + BlobProperty._positional  # Add modelclass as positional.

    @ndb.utils.positional(1 + _positional)
    def __init__(self, modelclass,
                 name=None, compressed=False, keep_keys=False,
                 **kwds):
        super(LocalStructuredProperty, self).__init__(name=name,
                                                      compressed=compressed,
                                                      **kwds)
        if self._indexed:
            raise NotImplementedError('Cannot index LocalStructuredProperty %s.' %
                                      self._name)
        self._modelclass = modelclass
        self._keep_keys = keep_keys

    def _validate(self, value):
        if isinstance(value, dict):
            # A dict is assumed to be the result of a _to_dict() call.
            return self._modelclass(**value)
        if not isinstance(value, self._modelclass):
            raise datastore_errors.BadValueError('Expected %s instance, got %r' %
                                                 (self._modelclass.__name__, value))

    def _to_base_type(self, value):
        if isinstance(value, self._modelclass):
            pb = value._to_pb(set_key=self._keep_keys)
            return pb.SerializePartialToString()

    def _from_base_type(self, value):
        if not isinstance(value, self._modelclass):
            pb = entity_pb.EntityProto()
            pb.MergePartialFromString(value)
            if not self._keep_keys:
                pb.clear_key()
            return self._modelclass._from_pb(pb)

    def _prepare_for_put(self, entity):
        # TODO: Using _get_user_value() here makes it impossible to
        # subclass this class and add a _from_base_type().  But using
        # _get_base_value() won't work, since that would return
        # the serialized (and possibly compressed) serialized blob.
        value = self._get_user_value(entity)
        if value is not None:
            if self._repeated:
                for subent in value:
                    if subent is not None:
                        subent._prepare_for_put()
            else:
                value._prepare_for_put()

    def _db_set_uncompressed_meaning(self, p):
        p.set_meaning(entity_pb.Property.ENTITY_PROTO)


class Resource(ndb.Expando):
    _uniques = None

    def __init__(self, *args, **kwargs):
        self._init_complete = False
        self._uniques_modified = []
        self._history = {}
        super(Resource, self).__init__(*args, **kwargs)
        self._init_complete = True

    @classmethod
    def _fix_up_properties(cls):
        """Most of the work is done in super. Here we simply wrap the super method and provide some additional
        functionality to setup the unique value checking within a resource

        Note: This is called by MetaModel, but may also be called manually
        after dynamically updating a model class.
        """
        super(Resource, cls)._fix_up_properties()
        cls._uniques = []  # Map of {name: Property}

        if cls.__module__ == __name__:  # Skip the classes in *this* file.
            return
        for name in set(dir(cls)):
            attr = getattr(cls, name, None)
            if isinstance(attr, ResourceProperty):
                try:
                    if attr._unique:
                        cls._uniques.append(attr._name)
                except AttributeError:
                    pass
        
    @property
    def uid(self):
        try:
            return self.key.urlsafe()
        except AttributeError:
            return None

    def to_dict(self):
        result = super(Resource, self).to_dict()
        try:
            result['uid'] = self.key.urlsafe()
        except AttributeError:
            # The datastore model has no key attribute, likely because it is a new instance and has not been
            # inserted into the datastore yet.
            pass

        return result


class UniqueValue(ndb.Model):
    pass


class NDBMethod(object):
    def __init__(self, code_name, resource_model, uniques_value_model=UniqueValue, force_unique_parse=False):
        self.code_name = code_name
        self.resource_model = resource_model
        self.uniques_value_model = uniques_value_model
        self.force_unique_parse = force_unique_parse

        self.pre_name = 'pre_{}'.format(self.code_name)
        self.post_name = 'post_{}'.format(self.code_name)

        self.pre_signal = signal(self.pre_name)
        self.post_signal = signal(self.post_name)

    def _transaction_receivers(self):
        return bool(self.pre_signal.receivers) or bool(self.post_signal.recievers)

    @async
    def _trigger_hook(self, signal, **kwargs):
        if bool(signal.receivers):
            kwargs['hook_name'] = signal.name
            for receiver in signal.receivers_for(self):
                yield receiver(self, **kwargs)

    def _build_unique_value_locks(self, uniques):
        """
        Generate unique model instances for each property=>value pair in uniques. Returns instances of
        self.unique_value_model, ready for commit in transaction

        :param uniques:
        :return unique_locks:
        """
        base_unique_key = u'{}.'.format(self.resource_model.__name__)

        return [ndb.Key(self.uniques_value_model, u'{}{}.{}'.format(base_unique_key, unique, value)) for unique, value in uniques.iteritems()]

    def _parse_resource_unique_values(self, resource):
        """
        Compile unique value name pairs from a resource object

        :param resource:
        :param force:
        :return:
        """
        if not resource._uniques:
            return None, None

        uniques = {}
        old_values = {}
        for unique in resource._uniques:
            if unique in resource._uniques_modified or self.force_unique_parse:
                value = getattr(resource, unique)
                if value:
                    uniques[unique] = value
                    try:
                        old_values[unique] = resource._history[unique][0]
                    except KeyError:
                        # There is no old value
                        pass

        return self._build_unique_value_locks(uniques=uniques), old_values if uniques else None, None

    @async
    def _check_unique_locks(self, uniques):
        """
        Run a check on the uniques to see if they are already present. We do this outside of a transaction
        to minimise the overhead of starting a transaction and failing.

        :param uniques:
        :return:
        """

        if uniques:
            existing = yield ndb.get_multi_async(uniques)

            existing_keys = [name.split('.', 2)[1] for name in [k.id() for k in uniques if k in existing]]

            if existing_keys:
                raise UniqueValueRequired(errors=existing_keys)

    @async
    def _create_unique_lock(self, unique_lock):
        unique_lock_exists = yield unique_lock.get_async()
        if not unique_lock_exists:
            inserted = yield self.uniques_value_model(key=unique_lock).put_async()
            raise ndb.Return(inserted)
        else:
            raise ndb.Return(None)

    @async
    def _create_unique_locks(self, uniques):
        """
        Create unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """

        if uniques:
            created = yield map(self._create_unique_lock, uniques)

            if created != uniques:
                raise UniqueValueRequired(errors=[name.split('.', 2)[1] for name in [k.id() for k in uniques if k not in created]])

    @async
    def _delete_unique_locks(self, uniques):
        """
        Delete unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """
        if uniques:
            yield ndb.delete_multi_async(uniques)

    @async
    def _internal_op(self, **kwargs):
        raise NotImplementedError

    @async
    def __call__(self, **kwargs):
        """
        API methods are very simple. They simply emit signal which you can receive and act upon. By default
        there are two signals: pre and post.

        :param kwargs:
        :return:
        """
        try:
            uniques, old_uniques = self._parse_resource_unique_values(resource=kwargs['resource'])
        except KeyError:
            uniques = None
            old_uniques = None
        else:
            if uniques:
                # Pre-check the uniques before setting up a transaction
                self._check_unique_locks(uniques=uniques)

        if self._transaction_receivers() or uniques:
            result = yield ndb.transaction_async(lambda: self._internal_op(uniques=uniques,
                                                                           old_uniques=old_uniques,
                                                                           **kwargs))
        else:
            result = yield self._internal_op(**kwargs)

        raise ndb.Return(result)


class NDBInsert(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'insert'
        kwargs['force_unique_parse'] = True
        super(NDBInsert, self).__init__(**kwargs)

    def _internal_op(self, datastore_model, uniques, **kwargs):
        """
        Insert model into the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        uniques will either be a list of ndb keys or None

        :param datastore_model:
        :param uniques:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['datastore_model'] = datastore_model
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        # This might raise an exception, but it doesn't return a value. If uniques are supplied then we are
        # automatically in a transaction
        if uniques:
            yield self._create_unique_locks(uniques=uniques)

        result = yield datastore_model.put_async(**kwargs)

        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)

        raise ndb.Return(result)


class NDBGet(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'get'
        kwargs['force_unique_parse'] = False
        super(NDBGet, self).__init__(**kwargs)

    def _internal_op(self, datastore_key, **kwargs):
        """
        Get model from the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        :param datastore_key:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['datastore_key'] = datastore_key
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        result = yield datastore_key.get_async(**kwargs)

        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)

        raise ndb.Return(result)


class NDBUpdate(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'update'
        kwargs['force_unique_parse'] = False
        super(NDBUpdate, self).__init__(**kwargs)

    def _internal_op(self, datastore_model, uniques, old_uniques, **kwargs):
        """
        Update model in the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        uniques will either be a list of ndb keys or None

        :param datastore_model:
        :param uniques:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['datastore_model'] = datastore_model
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        # This might raise an exception, but it doesn't return a value. If uniques are supplied then we are
        # automatically in a transaction
        if uniques:
            yield self._create_unique_locks(uniques=uniques)
            # create_unique_locks will either execute successfully or raise an exception, so we don't need it
            # to return a result
            if old_uniques:
                # this should probably be done via the task queue -- it doesn't need to be real time
                yield self._delete_unique_locks(uniques=old_uniques)

        result = yield datastore_model.put_async(**kwargs)

        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)

        raise ndb.Return(result)


class NDBDelete(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'delete'
        kwargs['force_unique_parse'] = True
        super(NDBDelete, self).__init__(**kwargs)

    def _internal_op(self, datastore_key, uniques, **kwargs):
        """
        Delete model from the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        :param datastore_key:
        :param kwargs:
        """
        kwargs['datastore_key'] = datastore_key
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        if uniques:
            yield self._delete_unique_locks(uniques=uniques)

        result = yield datastore_key.delete_async(**kwargs)

        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)

        raise ndb.Return(result)


class NDBDatastore(object):
    """
    NDB Datastore Interface. Sets up async, transaction supported, NDB methods with support for signals. This allows
    other API components to hook into datastore ops.

    If you do hook into a datastore op, understand that you wil trigger
    the op to run as a transaction. You should therefore limit the work that you do when receiving the signal. Also
    understand that there is a performance impact with running a transaction -- you should limit your use to essential
    tasks only.

    You will almost always be better off hooking into the API at a higher level, where you can run code outside of a
    transaction.

    """

    def __init__(self, resource_model, unwanted_resource_kwargs=None, *args, **kwargs):
        self._datastore_model = None
        # These are the built in resource attributes that we don't want to persist to NDB
        default_unwanted_kwargs = ['uniques_modified', 'immutable', 'track_unique_modifications', '_history',
                                   '_history_tracking']

        if unwanted_resource_kwargs is not None:
            default_unwanted_kwargs = default_unwanted_kwargs + unwanted_resource_kwargs

        self._unwanted_resource_kwargs = default_unwanted_kwargs

        self.insert = NDBInsert(resource_model=resource_model)
        self.get = NDBGet(resource_model=resource_model)
        self.update = NDBUpdate(resource_model=resource_model)
        self.delete = NDBDelete(resource_model=resource_model)

    def build_resource_uid(self, desired_id, parent=None, namespace=None, urlsafe=True):
        # TODO: change this to use self.resource_model instead of self.datastore_model.
        if parent and namespace:
            new_key = ndb.Key(self._datastore_model, desired_id, parent=parent, namespace=namespace)
        elif parent:
            new_key = ndb.Key(self._datastore_model, desired_id, parent=parent)
        elif namespace:
            new_key = ndb.Key(self._datastore_model, desired_id, namespace=namespace)
        else:
            new_key = ndb.Key(self._datastore_model, desired_id)

        if urlsafe:
            return new_key.urlsafe()
        else:
            return new_key

    @staticmethod
    def diff_model_properties(source, target):
        """
        Find the differences between two models (excluding the keys).

        :param source:
        :param target:
        :returns set of property names that have changed:
        """
        source_dict = source.to_dict()
        target_dict = target.to_dict()

        if hasattr(source, 'uid'):
            source_dict['uid'] = source.uid
        if hasattr(target, 'uid'):
            target_dict['uid'] = target.uid

        diff = DictDiffer(source_dict, target_dict)

        modified = diff.changed()
        return modified

    @staticmethod
    def update_model(source, target, filtered_properties=None):
        """
        Update target model properties with the values from source.
        Optional filter to update only specific properties.

        :param source:
        :param target:
        :param filtered_properties:
        :returns modified version of target:
        """
        source_dict = source.to_dict()

        if filtered_properties:
            modified_values = {}
            for filtered_property in filtered_properties:
                modified_values[filtered_property] = source_dict[filtered_property]
        else:
            modified_values = source_dict

        if modified_values:
            target.populate(**modified_values)
            return target
        else:
            return False

    def _normalise_output(self, output):
        if isinstance(output, self._datastore_model):
            return self._convert_datastore_model_to_resource(output)
        elif isinstance(output, list) and output and isinstance(output[0], self._datastore_model):
            return map(self._convert_datastore_model_to_resource, output)
        elif isinstance(output, ndb.Key):
            return self._convert_ndb_key_to_string(output)
        else:
            return output

    @staticmethod
    def _convert_ndb_key_to_string(datastore_key):
        return datastore_key.urlsafe()

    def _convert_string_to_ndb_key(self, datastore_key):
        try:
            parsed_key = ndb.Key(urlsafe=datastore_key)
        except ProtocolBuffer.ProtocolBufferDecodeError:
            raise ValueError(u'Specified key is not valid for NDB Datastore.')
        else:
            if parsed_key.kind() != self._datastore_model.__class__.__name__:
                raise TypeError('Only "{}" keys are accepted by this datastore instance'.format(
                    self._datastore_model.__class__.__name__))
            return parsed_key

    def parse_datastore_key(self, resource_uid):
        """
        Convert string to NDB Key.

        :param resource_uid:
        :returns datastore_key:
        """
        return self._convert_string_to_ndb_key(datastore_key=resource_uid)

    @staticmethod
    def _filter_unwanted_kwargs(kwargs, unwanted_keys):
        for unwanted in unwanted_keys:
            try:
                del kwargs[unwanted]
            except KeyError:
                pass

    def _convert_resource_to_datastore_model(self, resource):
        """
        Convert resource object into native ndb model. This is a very crude implementation. It is encouraged
        that you override this in your interface definition to give you maximum flexibility when storing values.

        This implementation is only present so that the interface works 'out of the box'.

        NOTE: If you use either DateProperty or DateTimeProperty with 'auto_now_add' be very careful. This class
        basically puts a new entity in the datastore each time you make an update. Because of this the
        'auto_now_add' timestamp will be overwritten each time. To combat this, mark relevant fields as
        immutable within your resource object so that we can pass the timestamp around without fear of it being
        overwritten. The timestamp can then be written to the new entity without having to fetch the stored
        data each time.

        :param resource:
        :returns ndb model:
        """
        if not isinstance(resource, self._resource_model):
            raise TypeError(
                'Only "{}" models are accepted by this datastore instance'.format(type(self._resource_model)))

        datastore_model_kwargs = resource.as_dict()
        self._filter_unwanted_kwargs(kwargs=datastore_model_kwargs,
                                     unwanted_keys=self._unwanted_resource_kwargs)

        if 'uid' in datastore_model_kwargs:
            del datastore_model_kwargs['uid']
        if resource.uid:
            datastore_model_kwargs['key'] = self._convert_string_to_ndb_key(datastore_key=resource.uid)

        # It is important that we don't accidentally overwrite auto set values in the datastore. Not very
        # efficient so, you could write a bespoke implementation for your model.
        for model_property in self._datastore_model._properties.iteritems():
            property_instance = model_property[1]

            prop_type = type(property_instance)

            prop_name = property_instance._code_name
            if prop_name in datastore_model_kwargs and (
                            prop_type is ndb.model.DateTimeProperty or prop_type is ndb.model.DateProperty):
                if property_instance._auto_now:
                    del datastore_model_kwargs[prop_name]
                if property_instance._auto_now_add and not datastore_model_kwargs[prop_name]:
                    # We only want to remove an auto_now_add property if it is not currently set
                    del datastore_model_kwargs[prop_name]
            elif prop_name in datastore_model_kwargs and prop_type is ndb.model.KeyProperty and \
                            datastore_model_kwargs[prop_name] is not None:
                datastore_model_kwargs[prop_name] = self._convert_string_to_ndb_key(
                    datastore_key=datastore_model_kwargs[prop_name])

        return self._datastore_model(**datastore_model_kwargs)

    def _convert_datastore_model_to_resource(self, datastore_model):
        """
        Convert native ndb model into resource object.

        :param datastore_model:
        :returns resource:
        """
        resource_kwargs = datastore_model.to_dict()
        self._filter_unwanted_kwargs(kwargs=resource_kwargs, unwanted_keys=self._unwanted_resource_kwargs)

        for model_property in datastore_model._properties.iteritems():
            property_instance = model_property[1]

            prop_type = type(property_instance)
            if prop_type is ndb.model.ComputedProperty:
                del resource_kwargs[property_instance._code_name]
            elif prop_type is ndb.model.KeyProperty and resource_kwargs[
                property_instance._code_name] is not None:
                resource_kwargs[property_instance._code_name] = self._convert_ndb_key_to_string(
                    datastore_key=resource_kwargs[property_instance._code_name])

        return self._resource_model(**resource_kwargs)

    def _transaction_log(self):
        pass
