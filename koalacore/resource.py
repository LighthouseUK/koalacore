# -*- coding: utf-8 -*-
"""
    koalacore.resource
    ~~~~~~~~~~~~~~~~~~

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

    The majority of the code in this file is taken from the NDB client
    library from Google. This was necessary to override the base classes
    of the sub properties. Yes, we could have modified the __bases__
    attribute, but that has its own set of problems.
"""
import logging
import datetime
import zlib
import copy
import pickle
import base64
import google.appengine.ext.ndb as ndb
from google.appengine.api import search
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore_errors, datastore_types, users
from .tools import tokenize


# These definitions are just for convenience - it means we don't have to import the gae search lib
# in each of the modules that implement resource properties.
Document = search.Document
update_queue = 'search-index-update'
AtomField = search.AtomField
TextField = search.TextField
HtmlField = search.HtmlField
NumberField = search.NumberField
DateField = search.DateField
GeoField = search.GeoField
GeoPoint = search.GeoPoint


# TODO: in order for large properties (e.g. text or blob) to be unique they need to be hashed. Need to find a good
# way of hashing the value before creating the unique key.

def _parse_resource_uid_property(resource_uid, field_name=None):
    prefix = ''
    key_id = ''

    if field_name is not None:
        prefix = '{}_'.format(field_name)

    try:
        key_id = str(resource_uid.raw.id())
    except AttributeError:
        pass

    return [
        AtomField(name='{}uid'.format(prefix), value=resource_uid.__unicode__()),
        AtomField(name='{}key_id'.format(prefix), value=key_id)
    ]


def _parse_date_property(date_instance, field_name):
    # date = resource_property._get_value()
    return [
        DateField(name='{}'.format(field_name), value=date_instance),
        NumberField(name='{}_year'.format(field_name), value=date_instance.year),
        NumberField(name='{}_month'.format(field_name), value=date_instance.month),
        NumberField(name='{}_day'.format(field_name), value=date_instance.day),
    ]


def _parse_time_property(time_instance, field_name):
    # time = resource_property._get_value().time()
    time_string = time_instance.strftime('%H:%M:%S')
    return [
        AtomField(name='{}'.format(field_name), value=time_string),
        NumberField(name='{}_hour'.format(field_name), value=time_instance.hour),
        NumberField(name='{}_minute'.format(field_name), value=time_instance.minute),
        NumberField(name='{}_second'.format(field_name), value=time_instance.second),
    ]


def _parse_datetime_property(datetime_instance, field_name):
    # datetime = resource_property._get_value()
    time_string = datetime_instance.strftime('%H:%M:%S')
    return [
        DateField(name='{}'.format(field_name), value=datetime_instance),
        AtomField(name='{}_time'.format(field_name), value=time_string),
        NumberField(name='{}_timestamp'.format(field_name), value=(datetime_instance - datetime.datetime(1970, 1, 1)).total_seconds()),
        NumberField(name='{}_hour'.format(field_name), value=datetime_instance.hour),
        NumberField(name='{}_minute'.format(field_name), value=datetime_instance.minute),
        NumberField(name='{}_second'.format(field_name), value=datetime_instance.second),
        NumberField(name='{}_year'.format(field_name), value=datetime_instance.year),
        NumberField(name='{}_month'.format(field_name), value=datetime_instance.month),
        NumberField(name='{}_day'.format(field_name), value=datetime_instance.day),
    ]


class ResourceProperty(ndb.Property):
    _positional = 1  # Only name is a positional argument.
    _attributes = ndb.Property._attributes + ['_immutable', '_unique', '_track_revisions']

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
                entity._uniques_modified.add(self._name)
            if self._track_revisions:
                if self._name in entity._history:
                    entity._history[self._name] = (entity._history[self._name][0], value)
                else:
                    entity._history[self._name] = (getattr(entity, self._name, None), value)

        super(ResourceProperty, self).__set__(entity=entity, value=value)

    def _to_search_property(self, field_name, value):
        """
        This method will be called with each of the values stored internally (if repeated; otherwise it will be
        called once)

        Should return a list of search properties (a single value can have multiple search fields e.g. time can be split
        into hour, minute, second). By default we return an empty list so that unsupported properties can be added
        without throwing errors unnecessarily.

        :param field_name:
        :param value:
        :return:
        """
        return []

    def _to_searchable_properties(self, entity):
        search_properties = []
        value = self._get_for_dict(entity=entity)
        if isinstance(value, list):
            for item in value:
                search_properties += self._to_search_property(field_name=self._code_name, value=item)
        else:
            return self._to_search_property(field_name=self._code_name, value=value)
        return search_properties


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

    def _to_search_property(self, field_name, value):
        return [
            NumberField(name=field_name, value=value)
        ]


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

    def _to_search_property(self, field_name, value):
        return [
            NumberField(name=field_name, value=value)
        ]


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

    def _to_search_property(self, field_name, value):
        return [
            AtomField(name=field_name, value='Yes' if value else 'No')
        ]


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
    _attributes = ResourceProperty._attributes + ['_fuzzy', '_strip', '_complex_fuzzy']

    @ndb.utils.positional(1 + ResourceProperty._positional)  # Add 1 for self.
    def __init__(self, name=None, strip_whitespace=True, fuzzy_search_support=False, complex_fuzzy=False, **kwargs):
        super(TextProperty, self).__init__(name=name, **kwargs)
        self._strip = strip_whitespace
        self._fuzzy = fuzzy_search_support
        self._complex_fuzzy = complex_fuzzy

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

        super(TextProperty, self).__set__(entity=entity, value=value)

    def _to_search_property(self, field_name, value):
        search_properties = [
            TextField(name=field_name, value=value)
        ]

        if self._fuzzy:
            search_properties.append(TextField(name='fuzzy_{}'.format(field_name),
                                                      value=tokenize(input_string=value, complex=self._complex_fuzzy)))
        return search_properties


class StringProperty(TextProperty):
    _indexed = True
    _attributes = TextProperty._attributes + ['_lower']

    @ndb.utils.positional(1 + ResourceProperty._positional)  # Add 1 for self.
    def __init__(self, name=None, force_lowercase=False, **kwargs):
        super(StringProperty, self).__init__(name=name, **kwargs)
        self._lower = force_lowercase

    def __set__(self, entity, value):
        """
        If you look at the documentation for the validation attribute of a property you will see that they allow you to
        coerce values inside a validation function. I find this to be somewhat unintuitive and prefer the validators
        to not modify the submitted values. Hence the addition of the `strip_whitespace` and `force_lowercase` init
        args.
        """

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

        super(StringProperty, self).__set__(entity=entity, value=value)

    def _to_search_property(self, field_name, value):
        search_properties = [
            AtomField(name=field_name, value=value)
        ]

        if self._fuzzy:
            search_properties.append(TextField(name='fuzzy_{}'.format(field_name), value=tokenize(input_string=value, complex=self._complex_fuzzy)))
        return search_properties


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

    def _to_search_property(self, field_name, value):
        return _parse_datetime_property(datetime_instance=value, field_name=field_name)


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

    def _to_search_property(self, field_name, value):
        return _parse_date_property(date_instance=value, field_name=field_name)


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

    def _to_search_property(self, field_name, value):
        return _parse_time_property(time_instance=value, field_name=field_name)


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

    def _to_search_property(self, field_name, value):
        return [
            GeoField(name=field_name, value=GeoPoint(latitude=value.lat, longitude=value.lon))
        ]


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


class ResourceUIDProperty(ResourceProperty):
    """
    This is almost exactly the same implementation as the KeyProperty in the NDB SDK, we just need to inherit from a
    different base class, and modify the accepted/return values. The rest of the functionality is the same -- the stored
    value is an NDB key instance(s)
    """

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

        super(ResourceUIDProperty, self).__init__(name, **kwds)

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

    @staticmethod
    def _convert_to_resource_uids(raw):
        if isinstance(raw, list):
            return [ResourceUID(raw=key) for key in raw]
        else:
            return ResourceUID(raw=raw)

    def _get_for_dict(self, entity):
        """
        Converts the returned value into ResourceUID instances
        """
        return self._convert_to_resource_uids(raw=super(ResourceUIDProperty, self)._get_for_dict(entity=entity))

    def __get__(self, entity, unused_cls=None):
        """Descriptor protocol: get the value from the entity."""
        if entity is None:
            return self  # __get__ called on class
        return self._convert_to_resource_uids(raw=self._get_value(entity))

    def __set__(self, entity, value):
        """Descriptor protocol: set the value on the entity."""
        if isinstance(value, (list, tuple, set, frozenset)):
            self._set_value(entity, [uid.raw for uid in value])
        else:
            self._set_value(entity, value.raw)

    def _to_search_property(self, field_name, value):
        return _parse_resource_uid_property(resource_uid=value, field_name=field_name)


class BlobKeyProperty(ResourceProperty):
    # TODO: this needs to be converted to use the ResourceUID class so that we have a unified interface across resources
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
    _uniques = []
    _default_indexed = False

    def __init__(self, *args, **kwargs):
        """
        Warnings:

        - _history is at the request level. Any change you make during the request will be recorded here as a mapping of
        initial value => most recent modification. It *will not* reset when you execute an NDB put. Therefore you should
        not rely on this for transactional change tracking. Instead, when a put is completed you should record the new
        value separately and then reconstruct the history with a query.

        - _uniques_modified is at the request level. If you execute multiple put operations within the same request
        (modify, put, modify, put) you might get unexpected behaviour -- the put may fail because a unique property is
        still shown as modified; the unique lock operation will fail because the unique value is already in
        the datastore.

        :param args:
        :param kwargs:
        """
        self._init_complete = False
        self._uniques_modified = set()
        self._history = {}
        super(Resource, self).__init__(*args, **kwargs)
        self._init_complete = True

    @classmethod
    def _fix_up_properties(cls):
        """Fix up the properties by calling their _fix_up() method.

        Note: This is called by MetaModel, but may also be called manually
        after dynamically updating a model class.
        """
        # Verify that _get_kind() returns an 8-bit string.
        kind = cls._get_kind()
        if not isinstance(kind, basestring):
            raise ndb.KindError('Class %s defines a _get_kind() method that returns '
                                'a non-string (%r)' % (cls.__name__, kind))
        if not isinstance(kind, str):
            try:
                kind = kind.encode('ascii')  # ASCII contents is okay.
            except UnicodeEncodeError:
                raise ndb.KindError('Class %s defines a _get_kind() method that returns '
                                    'a Unicode string (%r); please encode using utf-8' %
                                    (cls.__name__, kind))
        cls._properties = {}  # Map of {name: Property}
        if cls.__module__ == __name__:  # Skip the classes in *this* file.
            return
        for name in set(dir(cls)):
            attr = getattr(cls, name, None)
            if isinstance(attr, ndb.ModelAttribute) and not isinstance(attr, ndb.ModelKey):
                if name.startswith('_'):
                    raise TypeError('ModelAttribute %s cannot begin with an underscore '
                                    'character. _ prefixed attributes are reserved for '
                                    'temporary Model instance values.' % name)
                attr._fix_up(cls, name)
                if isinstance(attr, ResourceProperty):
                    if (attr._repeated or
                            (isinstance(attr, StructuredProperty) and
                                 attr._modelclass._has_repeated)):
                        cls._has_repeated = True
                    cls._properties[attr._name] = attr
                    try:
                        if attr._unique:
                            cls._uniques.append(attr._name)
                    except AttributeError:
                        pass
        cls._update_kind_map()

    @property
    def uid(self):
        try:
            return ResourceUID(raw=self.key)
        except (AttributeError, ValueError):
            return None

    @ndb.utils.positional(1)
    def to_dict(self, **kwargs):
        result = super(Resource, self).to_dict(**kwargs)
        try:
            result['uid'] = self.uid
        except AttributeError:
            result['uid'] = None
            # The datastore model has no key attribute, likely because it is a new instance and has not been
            # inserted into the datastore yet.
            pass
        except ValueError:
            result['uid'] = None
            # The model key is None and so init of ResourceUID fails
            pass

        return result

    @ndb.utils.positional(1)
    def to_searchable_properties(self, include=None, exclude=None):
        """Return search document properties generated from the resource's properties.

            This should only be run on non-projected models.

            Args:
              include: Optional set of property names to include, default all.
              exclude: Optional set of property names to skip, default none.
                A name contained in both include and exclude is excluded.
            """
        if (include is not None and
                not isinstance(include, (list, tuple, set, frozenset))):
            raise TypeError('include should be a list, tuple or set')
        if (exclude is not None and
                not isinstance(exclude, (list, tuple, set, frozenset))):
            raise TypeError('exclude should be a list, tuple or set')

        searchable_properties = []

        if exclude is None or (exclude is not None and 'uid' not in exclude):
            # Technically this could throw an AttributeError -- search docs should only be created for
            # resources with a UID.
            # We include the urlsafe version of the resource UID as well as the NDB key ID. This allows us to search
            # for a specific ID -- searching is not case-sensitive but the urlsafe resource UIDs are, so you get
            # erroneous search results unless you search by the key ID specifically.
            searchable_properties += _parse_resource_uid_property(resource_uid=self.uid)

        for prop in self._properties.itervalues():
            name = prop._code_name
            if include is not None and name not in include:
                continue
            if exclude is not None and name in exclude:
                continue
            searchable_properties += prop._to_searchable_properties(self)

        return searchable_properties

    @ndb.utils.positional(1)
    def to_search_doc(self, include=None, exclude=None):
        """Return a search document generated from the resource's properties.

            This should only be run on non-projected models.

            Args:
              include: Optional set of property names to include, default all.
              exclude: Optional set of property names to skip, default none.
                A name contained in both include and exclude is excluded.
            """
        return Document(
            doc_id=self.uid.__unicode__,
            fields=self.to_searchable_properties(include=include, exclude=exclude)
        )


class ResourceMock(Resource):
    pass


class BaseResourceUID(object):
    def __init__(self, urlsafe=None, raw=None):
        if urlsafe is None and raw is None:
            raise ValueError('You must supply either a raw resource uid or a urlsafe equivalent')

        if urlsafe:
            self.raw = self._parse_urlsafe(urlsafe=urlsafe)

        if raw:
            self.raw = raw

    def _parse_urlsafe(self, urlsafe):
        raise NotImplementedError('Please implement this method')

    def _serialize(self):
        raise NotImplementedError('Please implement this method')

    def urlsafe(self):
        urlsafe = base64.b64encode(self._serialize())
        return urlsafe.rstrip('=').replace('+', '-').replace('/', '_')

    def __str__(self):
        return self.urlsafe()

    def __unicode__(self):
        return unicode(self.__str__(), 'utf-8')

    def __eq__(self, other):
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            return self.raw == other.raw
        return NotImplemented

    def __ne__(self, other):
        """Define a non-equality test"""
        if isinstance(other, self.__class__):
            return not self.__eq__(other=other)
        return NotImplemented

    def __hash__(self):
        """Override the default hash behavior (that returns the id or the object)"""
        return self.raw.__hash__()


class ResourceUID(BaseResourceUID):
    """
    This is the default resource UID implementation for NDB resources. You can implement other implementations that
    inherit from BaseResourceUID to fit your needs. The main goal is to unify the __str__ value to be used by clients,
    without leaking implementation details.
    """
    def _parse_urlsafe(self, urlsafe):
        return ndb.Key(urlsafe=urlsafe)

    def _serialize(self):
        return self.raw.serialized()
