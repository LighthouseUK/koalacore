# -*- coding: utf-8 -*-
"""
    koala.datastore
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import logging
from blinker import signal
import google.appengine.ext.ndb as ndb
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


class NDBModelUtils(object):
    def to_dict(self):
        result = super(NDBModelUtils, self).to_dict()
        try:
            result['uid'] = self.key.urlsafe()
        except AttributeError:
            # The datastore model has no key attribute, likely because it is a new instance and has not been
            # inserted into the datastore yet.
            pass

        return result


class NDBResource(NDBModelUtils, ndb.Expando):
    created = ndb.DateTimeProperty('ndbrc', auto_now_add=True, indexed=False)
    updated = ndb.DateTimeProperty('ndbru', auto_now=True, indexed=False)


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
