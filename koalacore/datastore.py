# -*- coding: utf-8 -*-
"""
    koala.datastore
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import logging
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


class BaseDatastoreInterface(object):
    _datastore_model = None  # Must define!
    _resource_object = None  # Must define!

    @classmethod
    def _internal_insert(cls, datastore_model, **kwargs):
        """
        Insert model into the datastore. This should be defined by the derived class and never overridden!

        :param datastore_model:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _internal_get(cls, datastore_key, **kwargs):
        """
        Get model from datastore. This should be defined by the derived class and never overridden!

        :param datastore_key:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _internal_update(cls, datastore_model, **kwargs):
        """
        Insert updated model into datastore. This should be defined by the derived class and never overridden!

        :param datastore_model:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _internal_patch(cls, datastore_key, delta_update, **kwargs):
        """
        Patch model in the datastore. Delta_update is a dict that maps resource property names to new values.
        This should be defined by the derived class and never overridden!

        If we get this far then it is assumed that all checks have passed and we are clear to make the updates
        to the resource model.

        :param datastore_key:
        :param delta_update:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _internal_delete(cls, datastore_key, **kwargs):
        """
        Delete model from datastore. This should be defined by the derived class and never overridden!

        :param datastore_key:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _internal_list(cls, **kwargs):
        """
        Fetch datastore models. This should be defined by the derived class and never overridden!

        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def insert_async(cls, resource_object, **kwargs):
        """
        Wrapper around _internal_insert. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_object:
        :param kwargs:
        :returns future; resolves to the uid for the inserted model (string):
        """
        uniques, old_uniques = cls._parse_resource_object_unique_values(resource_object=resource_object, force=True)
        if uniques:
            cls._create_unique_locks(uniques=uniques)
        datastore_model = cls._convert_resource_object_to_datastore_model(resource_object=resource_object)
        return cls._internal_insert(datastore_model=datastore_model, **kwargs)

    @classmethod
    def get_async(cls, resource_uid, **kwargs):
        """
        Wrapper around _internal_get. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns future; resolves to resouce_object, or None:
        """
        return cls._internal_get(datastore_key=resource_uid, **kwargs)

    @classmethod
    def update_async(cls, resource_object, **kwargs):
        """
        Wrapper around _internal_update. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_object:
        :param kwargs:
        :returns future; resolves to the uid for the inserted model (string):
        """
        uniques, old_uniques = cls._parse_resource_object_unique_values(resource_object=resource_object)
        if uniques:
            result, errors = cls._create_unique_locks(uniques=uniques)
            if result and old_uniques:
                cls._delete_unique_locks(uniques=old_uniques)
        datastore_model = cls._convert_resource_object_to_datastore_model(resource_object=resource_object)
        return cls._internal_update(datastore_model=datastore_model, **kwargs)

    @classmethod
    def patch_async(cls, resource_uid, delta_update, **kwargs):
        """
        Wrapper around _internal_delta_update. May be overridden.

        - The unique checking is lazy. If you specify a delta update to a property that doesn't actually change
        it's value then you will receive a unique constrain exception. The simple solution it to only pass data
        that has changes - the whole point of a delta update in the first place.

        - Due to the way this method works, you should manually process any unique value locks that you need on a
        property.

        - IMPORTANT! - This method will not delete any old value locks as it doesn't know what the old values are.

        :param resource_uid:
        :param delta_update:
        :param kwargs:
        :returns future; resolves to the uid for the inserted model (string):
        """
        datastore_key = cls.parse_datastore_key(resource_uid=resource_uid)
        cls._create_unique_locks({k: v for k, v in delta_update.iteritems() if k in cls._resource_object._uniques})
        return cls._internal_patch(datastore_key=datastore_key, delta_update=delta_update, **kwargs)

    @classmethod
    def delete_async(cls, resource_uid, **kwargs):
        """
        Wrapper around _internal_delete. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns future:
        """
        return cls._internal_delete(datastore_key=resource_uid, **kwargs)

    @classmethod
    def list_async(cls, **kwargs):
        """
        Wrapper around _internal_list. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param kwargs:
        :returns future; resolves to list (of resource_objects or empty list):
        """
        return cls._internal_list(**kwargs)

    @classmethod
    def parse_insert_async_result(cls, future):
        """
        Evaluates the specified insert future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
        """
        return cls._normalise_output(output=future.get_result())

    @classmethod
    def parse_get_async_result(cls, future):
        """
        Evaluates the specified get future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a resource object, otherwise exception:
        """
        return cls._normalise_output(output=future.get_result())

    @classmethod
    def parse_update_async_result(cls, future):
        """
        Evaluates the specified update future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the updated entity, otherwise exception:
        """
        return cls._normalise_output(output=future.get_result())

    @classmethod
    def parse_patch_async_result(cls, future):
        """
        Evaluates the specified patch future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the patched entity, otherwise exception:
        """
        return cls._normalise_output(output=future.get_result())

    @classmethod
    def parse_delete_async_result(cls, future):
        """
        Evaluates the specified delete future and returns the value, after running it through '_normalise_output'.

        :param future:
        """
        cls._normalise_output(future.get_result())

    @classmethod
    def parse_list_async_result(cls, futures):
        """
        Evaluates the specified list future and returns the value, after running it through '_normalise_output'.

        :param futures:
        :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
        """
        # TODO: modify this to properly support query semantics and results
        return cls._normalise_output(output=futures.get_result())

    @classmethod
    def insert(cls, resource_object, **kwargs):
        """
        Wrapper around insert_async to automatically resolve async future. May be overridden.

        :param resource_object:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        insert_future = cls.insert_async(resource_object=resource_object, **kwargs)
        return cls.parse_insert_async_result(future=insert_future)

    @classmethod
    def get(cls, resource_uid, **kwargs):
        """
        Wrapper around get_async to automatically resolve async future. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns resource_object, or None:
        """
        entity_future = cls.get_async(resource_uid=resource_uid, **kwargs)
        return cls.parse_get_async_result(future=entity_future)

    @classmethod
    def update(cls, resource_object, **kwargs):
        """
        Wrapper around update_async to automatically resolve async future. May be overridden.

        :param resource_object:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        update_future = cls.update_async(resource_object=resource_object, **kwargs)
        return cls.parse_update_async_result(future=update_future)

    @classmethod
    def patch(cls, resource_uid, delta_update, **kwargs):
        """
        Wrapper around patch_async to automatically resolve async future. May be overridden.

        :param resource_uid:
        :param delta_update:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        update_future = cls.patch_async(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
        return cls.parse_update_async_result(future=update_future)

    @classmethod
    def delete(cls, resource_uid, **kwargs):
        """
        Wrapper around delete_async to automatically resolve async future. May be overridden. No return value.

        :param resource_uid:
        :param kwargs:
        """
        delete_future = cls.delete_async(resource_uid=resource_uid, **kwargs)
        cls.parse_delete_async_result(future=delete_future)

    @classmethod
    def list(cls, **kwargs):
        """
        Wrapper around list_async to automatically resolve async future(s). May be overridden.

        :param kwargs:
        :returns list (of resource_objects or empty):
        """
        futures = cls.list_async(**kwargs)
        return cls.parse_list_async_result(futures=futures)

    @classmethod
    def _build_unique_value_keys(cls, uniques):
        """
        Generate unique datastore keys for each property=>value pair in uniques. Return as list of strings

        :param uniques:
        :return unique_keys:
        """
        raise NotImplementedError

    @classmethod
    def _parse_resource_object_unique_values(cls, resource_object, force=False):
        """
        Compile unique value name pairs from a resource object

        :param resource_object:
        :param force:
        :return:
        """
        if not resource_object._uniques:
            return None, None

        uniques = {}
        old_values = {}
        for unique in resource_object._uniques:
            if unique in resource_object._uniques_modified or force:
                value = getattr(resource_object, unique)
                if value:
                    uniques[unique] = value
                    try:
                        old_values[unique] = resource_object._history[unique][0]
                    except KeyError:
                        # There is no old value
                        pass

        return uniques, old_values

    @classmethod
    def _create_unique_locks(cls, uniques):
        """
        Create unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def _delete_unique_locks(cls, uniques):
        """
        Delete unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def parse_datastore_key(cls, resource_uid):
        """
        Derived class should implement. Method to do any parsing necessary to a resource_uid before use.

        :param resource_uid:
        :returns datastore_key:
        """
        return resource_uid

    @classmethod
    def _convert_resource_object_to_datastore_model(cls, resource_object):
        """
        Derived class should implement. Method to convert between koala resource objects and native datastore models.

        :param resource_object:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _convert_datastore_model_to_resource_object(cls, datastore_model):
        """
        Derived class should implement. Method to convert between native datastore models and koala resource objects.

        :param datastore_model:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def _normalise_output(cls, output):
        """
        Convert output to normalised objects (resource objects or string uids).

        :param output:
        :return normalised object (could be list of objects):
        :raises NotImplementedError:
        """
        raise NotImplementedError

    @classmethod
    def get_future_result(cls, future):
        """
        Helper function to call the relevant future resolver method based on the 'method' property of future.

        :param future:
        :raises AttributeError (if future does not have a method property set:
        :returns result of future:
        """
        method = future.method

        if method == 'insert_async':
            return cls.parse_insert_async_result(future)
        elif method == 'get_async':
            return cls.parse_get_async_result(future)
        elif method == 'update_async':
            return cls.parse_update_async_result(future)
        elif method == 'patch_async':
            return cls.parse_patch_async_result(future)
        elif method == 'delete_async':
            return cls.parse_delete_async_result(future)
        elif method == 'list_async':
            return cls.parse_list_async_result(future)

    @classmethod
    def _transaction_log(cls):
        """
        Wrap each SPI method and log calls, with params, and use the fn name as a reference.
        This could form the basis of a data integrity check (a cron job could run to make sure all transactions have
        been processed).

        :raise NotImplementedError:
        """
        # Pass in the object received by the SPI which contains (hopefully) all of the data needed to populate an ndb
        # model. If the object has an ID property then use this to make a key which can be passed to the ndb model
        # constructor.
        # Iterate over the model properties and attempt to set each one using the data from the datastore_model. The
        # same logic could be used to get the data from a WTForms instance.

        # Make this into a decorator and apply it to all methods - intercept the args sent from the API and convert them
        # into ndb models for direct use in the functions. This also allows us to have many different types of backend
        # datastore whilst using largely the same code.
        raise NotImplementedError

try:
    from blinker import signal
except ImportError:
    # Required libraries are not available; skip definition
    logging.debug('Koala: Could not load the Blinker library; skipping remaining datastore definitions.')
    pass
else:
    class EventedDatastoreInterface(BaseDatastoreInterface):
        """
        Important to note that all of the signals defined here apply to every datastore class (they are not unique to
        an instance). Subscribers should specify the sender they wish to subscribe to explicitly.
        """
        # Signal constants
        HOOK_PRE_INSERT = 'hook_pre_insert'
        HOOK_POST_INSERT = 'hook_post_insert'
        HOOK_PRE_GET = 'hook_pre_get'
        HOOK_POST_GET = 'hook_post_get'
        HOOK_PRE_UPDATE = 'hook_pre_update'
        HOOK_POST_UPDATE = 'hook_post_update'
        HOOK_PRE_PATCH = 'hook_pre_patch'
        HOOK_POST_PATCH = 'hook_post_patch'
        HOOK_PRE_DELETE = 'hook_pre_delete'
        HOOK_POST_DELETE = 'hook_post_delete'
        HOOK_PRE_LIST = 'hook_pre_list'
        HOOK_POST_LIST = 'hook_post_list'

        # Insert method hooks and filters
        hook_pre_insert = signal(HOOK_PRE_INSERT)
        _hook_pre_insert_enabled = False
        hook_post_insert = signal(HOOK_POST_INSERT)
        _hook_post_insert_enabled = False

        # Get method hooks and filters
        hook_pre_get = signal(HOOK_PRE_GET)
        _hook_pre_get_enabled = False
        hook_post_get = signal(HOOK_POST_GET)
        _hook_post_get_enabled = False

        # Update method hooks and filters
        hook_pre_update = signal(HOOK_PRE_UPDATE)
        _hook_pre_update_enabled = False
        hook_post_update = signal(HOOK_POST_UPDATE)
        _hook_post_update_enabled = False

        # patch method hooks and filters
        hook_pre_patch = signal(HOOK_PRE_PATCH)
        _hook_pre_patch_enabled = False
        hook_post_patch = signal(HOOK_POST_PATCH)
        _hook_post_patch_enabled = False

        # Delete method hooks and filters
        hook_pre_delete = signal(HOOK_PRE_DELETE)
        _hook_pre_delete_enabled = False
        hook_post_delete = signal(HOOK_POST_DELETE)
        _hook_post_delete_enabled = False

        # List method hooks and filters
        hook_pre_list = signal(HOOK_PRE_LIST)
        _hook_pre_list_enabled = False
        hook_post_list = signal(HOOK_POST_LIST)
        _hook_post_list_enabled = False

        @classmethod
        def insert_async(cls, resource_object, **kwargs):
            """
            Wrapper around the base insert_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_object:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if cls._hook_pre_insert_enabled:
                cls.hook_pre_insert.send(cls, resource_object=resource_object, **kwargs)

            return super(EventedDatastoreInterface, cls).insert_async(resource_object=resource_object, **kwargs)

        @classmethod
        def parse_insert_async_result(cls, future):
            """
            Evaluates the specified insert future and returns the value, after running it through '_normalise_output'.
            Also triggers post insert events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
            """
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_insert_enabled:
                cls.hook_post_insert.send(cls, op_result=op_result, future=future)

            return op_result

        @classmethod
        def get_async(cls, resource_uid, **kwargs):
            """
            Wrapper around the base get_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param kwargs:
            :returns future; resolves to resource_object, or None:
            """

            if cls._hook_pre_get_enabled:
                cls.hook_pre_get.send(cls, resource_uid=resource_uid, **kwargs)

            return super(EventedDatastoreInterface, cls).get_async(resource_uid=resource_uid, **kwargs)

        @classmethod
        def parse_get_async_result(cls, future):
            """
            Evaluates the specified get future and returns the value, after running it through '_normalise_output'.
            Also triggers post get events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a resource object, otherwise exception:
            """
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_get_enabled:
                cls.hook_post_get.send(cls, op_result=op_result, future=future)

            return op_result

        @classmethod
        def update_async(cls, resource_object, **kwargs):
            """
            Wrapper around the base update_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_object:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if cls._hook_pre_update_enabled:
                cls.hook_pre_update.send(cls, resource_object=resource_object, **kwargs)

            return super(EventedDatastoreInterface, cls).update_async(resource_object=resource_object, **kwargs)

        @classmethod
        def parse_update_async_result(cls, future):
            """
            Evaluates the specified update future and returns the value, after running it through '_normalise_output'.
            Also triggers post update events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the updated entity, otherwise exception:
            """
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_update_enabled:
                cls.hook_post_update.send(cls, op_result=op_result, future=future)

            return op_result

        @classmethod
        def patch_async(cls, resource_uid, delta_update, **kwargs):
            """
            Wrapper around the base patch_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param delta_update:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if cls._hook_pre_patch_enabled:
                cls.hook_pre_patch.send(cls, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

            return super(EventedDatastoreInterface, cls).patch_async(resource_uid=resource_uid, delta_update=delta_update, **kwargs)

        @classmethod
        def parse_patch_async_result(cls, future):
            """
            Evaluates the specified patch future and returns the value, after running it through '_normalise_output'.
            Also triggers post patch events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the patched entity, otherwise exception:
            """
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_patch_enabled:
                cls.hook_post_patch.send(cls, op_result=op_result, future=future)

            return op_result

        @classmethod
        def delete_async(cls, resource_uid, **kwargs):
            """
            Wrapper around the base delete_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param kwargs:
            :returns future:
            """
            if cls._hook_pre_delete_enabled:
                cls.hook_pre_delete.send(cls, resource_uid=resource_uid, **kwargs)

            return super(EventedDatastoreInterface, cls).delete_async(resource_uid=resource_uid, **kwargs)

        @classmethod
        def parse_delete_async_result(cls, future):
            """
            Evaluates the specified delete future and returns the value, after running it through '_normalise_output'.
            Also triggers post delete events so that other modules can plugin functionality.
            :param future:
            """
            # There is no return value here but I've added it in to keep the method signatures/signal args consistent
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_delete_enabled:
                cls.hook_post_delete.send(cls, op_result=op_result, future=future)

        @classmethod
        def list_async(cls, **kwargs):
            """
            Wrapper around the base list_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param kwargs:
            :returns future; resolves to list (of resource_objects or empty list):
            """

            if cls._hook_pre_insert_enabled:
                cls.hook_pre_insert.send(cls, **kwargs)

            return super(EventedDatastoreInterface, cls).list_async(**kwargs)

        @classmethod
        def parse_list_async_result(cls, future):
            """
            Evaluates the specified list future and returns the value, after running it through '_normalise_output'.
            Also triggers post list events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
            """
            # TODO: modify this to properly support query semantics and results
            op_result = cls._normalise_output(future.get_result())

            if cls._hook_post_list_enabled:
                cls.hook_post_list.send(cls, op_result=op_result, future=future)

            return op_result

        @classmethod
        def parse_signal_receivers(cls):
            """
            Check for subscribers to each class signal, toggling the enabled flag accordingly. This should be invoked
            by the implementing module rather than via a signal connection (otherwise this class would modify itself
            when the signal is triggered, and therefore affect any classes which inherit from it).
            """

            # Toggle 'insert' hooks
            cls._hook_pre_insert_enabled = bool(cls.hook_pre_insert.receivers)
            cls._hook_post_insert_enabled = bool(cls.hook_post_insert.receivers)

            # Toggle 'get' hooks
            cls._hook_pre_get_enabled = bool(cls.hook_pre_get.receivers)
            cls._hook_post_get_enabled = bool(cls.hook_post_get.receivers)

            # Toggle 'update' hooks
            cls._hook_pre_update_enabled = bool(cls.hook_pre_update.receivers)
            cls._hook_post_update_enabled = bool(cls.hook_post_update.receivers)

            # Toggle 'patch' hooks
            cls._hook_pre_patch_enabled = bool(cls.hook_pre_patch.receivers)
            cls._hook_post_patch_enabled = bool(cls.hook_post_patch.receivers)

            # Toggle 'delete' hooks
            cls._hook_pre_delete_enabled = bool(cls.hook_pre_delete.receivers)
            cls._hook_post_delete_enabled = bool(cls.hook_post_delete.receivers)

            # Toggle 'list' hooks
            cls._hook_pre_list_enabled = bool(cls.hook_pre_list.receivers)
            cls._hook_post_list_enabled = bool(cls.hook_post_list.receivers)

    try:
        import google.appengine.ext.ndb as ndb
        from google.appengine.ext.ndb.google_imports import ProtocolBuffer
    except ImportError:
        # Required libraries are not available; skip definition
        pass
    else:
        class NDBEventedInterface(EventedDatastoreInterface):
            """
            NDB Evented Datastore Interface. Implements the base datastore methods above and adds in some additional
            helpers. Provides a consistent interface to datastores. This particular implementation supports event hooks
            and filters for extending functionality from other modules.

            Inheriting classes must implement the following class attributes:

            _resource_object (This is set by the base SPI class, but may be overridden)
            _datastore_model (Definition of the native datastore model for the SPI)

            TODO: properly implement/handle transactions in async configuration.
            """
            _unwanted_resource_kwargs = ['uniques_modified', 'immutable', 'track_unique_modifications', '_history', '_history_tracking']

            HOOK_TRANSACTION_PRE_INSERT = 'hook_transaction_pre_insert'
            HOOK_TRANSACTION_POST_INSERT = 'hook_transaction_post_insert'
            HOOK_TRANSACTION_PRE_GET = 'hook_transaction_pre_get'
            HOOK_TRANSACTION_POST_GET = 'hook_transaction_post_get'
            HOOK_TRANSACTION_PRE_UPDATE = 'hook_transaction_pre_update'
            HOOK_TRANSACTION_POST_UPDATE = 'hook_transaction_post_update'
            HOOK_TRANSACTION_PRE_DELETE = 'hook_transaction_pre_delete'
            HOOK_TRANSACTION_POST_DELETE = 'hook_transaction_post_delete'

            # Internal Insert hooks (no filters inside transaction)
            _hook_transaction_pre_insert = signal(HOOK_TRANSACTION_PRE_INSERT)
            _hook_transaction_pre_insert_enabled = False
            _hook_transaction_post_insert = signal(HOOK_TRANSACTION_POST_INSERT)
            _hook_transaction_post_insert_enabled = False

            # Internal Get hooks (no filters inside transaction)
            _hook_transaction_pre_get = signal(HOOK_TRANSACTION_PRE_GET)
            _hook_transaction_pre_get_enabled = False
            _hook_transaction_post_get = signal(HOOK_TRANSACTION_POST_GET)
            _hook_transaction_post_get_enabled = False

            # Internal Update hooks (no filters inside transaction)
            _hook_transaction_pre_update = signal(HOOK_TRANSACTION_PRE_UPDATE)
            _hook_transaction_pre_update_enabled = False
            _hook_transaction_post_update = signal(HOOK_TRANSACTION_POST_UPDATE)
            _hook_transaction_post_update_enabled = False

            # Internal Delete hooks (no filters inside transaction)
            _hook_transaction_pre_delete = signal(HOOK_TRANSACTION_PRE_DELETE)
            _hook_transaction_pre_delete_enabled = False
            _hook_transaction_post_delete = signal(HOOK_TRANSACTION_POST_DELETE)
            _hook_transaction_post_delete_enabled = False

            @classmethod
            def build_resource_uid(cls, desired_id, parent=None, namespace=None, urlsafe=True):
                if parent and namespace:
                    new_key = ndb.Key(cls._datastore_model, desired_id, parent=parent, namespace=namespace)
                elif parent:
                    new_key = ndb.Key(cls._datastore_model, desired_id, parent=parent)
                elif namespace:
                    new_key = ndb.Key(cls._datastore_model, desired_id, namespace=namespace)
                else:
                    new_key = ndb.Key(cls._datastore_model, desired_id)

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

            @classmethod
            def _internal_insert(cls, datastore_model, **kwargs):
                """
                Insert model into the ndb datastore. DO NOT OVERRIDE!

                :param datastore_model:
                :param kwargs:
                :returns future (key for the inserted entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_model'] = datastore_model

                if cls._hook_transaction_pre_insert_enabled:
                    cls._hook_transaction_pre_insert.send(cls, **params)

                op_result = datastore_model.put_async(**kwargs)

                if cls._hook_transaction_post_insert_enabled:
                    cls._hook_transaction_post_insert.send(cls, **params)

                op_result.method = 'insert_async'
                op_result.params = params

                return op_result

            @classmethod
            def _internal_get(cls, datastore_key, **kwargs):
                """
                Get a datastore_model from ndb using an ndb key. DO NOT OVERRIDE!

                :param datastore_key:
                :param kwargs:
                :returns future (fetched entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_key'] = datastore_key

                if cls._hook_transaction_pre_get_enabled:
                    cls._hook_transaction_pre_get.send(cls, **params)

                op_result = datastore_key.get_async(**kwargs)

                if cls._hook_transaction_post_get_enabled:
                    cls._hook_transaction_post_get.send(cls, **params)

                op_result.method = 'get_async'
                op_result.params = params

                return op_result

            @classmethod
            def _internal_update(cls, datastore_model, **kwargs):
                """
                Insert updated datastore model into ndb. DO NOT OVERRIDE!

                :param datastore_model:
                :param kwargs:
                :returns future (key for the updated entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_model'] = datastore_model

                if cls._hook_transaction_pre_update_enabled:
                    cls._hook_transaction_pre_update.send(cls, **params)

                op_result = datastore_model.put_async(**kwargs)

                if cls._hook_transaction_post_update_enabled:
                    cls._hook_transaction_post_update.send(cls, **params)

                op_result.method = 'update_async'
                op_result.params = params

                return op_result

            @classmethod
            def _internal_patch(cls, datastore_key, delta_update, **kwargs):
                """
                Delta update model in the datastore. This method runs a transaction. As such you MUST NOT call this
                method as part of another transaction (nested transactions do not work as expected with NDB).
                DO NOT OVERRIDE!

                If we get this far then it is assumed that all checks have passed and we are clear to make the updates
                to the resource model.

                TODO: need to delete old unique value locks

                :param datastore_key:
                :param delta_update:
                :param kwargs:
                :raises NotImplementedError:
                """
                @ndb.transactional_tasklet
                def delta_update_transaction(datastore_key, delta_update):
                    model = yield cls._internal_get(datastore_key=datastore_key)
                    if model is None:
                        yield False

                    resource = cls._normalise_output(model)

                    for property, value in delta_update.iteritems():
                        setattr(resource, property, value)

                    updated_model = cls._convert_resource_object_to_datastore_model(resource_object=resource)

                    updated_model_key = yield cls._internal_update(datastore_model=updated_model)
                    raise ndb.Return(cls._normalise_output(updated_model_key))

                params = {}
                params.update(kwargs)
                params['delta_update'] = delta_update

                op_result = delta_update_transaction(datastore_key=datastore_key, delta_update=delta_update)

                op_result.method = 'patch_async'
                op_result.params = params
                return op_result

            @classmethod
            def _internal_delete(cls, datastore_key, **kwargs):
                """
                Delete datastore_key from ndb. DO NOT OVERRIDE!

                :param datastore_key:
                :param kwargs:
                :returns future (technically there is no return value on success):
                """
                params = {}
                params.update(kwargs)
                params['datastore_key'] = datastore_key

                if cls._hook_transaction_pre_delete_enabled:
                    cls._hook_transaction_pre_delete.send(cls, **params)

                op_result = datastore_key.delete_async(**kwargs)

                if cls._hook_transaction_post_delete_enabled:
                    cls._hook_transaction_post_delete.send(cls, **params)

                op_result.method = 'delete_async'
                op_result.params = params

                return op_result

            @classmethod
            def get_async(cls, resource_uid, **kwargs):
                """
                Wrapper around _internal_get. At its simplest, simply converts between koala resource objects and native
                datastore model. May be overridden. Overriding the base implementation as the resource uid needs to be
                converted to an NDB Key instance.

                :param resource_uid:
                :param kwargs:
                :returns future:
                """
                datastore_key = cls._convert_string_to_ndb_key(resource_uid)
                return cls._internal_get(datastore_key, **kwargs)

            @classmethod
            def delete_async(cls, resource_uid, **kwargs):
                """
                Wrapper around _internal_delete. At its simplest, simply converts between koala resource objects and
                native datastore model. May be overridden. Overriding the base implementation as the resource uid needs
                to be converted to an NDB Key instance.

                :param resource_uid:
                :param kwargs:
                :returns future:
                """
                resource_object = cls.get(resource_uid=resource_uid)
                uniques, old_values = cls._parse_resource_object_unique_values(resource_object=resource_object, force=True)
                if uniques:
                    cls._delete_unique_locks(uniques=uniques)
                datastore_key = cls.parse_datastore_key(resource_uid=resource_uid)
                return cls._internal_delete(datastore_key, **kwargs)

            @classmethod
            def _normalise_output(cls, output):
                if isinstance(output, cls._datastore_model):
                    return cls._convert_datastore_model_to_resource_object(output)
                elif isinstance(output, list) and output and isinstance(output[0], cls._datastore_model):
                    return map(cls._convert_datastore_model_to_resource_object, output)
                elif isinstance(output, ndb.Key):
                    return cls._convert_ndb_key_to_string(output)
                else:
                    return output

            @classmethod
            def _convert_ndb_key_to_string(cls, datastore_key):
                return datastore_key.urlsafe()

            @classmethod
            def _convert_string_to_ndb_key(cls, datastore_key):
                try:
                    return ndb.Key(urlsafe=datastore_key)
                except ProtocolBuffer.ProtocolBufferDecodeError:
                    raise ValueError(u'Specified key is not valid for NDB Datastore.')

            @classmethod
            def parse_datastore_key(cls, resource_uid):
                """
                Convert string to NDB Key.

                :param resource_uid:
                :returns datastore_key:
                """
                return cls._convert_string_to_ndb_key(datastore_key=resource_uid)

            @classmethod
            def _build_unique_value_keys(cls, uniques):
                """
                Generate unique datastore keys for each property=>value pair in uniques. Return as list of strings

                :param uniques:
                :return unique_keys:
                """
                base_unique_key = u'{}.'.format(cls._resource_object.__name__)

                return [u'{}{}.{}'.format(base_unique_key, unique, value) for unique, value in uniques.iteritems()]

            @classmethod
            def _create_unique_locks(cls, uniques):
                """
                Create unique locks from a dict of property=>value pairs

                :param uniques:
                :return:
                """
                if not uniques:
                    return

                unique_keys = cls._build_unique_value_keys(uniques=uniques)

                if not unique_keys:
                    return

                result, errors = NDBUniqueValueModel.create_multi(unique_keys)

                if not result:
                    raise UniqueValueRequired(errors=[name.split('.', 2)[1] for name in errors])

                return result, errors

            @classmethod
            def _delete_unique_locks(cls, uniques):
                """
                Delete unique locks from a dict of property=>value pairs

                :param uniques:
                :return:
                """
                if not uniques:
                    return

                unique_keys = cls._build_unique_value_keys(uniques=uniques)

                if unique_keys:
                    NDBUniqueValueModel.delete_multi(unique_keys)

            @classmethod
            def _filter_unwanted_kwargs(cls, kwargs, unwanted_keys):
                for unwanted in unwanted_keys:
                    try:
                        del kwargs[unwanted]
                    except KeyError:
                        pass

            @classmethod
            def _convert_resource_object_to_datastore_model(cls, resource_object):
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

                :param resource_object:
                :returns ndb model:
                """
                datastore_model_kwargs = resource_object.as_dict()
                cls._filter_unwanted_kwargs(kwargs=datastore_model_kwargs, unwanted_keys=cls._unwanted_resource_kwargs)

                if 'uid' in datastore_model_kwargs:
                    del datastore_model_kwargs['uid']
                if resource_object.uid:
                    datastore_model_kwargs['key'] = cls._convert_string_to_ndb_key(datastore_key=resource_object.uid)

                # It is important that we don't accidentally overwrite auto set values in the datastore. Not very
                # efficient so, you could write a bespoke implementation for your model.
                for model_property in cls._datastore_model._properties.iteritems():
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
                    elif prop_name in datastore_model_kwargs and prop_type is ndb.model.KeyProperty and datastore_model_kwargs[prop_name] is not None:
                        datastore_model_kwargs[prop_name] = cls._convert_string_to_ndb_key(datastore_key=datastore_model_kwargs[prop_name])

                return cls._datastore_model(**datastore_model_kwargs)

            @classmethod
            def _convert_datastore_model_to_resource_object(cls, datastore_model):
                """
                Convert native ndb model into resource object.

                :param datastore_model:
                :returns resource_object:
                """
                resource_object_kwargs = datastore_model.to_dict()
                cls._filter_unwanted_kwargs(kwargs=resource_object_kwargs, unwanted_keys=cls._unwanted_resource_kwargs)

                for model_property in datastore_model._properties.iteritems():
                    property_instance = model_property[1]

                    prop_type = type(property_instance)
                    if prop_type is ndb.model.ComputedProperty:
                        del resource_object_kwargs[property_instance._code_name]
                    elif prop_type is ndb.model.KeyProperty and resource_object_kwargs[property_instance._code_name] is not None:
                        resource_object_kwargs[property_instance._code_name] = cls._convert_ndb_key_to_string(datastore_key=resource_object_kwargs[property_instance._code_name])

                return cls._resource_object(**resource_object_kwargs)

            @classmethod
            def _transaction_log(cls):
                pass

            @classmethod
            def parse_signal_receivers(cls):
                """
                See EventedDatastoreInterface class for doc string. Here we provide additional toggles to support NDB
                transaction events.
                """
                super(NDBEventedInterface, cls).parse_signal_receivers()

                # Toggle 'insert transaction' hooks
                cls._hook_transaction_pre_insert_enabled = bool(cls._hook_transaction_pre_insert.receivers)
                cls._hook_transaction_post_insert_enabled = bool(cls._hook_transaction_post_insert.receivers)

                # Toggle 'get transaction' hooks
                cls._hook_transaction_pre_get_enabled = bool(cls._hook_transaction_pre_get.receivers)
                cls._hook_transaction_post_get_enabled = bool(cls._hook_transaction_post_get.receivers)

                # Toggle 'update transaction' hooks
                cls._hook_transaction_pre_update_enabled = bool(cls._hook_transaction_pre_update.receivers)
                cls._hook_transaction_post_update_enabled = bool(cls._hook_transaction_post_update.receivers)

                # Toggle 'delete transaction' hooks
                cls._hook_transaction_pre_delete_enabled = bool(cls._hook_transaction_pre_delete.receivers)
                cls._hook_transaction_post_delete_enabled = bool(cls._hook_transaction_post_delete.receivers)


        class ModelUtils(object):
            def to_dict(self):
                result = super(ModelUtils, self).to_dict()
                try:
                    result['uid'] = self.key.urlsafe()
                except AttributeError:
                    # The datastore model has no key attribute, likely because it is a new instance and has not been
                    # inserted into the datastore yet.
                    pass

                return result


        class NDBResource(ModelUtils, ndb.Expando):
            created = ndb.DateTimeProperty('ndbrc', auto_now_add=True, indexed=False)
            updated = ndb.DateTimeProperty('ndbru', auto_now=True, indexed=False)


        class NDBUniqueValueModel(ndb.Expando):
            """A model to store unique values.

            The only purpose of this model is to "reserve" values that must be unique
            within a given scope, as a workaround because datastore doesn't support
            the concept of uniqueness for entity properties.

            For example, suppose we have a model `User` with three properties that
            must be unique across a given group: `username`, `auth_id` and `email`::

                class User(model.Model):
                    username = model.StringProperty(required=True)
                    auth_id = model.StringProperty(required=True)
                    email = model.StringProperty(required=True)

            To ensure property uniqueness when creating a new `User`, we first create
            `Unique` records for those properties, and if everything goes well we can
            save the new `User` record::

                @classmethod
                def create_user(cls, username, auth_id, email):
                    # Assemble the unique values for a given class and attribute scope.
                    uniques = [
                        'User.username.%s' % username,
                        'User.auth_id.%s' % auth_id,
                        'User.email.%s' % email,
                    ]

                    # Create the unique username, auth_id and email.
                    success, existing = Unique.create_multi(uniques)

                    if success:
                        # The unique values were created, so we can save the user.
                        user = User(username=username, auth_id=auth_id, email=email)
                        user.put()
                        return user
                    else:
                        # At least one of the values is not unique.
                        # Make a list of the property names that failed.
                        props = [name.split('.', 2)[1] for name in uniques]
                        raise ValueError('Properties %r are not unique.' % props)

            Based on the idea from http://goo.gl/pBQhB

            :copyright: 2011 by tipfy.org.
            :license: Apache Sotware License
            """

            @classmethod
            def create(cls, value):
                """Creates a new unique value.

                :param value:
                    The value to be unique, as a string.

                    The value should include the scope in which the value must be
                    unique (ancestor, namespace, kind and/or property name).

                    For example, for a unique property `email` from kind `User`, the
                    value can be `User.email:me@myself.com`. In this case `User.email`
                    is the scope, and `me@myself.com` is the value to be unique.
                :returns:
                    True if the unique value was created, False otherwise.
                """
                entity = cls(key=ndb.Key(cls, value))
                txn = lambda: entity.put() if not entity.key.get() else None
                return ndb.transaction(txn) is not None

            @classmethod
            def create_multi(cls, values):
                """Creates multiple unique values at once.

                :param values:
                    A sequence of values to be unique. See :meth:`create`.
                :returns:
                    A tuple (bool, list_of_keys). If all values were created, bool is
                    True and list_of_keys is empty. If one or more values weren't
                    created, bool is False and the list contains all the values that
                    already existed in datastore during the creation attempt.
                """
                # Maybe do a preliminary check, before going for transactions?
                # entities = model.get_multi(keys)
                # existing = [entity.key.id() for entity in entities if entity]
                # if existing:
                #    return False, existing

                # Create all records transactionally.
                keys = [ndb.Key(cls, value) for value in values]
                entities = [cls(key=key) for key in keys]
                func = lambda e: e.put() if not e.key.get() else None
                created = [ndb.transaction(lambda: func(e)) for e in entities]

                if created != keys:
                    # A poor man's "rollback": delete all recently created records.
                    ndb.delete_multi(k for k in created if k)
                    return False, [k.id() for k in keys if k not in created]

                return True, []

            @classmethod
            def delete_multi(cls, values):
                """Deletes multiple unique values at once.

                :param values:
                    A sequence of values to be deleted.
                """
                return ndb.delete_multi(ndb.Key(cls, v) for v in values)


        class NDBUniques(object):
            @classmethod
            def create(cls, data_type, unique_name, unique_value):
                unique = '%s.%s:%s' % (data_type, unique_name, unique_value)

                return NDBUniqueValueModel.create(unique)

            @classmethod
            def create_multi(cls, data_type, unique_name_value_tuples):
                uniques = []
                for kv_pair in unique_name_value_tuples:
                    key = '%s.%s:%s' % (data_type, kv_pair[0], kv_pair[1])
                    uniques.append((key, kv_pair[1]))

                ok, existing = NDBUniqueValueModel.create_multi(k for k, v in uniques)
                if ok:
                    return True, None
                else:
                    properties = [v for k, v in uniques if k in existing]
                    return False, properties

            @classmethod
            def delete_multi(cls, data_type, unique_name_value_tuples):
                uniques = []
                for kv_pair in unique_name_value_tuples:
                    key = '%s.%s:%s' % (data_type, kv_pair[0], kv_pair[1])
                    uniques.append((key, kv_pair[1]))

                return NDBUniqueValueModel.delete_multi(k for k, v in uniques)
