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


class DatastoreMock(object):
    def __init__(self, *args, **kwargs):
        pass


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
    def __init__(self, datastore_model, resource_model):
        self._datastore_model = datastore_model
        self._resource_model = resource_model

    def _internal_insert(self, datastore_model, **kwargs):
        """
        Insert model into the datastore. This should be defined by the derived class and never overridden!

        :param datastore_model:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _internal_get(self, datastore_key, **kwargs):
        """
        Get model from datastore. This should be defined by the derived class and never overridden!

        :param datastore_key:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _internal_update(self, datastore_model, **kwargs):
        """
        Insert updated model into datastore. This should be defined by the derived class and never overridden!

        :param datastore_model:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _internal_patch(self, datastore_key, delta_update, **kwargs):
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

    def _internal_delete(self, datastore_key, **kwargs):
        """
        Delete model from datastore. This should be defined by the derived class and never overridden!

        :param datastore_key:
        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _internal_list(self, **kwargs):
        """
        Fetch datastore models. This should be defined by the derived class and never overridden!

        :param kwargs:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def insert_async(self, resource, **kwargs):
        """
        Wrapper around _internal_insert. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource:
        :param kwargs:
        :returns future; resolves to the uid for the inserted model (string):
        """
        uniques, old_uniques = self._parse_resource_unique_values(resource=resource, force=True)
        if uniques:
            self._create_unique_locks(uniques=uniques)
        datastore_model = self._convert_resource_to_datastore_model(resource=resource)
        return self._internal_insert(datastore_model=datastore_model, **kwargs)

    def get_async(self, resource_uid, **kwargs):
        """
        Wrapper around _internal_get. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns future; resolves to resouce_object, or None:
        """
        return self._internal_get(datastore_key=resource_uid, **kwargs)

    def update_async(self, resource, **kwargs):
        """
        Wrapper around _internal_update. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource:
        :param kwargs:
        :returns future; resolves to the uid for the inserted model (string):
        """
        uniques, old_uniques = self._parse_resource_unique_values(resource=resource)
        if uniques:
            result, errors = self._create_unique_locks(uniques=uniques)
            if result and old_uniques:
                self._delete_unique_locks(uniques=old_uniques)
        datastore_model = self._convert_resource_to_datastore_model(resource=resource)
        return self._internal_update(datastore_model=datastore_model, **kwargs)

    def patch_async(self, resource_uid, delta_update, **kwargs):
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
        datastore_key = self.parse_datastore_key(resource_uid=resource_uid)
        self._create_unique_locks({k: v for k, v in delta_update.iteritems() if k in self._resource_model._uniques})
        return self._internal_patch(datastore_key=datastore_key, delta_update=delta_update, **kwargs)

    def delete_async(self, resource_uid, **kwargs):
        """
        Wrapper around _internal_delete. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns future:
        """
        return self._internal_delete(datastore_key=resource_uid, **kwargs)

    def list_async(self, **kwargs):
        """
        Wrapper around _internal_list. At its simplest, simply converts between koala resource objects and native
        datastore model. May be overridden.

        :param kwargs:
        :returns future; resolves to list (of resources or empty list):
        """
        return self._internal_list(**kwargs)

    def parse_insert_async_result(self, future):
        """
        Evaluates the specified insert future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
        """
        return self._normalise_output(output=future.get_result())

    def parse_get_async_result(self, future):
        """
        Evaluates the specified get future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a resource object, otherwise exception:
        """
        return self._normalise_output(output=future.get_result())

    def parse_update_async_result(self, future):
        """
        Evaluates the specified update future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the updated entity, otherwise exception:
        """
        return self._normalise_output(output=future.get_result())

    def parse_patch_async_result(self, future):
        """
        Evaluates the specified patch future and returns the value, after running it through '_normalise_output'.

        :param future:
        :returns result of future; hopefully a uid for the patched entity, otherwise exception:
        """
        return self._normalise_output(output=future.get_result())

    def parse_delete_async_result(self, future):
        """
        Evaluates the specified delete future and returns the value, after running it through '_normalise_output'.

        :param future:
        """
        self._normalise_output(future.get_result())

    def parse_list_async_result(self, futures):
        """
        Evaluates the specified list future and returns the value, after running it through '_normalise_output'.

        :param futures:
        :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
        """
        # TODO: modify this to properly support query semantics and results
        return self._normalise_output(output=futures.get_result())

    def insert(self, resource, **kwargs):
        """
        Wrapper around insert_async to automatically resolve async future. May be overridden.

        :param resource:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        insert_future = self.insert_async(resource=resource, **kwargs)
        return self.parse_insert_async_result(future=insert_future)

    def get(self, resource_uid, **kwargs):
        """
        Wrapper around get_async to automatically resolve async future. May be overridden.

        :param resource_uid:
        :param kwargs:
        :returns resource, or None:
        """
        entity_future = self.get_async(resource_uid=resource_uid, **kwargs)
        return self.parse_get_async_result(future=entity_future)

    def update(self, resource, **kwargs):
        """
        Wrapper around update_async to automatically resolve async future. May be overridden.

        :param resource:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        update_future = self.update_async(resource=resource, **kwargs)
        return self.parse_update_async_result(future=update_future)

    def patch(self, resource_uid, delta_update, **kwargs):
        """
        Wrapper around patch_async to automatically resolve async future. May be overridden.

        :param resource_uid:
        :param delta_update:
        :param kwargs:
        :returns the uid for the inserted model (string):
        """
        update_future = self.patch_async(resource_uid=resource_uid, delta_update=delta_update, **kwargs)
        return self.parse_update_async_result(future=update_future)

    def delete(self, resource_uid, **kwargs):
        """
        Wrapper around delete_async to automatically resolve async future. May be overridden. No return value.

        :param resource_uid:
        :param kwargs:
        """
        delete_future = self.delete_async(resource_uid=resource_uid, **kwargs)
        self.parse_delete_async_result(future=delete_future)

    def list(self, **kwargs):
        """
        Wrapper around list_async to automatically resolve async future(s). May be overridden.

        :param kwargs:
        :returns list (of resources or empty):
        """
        futures = self.list_async(**kwargs)
        return self.parse_list_async_result(futures=futures)

    def _build_unique_value_keys(self, uniques):
        """
        Generate unique datastore keys for each property=>value pair in uniques. Return as list of strings

        :param uniques:
        :return unique_keys:
        """
        raise NotImplementedError

    def _parse_resource_unique_values(self, resource, force=False):
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
            if unique in resource._uniques_modified or force:
                value = getattr(resource, unique)
                if value:
                    uniques[unique] = value
                    try:
                        old_values[unique] = resource._history[unique][0]
                    except KeyError:
                        # There is no old value
                        pass

        return uniques, old_values

    def _create_unique_locks(self, uniques):
        """
        Create unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """
        raise NotImplementedError

    def _delete_unique_locks(self, uniques):
        """
        Delete unique locks from a dict of property=>value pairs

        :param uniques:
        :return:
        """
        raise NotImplementedError

    def parse_datastore_key(self, resource_uid):
        """
        Derived class should implement. Method to do any parsing necessary to a resource_uid before use.

        :param resource_uid:
        :returns datastore_key:
        """
        return resource_uid

    def _convert_resource_to_datastore_model(self, resource):
        """
        Derived class should implement. Method to convert between koala resource objects and native datastore models.

        :param resource:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _convert_datastore_model_to_resource(self, datastore_model):
        """
        Derived class should implement. Method to convert between native datastore models and koala resource objects.

        :param datastore_model:
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def _normalise_output(self, output):
        """
        Convert output to normalised objects (resource objects or string uids).

        :param output:
        :return normalised object (could be list of objects):
        :raises NotImplementedError:
        """
        raise NotImplementedError

    def get_future_result(self, future):
        """
        Helper function to call the relevant future resolver method based on the 'method' property of future.

        :param future:
        :raises AttributeError (if future does not have a method property set:
        :returns result of future:
        """
        method = future.method

        if method == 'insert_async':
            return self.parse_insert_async_result(future)
        elif method == 'get_async':
            return self.parse_get_async_result(future)
        elif method == 'update_async':
            return self.parse_update_async_result(future)
        elif method == 'patch_async':
            return self.parse_patch_async_result(future)
        elif method == 'delete_async':
            return self.parse_delete_async_result(future)
        elif method == 'list_async':
            return self.parse_list_async_result(future)

    def _transaction_log(self):
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

        def __init__(self, *args, **kwargs):
            super(EventedDatastoreInterface, self).__init__(*args, **kwargs)

            # Insert method hooks and filters
            self.hook_pre_insert = signal('hook_pre_insert')
            self.hook_post_insert = signal('hook_post_insert')

            # Get method hooks and filters
            self.hook_pre_get = signal('hook_pre_get')
            self.hook_post_get = signal('hook_post_get')

            # Update method hooks and filters
            self.hook_pre_update = signal('hook_pre_update')
            self.hook_post_update = signal('hook_post_update')

            # patch method hooks and filters
            self.hook_pre_patch = signal('hook_pre_patch')
            self.hook_post_patch = signal('hook_post_patch')

            # Delete method hooks and filters
            self.hook_pre_delete = signal('hook_pre_delete')
            self.hook_post_delete = signal('hook_post_delete')

            # List method hooks and filters
            self.hook_pre_list = signal('hook_pre_list')
            self.hook_post_list = signal('hook_post_list')

        @property
        def _hook_pre_insert_enabled(self):
            return bool(self.hook_pre_insert.receivers)

        @property
        def _hook_post_insert_enabled(self):
            return bool(self.hook_post_insert.receivers)

        @property
        def _hook_pre_get_enabled(self):
            return bool(self.hook_pre_get.receivers)

        @property
        def _hook_post_get_enabled(self):
            return bool(self.hook_post_get.receivers)

        @property
        def _hook_pre_update_enabled(self):
            return bool(self.hook_pre_update.receivers)

        @property
        def _hook_post_update_enabled(self):
            return bool(self.hook_post_update.receivers)

        @property
        def _hook_pre_patch_enabled(self):
            return bool(self.hook_pre_patch.receivers)

        @property
        def _hook_post_patch_enabled(self):
            return bool(self.hook_post_patch.receivers)

        @property
        def _hook_pre_delete_enabled(self):
            return bool(self.hook_pre_delete.receivers)

        @property
        def _hook_post_delete_enabled(self):
            return bool(self.hook_post_delete.receivers)

        @property
        def _hook_pre_list_enabled(self):
            return bool(self.hook_pre_list.receivers)

        @property
        def _hook_post_list_enabled(self):
            return bool(self.hook_post_list.receivers)

        def insert_async(self, resource, **kwargs):
            """
            Wrapper around the base insert_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if self._hook_pre_insert_enabled:
                self.hook_pre_insert.send(self, resource=resource, **kwargs)

            return super(EventedDatastoreInterface, self).insert_async(resource=resource, **kwargs)

        def parse_insert_async_result(self, future):
            """
            Evaluates the specified insert future and returns the value, after running it through '_normalise_output'.
            Also triggers post insert events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
            """
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_insert_enabled:
                self.hook_post_insert.send(self, op_result=op_result, future=future)

            return op_result

        def get_async(self, resource_uid, **kwargs):
            """
            Wrapper around the base get_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param kwargs:
            :returns future; resolves to resource, or None:
            """

            if self._hook_pre_get_enabled:
                self.hook_pre_get.send(self, resource_uid=resource_uid, **kwargs)

            return super(EventedDatastoreInterface, self).get_async(resource_uid=resource_uid, **kwargs)

        def parse_get_async_result(self, future):
            """
            Evaluates the specified get future and returns the value, after running it through '_normalise_output'.
            Also triggers post get events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a resource object, otherwise exception:
            """
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_get_enabled:
                self.hook_post_get.send(self, op_result=op_result, future=future)

            return op_result

        def update_async(self, resource, **kwargs):
            """
            Wrapper around the base update_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if self._hook_pre_update_enabled:
                self.hook_pre_update.send(self, resource=resource, **kwargs)

            return super(EventedDatastoreInterface, self).update_async(resource=resource, **kwargs)

        def parse_update_async_result(self, future):
            """
            Evaluates the specified update future and returns the value, after running it through '_normalise_output'.
            Also triggers post update events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the updated entity, otherwise exception:
            """
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_update_enabled:
                self.hook_post_update.send(self, op_result=op_result, future=future)

            return op_result

        def patch_async(self, resource_uid, delta_update, **kwargs):
            """
            Wrapper around the base patch_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param delta_update:
            :param kwargs:
            :returns future; resolves to the uid for the inserted model (string):
            """

            if self._hook_pre_patch_enabled:
                self.hook_pre_patch.send(self, resource_uid=resource_uid, delta_update=delta_update, **kwargs)

            return super(EventedDatastoreInterface, self).patch_async(resource_uid=resource_uid,
                                                                      delta_update=delta_update, **kwargs)

        def parse_patch_async_result(self, future):
            """
            Evaluates the specified patch future and returns the value, after running it through '_normalise_output'.
            Also triggers post patch events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the patched entity, otherwise exception:
            """
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_patch_enabled:
                self.hook_post_patch.send(self, op_result=op_result, future=future)

            return op_result

        def delete_async(self, resource_uid, **kwargs):
            """
            Wrapper around the base delete_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param resource_uid:
            :param kwargs:
            :returns future:
            """
            if self._hook_pre_delete_enabled:
                self.hook_pre_delete.send(self, resource_uid=resource_uid, **kwargs)

            return super(EventedDatastoreInterface, self).delete_async(resource_uid=resource_uid, **kwargs)

        def parse_delete_async_result(self, future):
            """
            Evaluates the specified delete future and returns the value, after running it through '_normalise_output'.
            Also triggers post delete events so that other modules can plugin functionality.
            :param future:
            """
            # There is no return value here but I've added it in to keep the method signatures/signal args consistent
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_delete_enabled:
                self.hook_post_delete.send(self, op_result=op_result, future=future)

        def list_async(self, **kwargs):
            """
            Wrapper around the base list_async() method to add event hooks. May be overridden.
            Triggers hook and filter events for extending functionality.
            :param kwargs:
            :returns future; resolves to list (of resources or empty list):
            """

            if self._hook_pre_insert_enabled:
                self.hook_pre_insert.send(self, **kwargs)

            return super(EventedDatastoreInterface, self).list_async(**kwargs)

        def parse_list_async_result(self, future):
            """
            Evaluates the specified list future and returns the value, after running it through '_normalise_output'.
            Also triggers post list events so that other modules can plugin functionality.
            :param future:
            :returns result of future; hopefully a uid for the newly inserted entity, otherwise exception:
            """
            # TODO: modify this to properly support query semantics and results
            op_result = self._normalise_output(future.get_result())

            if self._hook_post_list_enabled:
                self.hook_post_list.send(self, op_result=op_result, future=future)

            return op_result


    try:
        import google.appengine.ext.ndb as ndb
        from google.appengine.ext.ndb.google_imports import ProtocolBuffer
    except ImportError:
        # Required libraries are not available; skip definition
        pass
    else:
        class KoalaNDB(EventedDatastoreInterface):
            """
            NDB Evented Datastore Interface. Implements the base datastore methods above and adds in some additional
            helpers. Provides a consistent interface to datastores. This particular implementation supports event hooks
            and filters for extending functionality from other modules.

            Inheriting classes must implement the following class attributes:

            _resource (This is set by the base SPI class, but may be overridden)
            _datastore_model (Definition of the native datastore model for the SPI)

            TODO: properly implement/handle transactions in async configuration.
            """

            def __init__(self, unwanted_resource_kwargs=None, *args, **kwargs):
                super(KoalaNDB, self).__init__(*args, **kwargs)

                default_unwanted_kwargs = ['uniques_modified', 'immutable', 'track_unique_modifications', '_history',
                                           '_history_tracking']

                if unwanted_resource_kwargs is not None:
                    default_unwanted_kwargs = default_unwanted_kwargs + unwanted_resource_kwargs

                self._unwanted_resource_kwargs = default_unwanted_kwargs

                # Internal Insert hooks (no filters inside transaction)
                self._hook_transaction_pre_insert = signal('hook_transaction_pre_insert')
                self._hook_transaction_post_insert = signal('hook_transaction_post_insert')

                # Internal Get hooks (no filters inside transaction)
                self._hook_transaction_pre_get = signal('hook_transaction_pre_get')
                self._hook_transaction_post_get = signal('hook_transaction_post_get')

                # Internal Update hooks (no filters inside transaction)
                self._hook_transaction_pre_update = signal('hook_transaction_pre_update')
                self._hook_transaction_post_update = signal('hook_transaction_post_update')

                # Internal Delete hooks (no filters inside transaction)
                self._hook_transaction_pre_delete = signal('hook_transaction_pre_delete')
                self._hook_transaction_post_delete = signal('hook_transaction_post_delete')

            @property
            def _hook_transaction_pre_insert_enabled(self):
                return bool(self._hook_transaction_pre_insert.receivers)

            @property
            def _hook_transaction_post_insert_enabled(self):
                return bool(self._hook_transaction_post_insert.receivers)

            @property
            def _hook_transaction_pre_get_enabled(self):
                return bool(self._hook_transaction_pre_get.receivers)

            @property
            def _hook_transaction_post_get_enabled(self):
                return bool(self._hook_transaction_post_get.receivers)

            @property
            def _hook_transaction_pre_update_enabled(self):
                return bool(self._hook_transaction_pre_update.receivers)

            @property
            def _hook_transaction_post_update_enabled(self):
                return bool(self._hook_transaction_post_update.receivers)

            @property
            def _hook_transaction_pre_delete_enabled(self):
                return bool(self._hook_transaction_pre_delete.receivers)

            @property
            def _hook_transaction_post_delete_enabled(self):
                return bool(self._hook_transaction_post_delete.receivers)

            def build_resource_uid(self, desired_id, parent=None, namespace=None, urlsafe=True):
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

            def _internal_insert(self, datastore_model, **kwargs):
                """
                Insert model into the ndb datastore. DO NOT OVERRIDE!

                :param datastore_model:
                :param kwargs:
                :returns future (key for the inserted entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_model'] = datastore_model

                if self._hook_transaction_pre_insert_enabled:
                    self._hook_transaction_pre_insert.send(self, **params)

                op_result = datastore_model.put_async(**kwargs)

                if self._hook_transaction_post_insert_enabled:
                    self._hook_transaction_post_insert.send(self, **params)

                op_result.method = 'insert_async'
                op_result.params = params

                return op_result

            def _internal_get(self, datastore_key, **kwargs):
                """
                Get a datastore_model from ndb using an ndb key. DO NOT OVERRIDE!

                :param datastore_key:
                :param kwargs:
                :returns future (fetched entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_key'] = datastore_key

                if self._hook_transaction_pre_get_enabled:
                    self._hook_transaction_pre_get.send(self, **params)

                op_result = datastore_key.get_async(**kwargs)

                if self._hook_transaction_post_get_enabled:
                    self._hook_transaction_post_get.send(self, **params)

                op_result.method = 'get_async'
                op_result.params = params

                return op_result

            def _internal_update(self, datastore_model, **kwargs):
                """
                Insert updated datastore model into ndb. DO NOT OVERRIDE!

                :param datastore_model:
                :param kwargs:
                :returns future (key for the updated entity):
                """
                params = {}
                params.update(kwargs)
                params['datastore_model'] = datastore_model

                if self._hook_transaction_pre_update_enabled:
                    self._hook_transaction_pre_update.send(self, **params)

                op_result = datastore_model.put_async(**kwargs)

                if self._hook_transaction_post_update_enabled:
                    self._hook_transaction_post_update.send(self, **params)

                op_result.method = 'update_async'
                op_result.params = params

                return op_result

            def _internal_patch(self, datastore_key, delta_update, **kwargs):
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
                    model = yield self._internal_get(datastore_key=datastore_key)
                    if model is None:
                        yield False

                    resource = self._normalise_output(model)

                    for resource_property, value in delta_update.iteritems():
                        setattr(resource, resource_property, value)

                    updated_model = self._convert_resource_to_datastore_model(resource=resource)

                    updated_model_key = yield self._internal_update(datastore_model=updated_model)
                    raise ndb.Return(self._normalise_output(updated_model_key))

                params = {}
                params.update(kwargs)
                params['delta_update'] = delta_update

                op_result = delta_update_transaction(datastore_key=datastore_key, delta_update=delta_update)

                op_result.method = 'patch_async'
                op_result.params = params
                return op_result

            def _internal_delete(self, datastore_key, **kwargs):
                """
                Delete datastore_key from ndb. DO NOT OVERRIDE!

                :param datastore_key:
                :param kwargs:
                :returns future (technically there is no return value on success):
                """
                params = {}
                params.update(kwargs)
                params['datastore_key'] = datastore_key

                if self._hook_transaction_pre_delete_enabled:
                    self._hook_transaction_pre_delete.send(self, **params)

                op_result = datastore_key.delete_async(**kwargs)

                if self._hook_transaction_post_delete_enabled:
                    self._hook_transaction_post_delete.send(self, **params)

                op_result.method = 'delete_async'
                op_result.params = params

                return op_result

            def _internal_list(self, **kwargs):
                pass

            def get_async(self, resource_uid, **kwargs):
                """
                Wrapper around _internal_get. At its simplest, simply converts between koala resource objects and native
                datastore model. May be overridden. Overriding the base implementation as the resource uid needs to be
                converted to an NDB Key instance.

                :param resource_uid:
                :param kwargs:
                :returns future:
                """
                datastore_key = self._convert_string_to_ndb_key(resource_uid)
                return self._internal_get(datastore_key, **kwargs)

            def delete_async(self, resource_uid, **kwargs):
                """
                Wrapper around _internal_delete. At its simplest, simply converts between koala resource objects and
                native datastore model. May be overridden. Overriding the base implementation as the resource uid needs
                to be converted to an NDB Key instance.

                :param resource_uid:
                :param kwargs:
                :returns future:
                """
                resource = self.get(resource_uid=resource_uid)
                uniques, old_values = self._parse_resource_unique_values(resource=resource, force=True)
                if uniques:
                    self._delete_unique_locks(uniques=uniques)
                datastore_key = self.parse_datastore_key(resource_uid=resource_uid)
                return self._internal_delete(datastore_key, **kwargs)

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

            def _build_unique_value_keys(self, uniques):
                """
                Generate unique datastore keys for each property=>value pair in uniques. Return as list of strings

                :param uniques:
                :return unique_keys:
                """
                base_unique_key = u'{}.'.format(self._resource_model.__name__)

                return [u'{}{}.{}'.format(base_unique_key, unique, value) for unique, value in uniques.iteritems()]

            def _create_unique_locks(self, uniques):
                """
                Create unique locks from a dict of property=>value pairs

                :param uniques:
                :return:
                """
                if not uniques:
                    return

                unique_keys = self._build_unique_value_keys(uniques=uniques)

                if not unique_keys:
                    return

                result, errors = NDBUniqueValueModel.create_multi(unique_keys)

                if not result:
                    raise UniqueValueRequired(errors=[name.split('.', 2)[1] for name in errors])

                return result, errors

            def _delete_unique_locks(self, uniques):
                """
                Delete unique locks from a dict of property=>value pairs

                :param uniques:
                :return:
                """
                if not uniques:
                    return

                unique_keys = self._build_unique_value_keys(uniques=uniques)

                if unique_keys:
                    NDBUniqueValueModel.delete_multi(unique_keys)

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
