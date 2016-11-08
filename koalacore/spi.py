# -*- coding: utf-8 -*-
"""
    koalacore.spi
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
"""

from blinker import signal
import google.appengine.ext.ndb as ndb
from google.appengine.ext.ndb.google_imports import ProtocolBuffer
from .tools import DictDiffer
from .exceptions import KoalaException
from .resource import ResourceUID


__author__ = 'Matt Badger'


async = ndb.tasklet


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


class SPIMock(object):
    def __init__(self, methods=None, **kwargs):
        self._create_methods(methods=methods, **kwargs)

    def _create_methods(self, methods, **kwargs):
        pass

    def __getattr__(self, name):
        if not name.startswith('__') and not name.endswith('__'):
            raise ndb.Return("'%s' was called" % name)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, name))


class DatastoreMock(SPIMock):
    def __init__(self, resource_model, **kwargs):
        kwargs['resource_model'] = resource_model
        super(DatastoreMock, self).__init__(**kwargs)

    def _create_methods(self, methods, **kwargs):
        for method in methods:
            setattr(self, method, NDBMethod(code_name=method, **kwargs))


class SearchMock(SPIMock):
    def __init__(self, update_queue='test', **kwargs):
        super(SearchMock, self).__init__(**kwargs)
        self.update_queue = update_queue

    def _create_methods(self, methods, **kwargs):
        for method in methods:
            setattr(self, method, SPIMethod(code_name=method, **kwargs))


class SearchResults(object):
    def __init__(self, results_count, results, cursor=None):
        self.results_count = results_count
        self.results = results
        self.cursor = cursor


class Result(object):
    def __init__(self, uid):
        # UID is the identifier for the search result. This will be used mainly to link to additional information about
        # the result
        self.uid = uid


class SPIMethod(object):
    def __init__(self, code_name, resource_name):
        self.code_name = code_name
        self.resource_name = resource_name

        self.pre_name = 'pre_{}'.format(self.code_name)
        self.post_name = 'post_{}'.format(self.code_name)

        self.pre_signal = signal(self.pre_name)
        self.post_signal = signal(self.post_name)

    @async
    def _trigger_hook(self, signal, **kwargs):
        if bool(signal.receivers):
            kwargs['hook_name'] = signal.name
            for receiver in signal.receivers_for(self):
                yield receiver(self, **kwargs)

    @async
    def _internal_op(self, **kwargs):
        raise NotImplementedError

    @async
    def __call__(self, **kwargs):
        """
        SPI methods are very simple. They simply emit signal which you can receive and act upon. By default
        there are two signals: pre and post.

        :param kwargs:
        :return:
        """
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)
        result = yield self._internal_op(**kwargs)
        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)
        raise ndb.Return(result)


class UniqueValue(ndb.Model):
    pass


class NDBMethod(SPIMethod):
    def __init__(self, uniques_value_model=UniqueValue, force_unique_parse=False, **kwargs):
        super(NDBMethod, self).__init__(**kwargs)
        self.uniques_value_model = uniques_value_model
        self.force_unique_parse = force_unique_parse

    def _transaction_receivers(self):
        return bool(self.pre_signal.receivers) or bool(self.post_signal.recievers)

    def _build_unique_value_locks(self, uniques):
        """
        Generate unique model instances for each property=>value pair in uniques. Returns instances of
        self.unique_value_model, ready for commit in transaction

        :param uniques:
        :return unique_locks:
        """
        base_unique_key = u'{}.'.format(self.resource_name)

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

    def _internal_op(self, resource, uniques, **kwargs):
        """
        Insert model into the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        uniques will either be a list of ndb keys or None

        :param resource:
        :param uniques:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['resource'] = resource
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        # This might raise an exception, but it doesn't return a value. If uniques are supplied then we are
        # automatically in a transaction
        if uniques:
            yield self._create_unique_locks(uniques=uniques)

        result = yield resource.put_async(**kwargs)
        resource_uid = ResourceUID(raw=result)

        yield self._trigger_hook(signal=self.post_signal, op_result=resource_uid, **kwargs)

        raise ndb.Return(resource_uid)


class NDBGet(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'get'
        kwargs['force_unique_parse'] = False
        super(NDBGet, self).__init__(**kwargs)

    def _internal_op(self, resource_uid, **kwargs):
        """
        Get model from the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        :param resource_uid:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['resource_uid'] = resource_uid
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        result = yield resource_uid.raw.get_async(**kwargs)

        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)

        raise ndb.Return(result)


class NDBUpdate(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'update'
        kwargs['force_unique_parse'] = False
        super(NDBUpdate, self).__init__(**kwargs)

    def _internal_op(self, resource, uniques, old_uniques, **kwargs):
        """
        Update model in the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        uniques will either be a list of ndb keys or None

        :param resource:
        :param uniques:
        :param kwargs:
        :returns future (key for the inserted entity):
        """
        kwargs['resource'] = resource
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

        result = yield resource.put_async(**kwargs)
        resource_uid = ResourceUID(raw=result)

        yield self._trigger_hook(signal=self.post_signal, op_result=resource_uid, **kwargs)

        raise ndb.Return(resource_uid)


class NDBDelete(NDBMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'delete'
        kwargs['force_unique_parse'] = True
        super(NDBDelete, self).__init__(**kwargs)

    def _internal_op(self, resource_uid, uniques, **kwargs):
        """
        Delete model from the ndb datastore. Everything in here should be able to be executed within an NDB
        transaction -- there should be no processing except for the NDB ops. This applies to any connected
        receivers of the transaction signals as well -- they should either be performing a datastore op or
        enqueueing a transactional task.

        :param resource_uid:
        :param kwargs:
        """
        kwargs['resource_uid'] = resource_uid
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)

        if uniques:
            yield self._delete_unique_locks(uniques=uniques)

        result = yield resource_uid.raw.delete_async(**kwargs)

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

        resource_name = resource_model.__name__

        self.insert = NDBInsert(resource_name=resource_name)
        self.get = NDBGet(resource_name=resource_name)
        self.update = NDBUpdate(resource_name=resource_name)
        self.delete = NDBDelete(resource_name=resource_name)

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
