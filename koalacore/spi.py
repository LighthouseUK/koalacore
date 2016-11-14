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
from google.appengine.api import search
from .tools import DictDiffer
from .exceptions import KoalaException
from .resource import ResourceUID


__author__ = 'Matt Badger'


async = ndb.tasklet
Index = search.Index


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


class BaseSPI(object):
    def __init__(self, resource_model, **kwargs):
        self.resource_model = resource_model


class SPIContainer(object):
    def __init__(self, resource_model, resource_update_queue=None, search_index_update_queue=None, **kwargs):
        # we need the resource model, but only for init; no point saving it as an attribute
        self.resource_update_queue = resource_update_queue
        self.search_index_update_queue = search_index_update_queue


class SPIMock(BaseSPI):
    def __init__(self, methods=None, **kwargs):
        super(SPIMock, self).__init__(**kwargs)
        if methods is not None:
            self._create_methods(methods=methods, **kwargs)

    def _create_methods(self, methods, **kwargs):
        pass

    def __getattr__(self, name):
        if not name.startswith('__') and not name.endswith('__'):
            raise ndb.Return("'%s' was called" % name)
        raise AttributeError("%r object has no attribute %r" % (self.__class__, name))


class DatastoreMock(SPIMock):
    def _create_methods(self, methods, **kwargs):
        for method in methods:
            setattr(self, method, NDBMethod(code_name=method, **kwargs))


class SearchMock(SPIMock):
    def _create_methods(self, methods, **kwargs):
        for method in methods:
            setattr(self, method, SPIMethod(code_name=method, **kwargs))


class GAESPI(SPIContainer):
    def __init__(self, datastore_config=None, search_config=None, **kwargs):
        super(GAESPI, self).__init__(**kwargs)
        # Need the task queue to use for updates to search doc and resource model, if necessary
        default_datastore_config = {
            'type': DatastoreMock,
            'resource_model': kwargs['resource_model'],
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
            'resource_model': kwargs['resource_model'],
            'result_model': Result,
            'search_results_model': SearchResults,
        }

        try:
            default_search_config.update(search_config)
        except (KeyError, TypeError):
            pass

        new_search_index_type = default_search_config['type']
        del default_search_config['type']
        self.search_index = new_search_index_type(**default_search_config)

        # TODO: methods to hook into datastore and create/update search docs


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
    def __init__(self, code_name, **kwargs):
        self.code_name = code_name

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
    def __init__(self, resource_name, uniques_value_model=UniqueValue, force_unique_parse=False, **kwargs):
        super(NDBMethod, self).__init__(**kwargs)
        self.resource_name = resource_name
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

        yield self._trigger_hook(signal=self.post_signal, op_result=resource_uid, **kwargs)

        raise ndb.Return(resource_uid)


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

    def __init__(self, resource_model, **kwargs):
        resource_name = resource_model.__name__
        self.insert = NDBInsert(resource_name=resource_name)
        self.get = NDBGet(resource_name=resource_name)
        self.update = NDBUpdate(resource_name=resource_name)
        self.delete = NDBDelete(resource_name=resource_name)

    def build_resource_uid(self, desired_id, parent=None, namespace=None, urlsafe=True):
        # TODO: fix this for new resource model
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


def diff_resource_properties(source, target):
    """
    Find the differences between two resource instances (excluding the keys).

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


class SearchMethod(SPIMethod):
    def __init__(self, search_index, **kwargs):
        super(SearchMethod, self).__init__(**kwargs)
        self.search_index = search_index

    @async
    def __call__(self, **kwargs):
        """
        API methods are very simple. They simply emit signal which you can receive and act upon. By default
        there are two signals: pre and post.

        :param kwargs:
        :return:
        """
        yield self._trigger_hook(signal=self.pre_signal, **kwargs)
        result = yield self._internal_op(**kwargs)
        yield self._trigger_hook(signal=self.post_signal, op_result=result, **kwargs)
        raise ndb.Return(result)


class SearchInsert(SearchMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'insert'
        super(SearchInsert, self).__init__(**kwargs)

    def _internal_op(self, search_doc, **kwargs):
        """
        Insert search_doc into the Search index.

        :param search_doc:
        :param kwargs:
        :returns future (key for the inserted search doc):
        """
        result = yield self.search_index.put_async(search_doc, **kwargs)
        raise ndb.Return(result)


class SearchGet(SearchMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'get'
        super(SearchGet, self).__init__(**kwargs)

    def _internal_op(self, search_doc_uid, **kwargs):
        """
        Get model from the Search index.

        :param search_doc_uid:
        :param kwargs:
        :returns future (key for the inserted search doc):
        """
        result = yield self.search_index.get_async(search_doc_uid, **kwargs)
        raise ndb.Return(result)


class SearchUpdate(SearchMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'update'
        super(SearchUpdate, self).__init__(**kwargs)

    def _internal_op(self, search_doc, **kwargs):
        """
        Update model in the Search index.

        :param search_doc:
        :param kwargs:
        :returns future (key for the inserted search doc):
        """
        result = yield self.search_index.put_async(search_doc, **kwargs)
        raise ndb.Return(result)


class SearchDelete(SearchMethod):
    def __init__(self, **kwargs):
        kwargs['code_name'] = 'delete'
        super(SearchDelete, self).__init__(**kwargs)

    def _internal_op(self, search_doc_uid, **kwargs):
        """
        Delete model from the Search index.

        :param search_doc_uid:
        :param kwargs:
        """
        result = yield self.search_index.delete_async(search_doc_uid, **kwargs)
        raise ndb.Return(result)


class SearchQuery(SearchMethod):
    def __init__(self, repeated_properties=None, result_model=Result, search_results_model=SearchResults, **kwargs):
        super(SearchQuery, self).__init__(**kwargs)
        self.repeated_properties = repeated_properties
        self.result_model = result_model
        self.search_results_model = search_results_model

    def _internal_op(self, query_string, explicit_query_string_overrides=None, cursor_support=False,
                     existing_cursor=None, limit=20, number_found_accuracy=None, offset=None, sort_options=None,
                     returned_fields=None, ids_only=False, snippeted_fields=None, returned_expressions=None,
                     sort_limit=1000, **kwargs):
        """
        Query search records in the search index. Essentially the params are the same as for GAE Search API.
        The exceptions are cursor, returned_expressions and sort_options.

        'explicit_query_string_overrides' is an iterable of tuples of the form ('property', 'value') which can be
        used to explicitly overwrite values from the supplied query string. This is useful if you have some custom
        filters that must only have certain values. It can also be used to prevent searches occurring with
        restricted values; useful as part of permission systems.

        Cursor is replaced by two args - cursor_support and existing_cursor. Existing cursor is the websafe version
        of a cursor returned by a previous query. Obviously if cursor_support is False then we don't process the
        cursor.

        Both returned_expressions and sort_options are lists of tuples instead of passing in search.FieldExpressions
        or search.SortOptions (as this would leak implementation to the client).

        returned_expression = ('name_of_expression', 'expression')
        sort_option = ('sort_expression, 'direction', 'default_value)

        See https://cloud.google.com/appengine/docs/python/search/options for more detailed explanations.

        Sort limit should be overridden if possible matches exceeds 1000. It should be set to a value higher, or
        equal to, the maximum number of results that could be found for a given search.

        :param query_string:
        :param explicit_query_string_overrides:
        :param cursor_support:
        :param existing_cursor:
        :param limit:
        :param number_found_accuracy:
        :param offset:
        :param sort_options:
        :param returned_fields:
        :param ids_only:
        :param snippeted_fields:
        :param returned_expressions:
        :param sort_limit:
        :param args:
        :param kwargs:
        :raises search.Error:
        :raises TypeError:
        :raises ValueError:
        :returns future (SearchResults object which contains matching Results):
        """

        cursor = None
        compiled_sort_options = None
        compiled_field_expressions = None

        if explicit_query_string_overrides:
            # TODO: use regex to split up the query string and swap out/append the explicit params. At the moment
            # multiple values could be passed for the same category, leading to possible data leaks
            query_fragments = []

            for explicit_param in explicit_query_string_overrides:
                query_fragments.append(u'{}="{}"'.format(explicit_param[0],
                                                         explicit_param[1].replace(',', '\,').replace('+',
                                                                                                      '\+').strip()))

            explicit_string = u' AND '.join(query_fragments)
            if explicit_string:
                query_string = u'{} {}'.format(query_string, explicit_string)

        if cursor_support:
            if existing_cursor:
                cursor = search.Cursor(web_safe_string=existing_cursor)
            else:
                cursor = search.Cursor()

        if sort_options:
            parsed_options = [search.SortExpression(expression=sort_option[0],
                                                    direction=sort_option[1],
                                                    default_value=sort_option[2]) for sort_option in sort_options]
            compiled_sort_options = search.SortOptions(expressions=parsed_options, limit=sort_limit)

        if returned_expressions:
            compiled_field_expressions = [search.FieldExpression(name=field_exp[0], expression=field_exp[1]) for
                                          field_exp in returned_expressions]

        options = search.QueryOptions(
            ids_only=ids_only,
            limit=limit,
            snippeted_fields=snippeted_fields,
            number_found_accuracy=number_found_accuracy,
            returned_fields=returned_fields,
            returned_expressions=compiled_field_expressions,
            sort_options=compiled_sort_options,
            offset=offset,
            cursor=cursor,
        )

        query = search.Query(query_string=query_string, options=options)
        search_result = yield self.search_index.search_async(query=query)

        result_cursor = None
        parsed_results = []

        for result in search_result:
            parsed_result = self.result_model(uid=result.doc_id)

            for field in result.fields:
                if self.repeated_properties is not None and (field.name in self.repeated_properties):
                    # Attempt to handle repeated fields in the search result
                    try:
                        setattr(parsed_result, field.name, [field.value for field in result[field.name]])
                    except TypeError:
                        # This is ok; simply proceed to set the field value as normal
                        pass
                    else:
                        # On success we want to skip the remaining code because we have already set the value
                        continue

                setattr(parsed_result, field.name, field.value)

            parsed_results.append(parsed_result)

        if search_result.cursor:
            result_cursor = search_result.cursor.web_safe_string

        result = self.search_results_model(results_count=search_result.number_found,
                                           results=parsed_results,
                                           cursor=result_cursor)

        raise ndb.Return(result)


class GAESearch(object):

    def __init__(self, resource_model, index_name=None, result_model=Result, search_results_model=SearchResults):
        if index_name is None:
            index_name = resource_model.__name__

        self.index_name = index_name

        self.search_index = Index(name=self.index_name)
        resource_repeated_properties = None

        if resource_model is not None:
            resource_repeated_properties = set(resource_property._code_name for resource_property in resource_model._properties if resource_property._repeated)

        self.insert = SearchInsert(search_index=self.search_index)
        self.get = SearchGet(search_index=self.search_index)
        self.update = SearchUpdate(search_index=self.search_index)
        self.delete = SearchDelete(search_index=self.search_index)
        # The main method!
        self.search = SearchQuery(search_index=self.search_index,
                                  repeated_properties=resource_repeated_properties,
                                  result_model=result_model,
                                  search_results_model=search_results_model)
