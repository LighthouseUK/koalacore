# -*- coding: utf-8 -*-
"""
    koala.search
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import logging

__author__ = 'Matt Badger'


class ClassPropertyDescriptor(object):

    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, klass=None):
        if klass is None:
            klass = type(obj)
        return self.fget.__get__(obj, klass)()

    def __set__(self, obj, value):
        if not self.fset:
            raise AttributeError("can't set attribute")
        type_ = type(obj)
        return self.fset.__get__(obj, type_)(value)

    def setter(self, func):
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self


def classproperty(func):
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return ClassPropertyDescriptor(func)


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


class BaseSearchInterface(object):
    _search_document_model = None
    _index_name = ''
    index = None
    _result = Result
    _search_result = SearchResults
    _check_duplicates = False

    @classmethod
    def _internal_insert(cls, search_record, *args, **kwargs):
        """
        Insert record into search index. This should be defined by the derived class and never overridden!
        :param search_record:
        :param args:
        :param kwargs:
        :returns insert result:
        """
        raise NotImplementedError

    @classmethod
    def _internal_get(cls, search_record_uid, *args, **kwargs):
        """
        Get record from search index by key. This should be defined by the derived class and never overridden!
        :param search_record_uid:
        :param args:
        :param kwargs:
        :returns search_record:
        """
        raise NotImplementedError

    @classmethod
    def _internal_update(cls, search_record, *args, **kwargs):
        """
        Update record in search index
        :param search_record:
        :param args:
        :param kwargs:
        :returns update result:
        """
        raise NotImplementedError

    @classmethod
    def _internal_delete(cls, search_record_uid, *args, **kwargs):
        """
        Delete model from search index. This should be defined by the derived class and never overridden!
        :param search_record_uid:
        :param args:
        :param kwargs:
        """
        raise NotImplementedError

    @classmethod
    def _internal_search(cls, query_string, *args, **kwargs):
        """
        Search records in search index. This should be defined by the derived class and never overridden!
        :param query_string:
        :param args:
        :param kwargs:
        :returns list of search records:
        """
        raise NotImplementedError

    @classmethod
    def insert_async(cls, resource_object, *args, **kwargs):
        """
        Wrapper around _internal_insert. At its simplest, simply converts between koala resource objects and native
        search record models. May be overridden. Must call cls._internal_insert.
        :param resource_object:
        :param args:
        :param kwargs:
        :returns future
        """
        search_record = cls._convert_resource_object_to_search_record(resource_object, **kwargs)
        return cls._internal_insert(search_record, *args, **kwargs)

    @classmethod
    def get_async(cls, resource_object_uid, *args, **kwargs):
        """
        Wrapper around _internal_get. At its simplest, simply converts between koala resource objects and native
        search record models. May be overridden. Must call cls._internal_get.
        :param resource_object_uid:
        :param args:
        :param kwargs:
        :returns future
        """
        return cls._internal_get(resource_object_uid, *args, **kwargs)

    @classmethod
    def update_async(cls, resource_object, *args, **kwargs):
        """
        Wrapper around _internal_update. At its simplest, simply converts between koala resource objects and native
        search record models. May be overridden. Must call cls._internal_update.
        :param resource_object:
        :param args:
        :param kwargs:
        :returns future
        """
        search_record = cls._convert_resource_object_to_search_record(resource_object)
        return cls._internal_update(search_record, *args, **kwargs)

    @classmethod
    def delete_async(cls, resource_object_uid, *args, **kwargs):
        """
        Wrapper around _internal_delete. At its simplest, simply converts between koala resource objects and native
        search record models. May be overridden. Must call cls._internal_delete.
        :param resource_object_uid:
        :param args:
        :param kwargs:
        """
        return cls._internal_delete(resource_object_uid, *args, **kwargs)

    @classmethod
    def search_async(cls, query_string, *args, **kwargs):
        """
        Wrapper around _internal_search. At its simplest, simply converts between koala resource objects and native
        search record models. May be overridden. Must call cls._internal_search.
        :param query_string:
        :param args:
        :param kwargs:
        :returns future
        """
        return cls._internal_search(query_string=query_string, *args, **kwargs)

    @classmethod
    def insert(cls, resource_object, *args, **kwargs):
        """
        Wrapper around insert_async to automatically resolve async future. May be overridden.
        Must call cls.insert_async.
        :param resource_object:
        :param args:
        :param kwargs:
        :returns insert result:
        """
        insert_future = cls.insert_async(resource_object, *args, **kwargs)
        return cls.get_future_result(insert_future)

    @classmethod
    def get(cls, resource_object_uid, *args, **kwargs):
        """
        Wrapper around get_async to automatically resolve async future. May be overridden. Must call cls.get_async.
        :param resource_object_uid:
        :param args:
        :param kwargs:
        :returns get result:
        """
        get_future = cls.get_async(resource_object_uid, *args, **kwargs)
        return cls.get_future_result(get_future)

    @classmethod
    def update(cls, resource_object, *args, **kwargs):
        """
        Wrapper around update_async to automatically resolve async future. May be overridden.
        Must call cls.update_async.
        :param resource_object:
        :param args:
        :param kwargs:
        :returns update result:
        """
        update_future = cls.update_async(resource_object, *args, **kwargs)
        return cls.get_future_result(update_future)

    @classmethod
    def delete(cls, resource_object_uid, *args, **kwargs):
        """
        Wrapper around delete_async to automatically resolve async future. May be overridden.
        Must call cls.delete_async.
        :param resource_object_uid:
        :param args:
        :param kwargs:
        """
        delete_future = cls.delete_async(resource_object_uid, *args, **kwargs)
        cls.get_future_result(delete_future)

    @classmethod
    def search(cls, query_string, *args, **kwargs):
        """
        Wrapper around search_async to automatically resolve async future. May be overridden.
        Must call cls.search_async.
        :param query_string:
        :param args:
        :param kwargs:
        :return list of search records:
        """
        search_future = cls.search_async(query_string=query_string, *args, **kwargs)
        return cls.get_future_result(search_future)

    @classmethod
    def _convert_resource_object_to_search_record(cls, resource_object, **kwargs):
        """
        Derived class should implement. Method to convert between koala resource objects and native search_records.
        :param resource_object:
        :returns search_record:
        """
        raise NotImplementedError

    @classmethod
    def _convert_search_result_to_result_object(cls, search_result):
        """
        Derived class should implement. Method to convert between native search results and result object.
        :param search_result:
        :returns result_object:
        """
        raise NotImplementedError

    @classmethod
    def get_future_result(cls, future):
        """
        Derived class may override as necessary. Resolve a future to its resulting value.
        :param future:
        :returns result of future resolution, passed through normalisation function:
        """
        if isinstance(future, list):
            return [cls._normalise_output(item.get_result()) for item in future]
        else:
            return cls._normalise_output(future.get_result())

    @classmethod
    def _normalise_output(cls, output):
        """
        Convert output to normalised objects (resource objects or string uids)
        :param output:
        :return normalised object (could be list of objects):
        """
        return output


try:
    from google.appengine.api import search
except ImportError:
    # Required libraries are not available; skip definition
    pass
else:
    class GAESearchInterface(BaseSearchInterface):
        """
        GAE Search mixins. Implements the base search methods above and adds in some additional helpers. This
        is mainly to provide a consistent interface for all datastores, which is particularly useful for writing mixins.
        """
        _search_document_model = search.Document
        _search_index_model = search.Index

        # These definitions are just for convenience - it means we don't have to import the gae search lib
        # in each of the modules that implement the class.
        atom_field = search.AtomField
        text_field = search.TextField
        html_field = search.HtmlField
        number_field = search.NumberField
        date_field = search.DateField
        geopoint_field = search.GeoField

        @classproperty
        def index(cls):
            return cls._search_index_model(name=cls._index_name)

        @classmethod
        def _internal_insert(cls, search_record, *args, **kwargs):
            """
            Insert search record into the search index.
            :param search_record:
            :param args:
            :param kwargs:
            :returns future:
            :raises search.PutError:
            :raises TypeError:
            :raises ValueError:
            """
            try:
                return cls.index.put_async(search_record, *args, **kwargs)
            except (search.PutError, TypeError, ValueError):
                logging.exception(u'Insert search record into {0} failed.'.format(cls.index.name))
                raise

        @classmethod
        def _internal_get(cls, search_record_uid, *args, **kwargs):
            """
            Get search record with search_record_uid from the search index
            :param search_record_uid:
            :param args:
            :param kwargs:
            :returns future:
            """
            return cls.index.get_async(search_record_uid, *args, **kwargs)

        @classmethod
        def _internal_update(cls, search_record, *args, **kwargs):
            """
            Insert an updated search record into the search index.
            This is essentially an alias of insert (they are equivilant for GAE Full Text Search).
            :param search_record:
            :param args:
            :param kwargs:
            :returns future:
            :raises search.Error:
            :raises TypeError:
            :raises ValueError:
            """
            try:
                return cls.index.put_async(search_record, *args, **kwargs)
            except (search.PutError, TypeError, ValueError):
                logging.exception(u'Updating search record in {0} failed.'.format(cls.index.name))
                raise

        @classmethod
        def _internal_delete(cls, search_record_uid, *args, **kwargs):
            """
            Delete search record from search index
            :param search_record_uid:
            :param args:
            :param kwargs:
            :returns future:
            :raises search.DeleteError:
            :raises ValueError:
            """
            try:
                return cls.index.delete_async(search_record_uid, *args, **kwargs)
            except (search.DeleteError, ValueError):
                logging.exception(u'Delete search record from {0} failed.'.format(cls.index.name))
                raise

        @classmethod
        def _internal_search(cls, query_string, explicit_query_string_overrides=None, cursor_support=False,
                             existing_cursor=None, limit=20, number_found_accuracy=None, offset=None, sort_options=None,
                             returned_fields=None, ids_only=False, snippeted_fields=None, returned_expressions=None,
                             sort_limit=1000, *args, **kwargs):
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
            """

            cursor = None
            compiled_sort_options = None
            compiled_field_expressions = None

            if explicit_query_string_overrides:
                # TODO: use regex to split up the query string and swap out/append the explicit params. At the moment
                # multiple values could be passed for the same category, leading to possible data leaks
                query_fragments = []

                for explicit_param in explicit_query_string_overrides:
                    query_fragments.append(u'{}="{}"'.format(explicit_param[0], explicit_param[1].replace(',', '\,').replace('+', '\+').strip()))

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
            try:
                return cls.index.search_async(query=query)
            except (search.Error, TypeError, ValueError):
                logging.exception(u"Query {0} in {1} failed.".format(query_string, cls.index.name))
                raise

        @classmethod
        def insert_multi_async(cls, resource_objects, *args, **kwargs):
            """
            Insert search records into the search index.
            Not strictly necessary, but keeps interface consistent with NDB.
            :param resource_objects:
            :param args:
            :param kwargs:
            :returns future:
            :raises search.PutError:
            :raises TypeError:
            :raises ValueError:
            """
            search_records = [cls._convert_resource_object_to_search_record(search_record) for search_record in
                              resource_objects]
            try:
                return cls.index.put_async(search_records, *args, **kwargs)
            except (search.PutError, TypeError, ValueError):
                logging.exception(u'Insert search record into {0} failed.'.format(cls.index.name))
                raise

        @classmethod
        def delete_multi_async(cls, resource_object_uids, *args, **kwargs):
            """
            Delete search record from search index.
            Not strictly necessary, but keeps interface consistent with NDB.
            :param resource_object_uids:
            :param args:
            :param kwargs:
            :returns future:
            :raises search.DeleteError:
            :raises ValueError:
            """
            try:
                return cls.index.delete_async(resource_object_uids, *args, **kwargs)
            except (search.DeleteError, ValueError):
                logging.exception(u'Delete search record from {0} failed.'.format(cls.index.name))
                raise

        @classmethod
        def insert_multi(cls, resource_objects, *args, **kwargs):
            """
            Convenience wrapper for the insert_multi_async method to return the result of the future.
            See 'insert_multi_async' for arguments.
            :param resource_objects:
            :param args:
            :param kwargs:
            :returns result of future:
            """
            future = cls.insert_multi_async(resource_objects, *args, **kwargs)
            return cls.get_future_result(future=future)

        @classmethod
        def delete_multi(cls, resource_object_uids, *args, **kwargs):
            """
            Convenience wrapper for the delete_multi_async method to return the result of the future.
            See 'delete_multi_async' for arguments.
            :param resource_object_uids:
            :param args:
            :param kwargs:
            :returns result of future:
            """
            future = cls.delete_multi_async(resource_object_uids, *args, **kwargs)
            return cls.get_future_result(future=future)

        @classmethod
        def _normalise_output(cls, output):
            if isinstance(output, cls._search_document_model):
                return cls._convert_search_document_to_resource_object(output)
            elif isinstance(output, search.SearchResults):
                return cls._convert_search_result_to_result_object(output)
            else:
                return output

        @classmethod
        def _convert_resource_object_to_search_record(cls, resource_object, **kwargs):
            """
            Method to convert between koala resource objects and native search_records. Can be overridden if you want to
            do some custom processing, or if your resource object does not support the 'to_search_doc()' method.
            :param resource_object:
            :returns search_record:
            """
            search_doc = cls._search_document_model(
                doc_id=resource_object.uid,
                fields=resource_object.to_search_doc() + [cls.atom_field(name='uid', value=resource_object.uid)]
            )
            return search_doc

        @classmethod
        def _convert_search_result_to_result_object(cls, search_result):
            """
            Method to convert between native search results and result object. This is simply a helper method. You may
            want to use all of the features of GAE Search, in which case you can modify the class to not call this
            method.
            :param search_result:
            :returns result_object:
            """
            cursor = None
            parsed_results = []
            check_duplicates = cls._check_duplicates
            duplicate_properties = None

            for result in search_result:
                parsed_result = cls._result(uid=result.doc_id)

                if check_duplicates:
                    duplicate_properties = cls.find_duplicate_properties(result)

                if duplicate_properties:
                    for field in result.fields:
                        cls._set_result_property_with_duplicate_check(result_object=parsed_result, field=field, duplicates=duplicate_properties)
                    for field_name in duplicate_properties:
                        setattr(parsed_result, field_name, [field.value for field in result[field_name]])
                else:
                    for field in result.fields:
                        cls._set_result_property(result_object=parsed_result, field=field)

                parsed_results.append(parsed_result)

            if search_result.cursor:
                cursor = search_result.cursor.web_safe_string

            return cls._search_result(results_count=search_result.number_found, results=parsed_results, cursor=cursor)

        @classmethod
        def _convert_search_document_to_resource_object(cls, search_document):
            """
            Derived class should implement. Method to convert between native search documents and koala resource
            objects. This is specific to the GAE interface because the search doc is different to the search results.
            :param search_document:
            :returns resource_object:
            """
            return search_document

        @staticmethod
        def find_duplicate_properties(result):
            """
            For each field in a result check if there are multiple instances with the same name
            :param result:
            :return:
            """
            seen = set()
            seen_add = seen.add
            return set(field.name for field in result.fields if field.name in seen or seen_add(field.name))

        @classmethod
        def _set_result_property(cls, result_object, field):
            """
            You should override this method if you need to do some fancy parsing of the field values, or even skip them,
            when parsing the search results. By default we simply set every field in the result object.
            :param result_object:
            :param field (from ScoredDocument):
            """
            try:
                setattr(result_object, field.name, field.value)
            except AttributeError:
                # If we try to set an attribute that already exists then we will get an error. Generally this will only
                # happen with custom result objects that use @property
                pass

        @classmethod
        def _set_result_property_with_duplicate_check(cls, result_object, field, duplicates):
            """
            Same as _set_result_property but
            :param result_object:
            :param field (from ScoredDocument):
            """
            if field.name not in duplicates:
                setattr(result_object, field.name, field.value)
