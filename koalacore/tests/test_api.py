# -*- coding: utf-8 -*-
"""
    koalacore.test_api
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
import unittest
from blinker import signal
from koalacore.api import parse_api_config, parse_api_path
from koalacore.resource import Resource, StringProperty, ResourceUIDProperty, ResourceUID, DateProperty, DateTimeProperty, TimeProperty
from koalacore.rpc import NDBDatastore, GAESearch, UniqueValueRequired, NDBInsert
from google.appengine.ext import testbed
from google.appengine.ext import deferred
import google.appengine.ext.ndb as ndb
import copy
import pickle
from google.appengine.datastore import datastore_stub_util  # noqa

__author__ = 'Matt Badger'


# TODO: attache to all hooks and keep a count of the activations. They should match the number of methods x3. Easy way
# to test that they have all been activated.


class INodeResource(Resource):
    file_name_example = StringProperty('fne', verbose_name='File Name Example')
    # TODO: store owner (user), owner group(s) [list]
    # TODO: store permissions for owner, group, global
    # TODO: store additional ACL rules
    # TODO: store additional attributes e.g. read-only
    # TODO: store last access timestamp?
    # TODO: default owner group is self? or parent?
    pass


class INode(Resource):
    file_name = StringProperty('fn', verbose_name='File Name', unique=True, strip_whitespace=True, force_lowercase=True, fuzzy_search_support=True, complex_fuzzy=True)
    key_test = ResourceUIDProperty('kt', verbose_name='Key Test', repeated=True)
    date_test = DateProperty('dt', auto_now_add=True)
    datetime_test = DateTimeProperty('dtt', auto_now_add=True)
    time_test = TimeProperty('tt', auto_now_add=True)


class IdentityResource(Resource):
    # TODO: store the identity name e.g. username or the client name
    # TODO: contains additional ACL/unix permissions granted to the user
    # TODO: contains all of the groups the user is a member of
    # TODO: contains a map of groups to names
    # TODO: contains a map of groups to permissions

    pass


class Identity(Resource):
    pass


def create_user_identity(sender, **kwargs):
    # TODO: hook into user.create method and generate an identity for them automatically. Must do in transaction.
    pass


test_def = {
    'companies': {
        'strict_parent': False,
        'sub_apis': {
            'users': {
                'strict_parent': True,
            },
            'entries': {
                'strict_parent': True,
                'sub_apis': {
                    'results': {
                        'strict_parent': True,
                    }
                },
            }
        },
    }
}


class SignalTester(object):

    def __init__(self):
        self.hook_activations = {}
        self.filter_activations = {}

    def hook_subscriber(self, sender, **kwargs):
        hook_name = kwargs.get('hook_name', 'anonymous')

        if hook_name not in self.hook_activations:
            self.hook_activations[hook_name] = {}

        try:
            self.hook_activations[hook_name][sender].append(kwargs)
        except (KeyError, AttributeError):
            self.hook_activations[hook_name] = {
                sender: [kwargs]
            }


class AsyncSignalTester(object):

    def __init__(self):
        self.hook_activations = {}
        self.hook_activations_count = 0
        self.filter_activations = {}
        self.filter_activations_count = 0

    @ndb.tasklet
    def hook_subscriber(self, sender, **kwargs):
        hook_name = kwargs.get('hook_name', 'anonymous')

        if hook_name not in self.hook_activations:
            self.hook_activations[hook_name] = {}

        try:
            self.hook_activations[hook_name][sender].append(kwargs)
        except (KeyError, AttributeError):
            self.hook_activations[hook_name] = {
                sender: [kwargs]
            }

        self.hook_activations_count += 1


def build_api(api_config=None, **overrides):
    if api_config is None:
        api_config = copy.deepcopy(test_def)
    return parse_api_config(api_definition=api_config, **overrides)


class TestAPIConfigParserDefaults(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_search_stub()
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    def test_config_parser(self):
        test_api = build_api()

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')

    def test_path_parser(self):
        test_api = build_api()
        api_method = parse_api_path(api=test_api, path='.companies.get')
        pass


class TestAPIConfigParser(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_search_stub()
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

    def tearDown(self):
        self.testbed.deactivate()

    def test_config_parser(self):
        test_api = build_api()

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')

    def test_companies_methods(self):
        test_api = build_api()

        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')

        for api_method in test_api.companies.methods:
            method_instance = getattr(test_api.companies, api_method, False)
            signal_tester_1 = AsyncSignalTester()
            signal_tester_2 = AsyncSignalTester()

            self.assertTrue(method_instance, u'API method missing')

            signal(method_instance.pre_name).connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
            signal(method_instance.pre_name).connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
            signal(method_instance._full_name).connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
            signal(method_instance._full_name).connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
            signal(method_instance.post_name).connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
            signal(method_instance.post_name).connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
            result_future = method_instance(resource_uid='testresourceid', identity_uid='thisisatestidentitykey')
            result = result_future.get_result()

            self.assertEqual(signal_tester_1.hook_activations_count, 3, u'{} should trigger 3 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[method_instance.pre_name][method_instance]), 1, u'{} should trigger 1 pre hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[method_instance._full_name][method_instance]), 1, u'{} should trigger 1 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[method_instance.post_name][method_instance]), 1, u'{} should trigger 1 post hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[method_instance.pre_name][method_instance]), 1, u'{} should trigger 1 pre hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[method_instance._full_name][method_instance]), 1, u'{} should trigger 1 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[method_instance.post_name][method_instance]), 1, u'{} should trigger 1 post hooks'.format(api_method))


class TestResource(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_search_stub()
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        # TODO: test the advanced attributes for properties e.g. unique, stip, lower

    def tearDown(self):
        self.testbed.deactivate()

    def test_resource_init(self):
        test = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')), ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])
        test.file_name = 'changedfilename'
        # new_uid = test.put(use_cache=False, use_memcache=False)
        new_uid = test.put()
        searchable = test.to_searchable_properties()
        retrieved = new_uid.get()
        retrieved.file_name = 'newfilename'
        pass


class TestGaeApi(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
        self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
        self.testbed.init_memcache_stub()
        self.testbed.init_search_stub()
        # self.testbed.init_taskqueue_stub(root_path='./koalacore/tests', auto_task_running=True)
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        ndb.get_context().clear_cache()
        self.api_config = {
            'companies': {
                'strict_parent': False,
                'resource_model': INode,
                'spi_config': {
                    'datastore_config': {
                        'type': NDBDatastore,
                    },
                    'search_config': {
                        'type': GAESearch,
                    }
                },
                'sub_apis': {
                    'users': {
                        'strict_parent': True,
                    },
                    'entries': {
                        'strict_parent': True,
                        'sub_apis': {
                            'results': {
                                'strict_parent': True,
                            }
                        },
                    }
                },
            }
        }

    def tearDown(self):
        self.testbed.deactivate()

    def test_insert_resource(self):
        test_api = build_api(api_config=self.api_config)
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        result_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        result = result_future.get_result()

        self.assertIsInstance(result, ResourceUID, u'Expected instance of ResourceUID')

        # TODO: try pickling the SPI, SPImethods and see what breaks

        tasks = self.task_queue.get_filtered_tasks()
        while tasks:
            tasks = self.task_queue.get_filtered_tasks()

        search_result_future = test_api.companies.search(query_string='file_name: {}'.format(test_resource.file_name))
        search_result = search_result_future.get_result()
        self.assertEqual(search_result.results_count, 1, u'Query returned incorrect count')
        self.assertEqual(len(search_result.results), 1, u'Query returned incorrect number of results')

    def test_insert_resource_unique_colission(self):
        test_api = build_api(api_config=self.api_config)
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        result_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        result = result_future.get_result()

        test_resource_2 = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])
        with self.assertRaises(UniqueValueRequired):
            result_future_2 = test_api.companies.insert(resource=test_resource_2, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
            result_2 = result_future_2.get_result()

        # TODO: test that the resource is not partially committed

    def test_get_resource(self):
        test_api = build_api(api_config=self.api_config)
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        insert_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource_uid = insert_future.get_result()

        get_future = test_api.companies.get(resource_uid=resource_uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource = get_future.get_result()

        self.assertTrue(resource, u'Stored value mismatch')
        self.assertTrue(resource.uid, u'Stored value mismatch')

        for code_name in test_resource._properties:
            if INode._properties[code_name]._repeated:
                self.assertItemsEqual(getattr(resource, code_name),
                                      getattr(test_resource, code_name),
                                      u'`{}` value mismatch'.format(code_name))
            else:
                self.assertEqual(getattr(resource, code_name),
                                 getattr(test_resource, code_name),
                                 u'`{}` value mismatch'.format(code_name))

    def test_update_resource(self):
        test_api = build_api(api_config=self.api_config)
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        insert_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource_uid = insert_future.get_result()

        get_future = test_api.companies.get(resource_uid=resource_uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource = get_future.get_result()

        resource.file_name = 'modifiedfilename'

        update_future = test_api.companies.update(resource=resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        updated_resource_uid = update_future.get_result()

        updated_get_future = test_api.companies.get(resource_uid=updated_resource_uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        updated_resource = updated_get_future.get_result()

        for code_name in test_resource._properties:
            if INode._properties[code_name]._repeated:
                self.assertItemsEqual(getattr(updated_resource, code_name),
                                      getattr(resource, code_name),
                                      u'`{}` value mismatch'.format(code_name))
            else:
                self.assertEqual(getattr(updated_resource, code_name),
                                 getattr(resource, code_name),
                                 u'`{}` value mismatch'.format(code_name))

    def test_delete_resource(self):
        test_api = build_api(api_config=self.api_config)
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])
        test_resource_2 = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        insert_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource_uid = insert_future.get_result()

        get_future = test_api.companies.get(resource_uid=resource_uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource = get_future.get_result()

        self.assertTrue(resource, u'Resource should exist')

        delete_future = test_api.companies.delete(resource_uid=resource.uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        deleted_resource_uid = delete_future.get_result()

        # Check that the deleted resource_uid matches resource.uid
        self.assertEqual(deleted_resource_uid, resource_uid, u'Deleted key and resource uid should match')

        deleted_get_future = test_api.companies.get(resource_uid=resource_uid, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        result = deleted_get_future.get_result()

        # Check that the resource has actually been removed from the datastore
        self.assertFalse(result, u'Resource should have been deleted')

        # Check that the unique value locks have been removed by trying to insert them again
        insert_future = test_api.companies.insert(resource=test_resource_2, identity_uid='thisisatestidentitykey', use_cache=False, use_memcache=False)
        resource_uid_2 = insert_future.get_result()

        self.assertTrue(resource_uid_2, u'Unique lock check failed after delete')
