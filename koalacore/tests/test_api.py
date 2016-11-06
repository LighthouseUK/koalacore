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
from koalacore.api import parse_api_config
from koalacore.resource import Resource, StringProperty, ResourceUIDProperty, ResourceUID
from koalacore.datastore import DatastoreMock
from koalacore.search import SearchMock
from google.appengine.ext import testbed
from google.appengine.ext import deferred
import google.appengine.ext.ndb as ndb
import copy

__author__ = 'Matt Badger'


# TODO: attache to all hooks and keep a count of the activations. They should match the number of methods x3. Easy way
# to test that they have all been activated.


class INodeResource(Resource):
    file_name = StringProperty('fn', verbose_name='File Name')
    # TODO: store owner (user), owner group(s) [list]
    # TODO: store permissions for owner, group, global
    # TODO: store additional ACL rules
    # TODO: store additional attributes e.g. read-only
    # TODO: store last access timestamp?
    # TODO: default owner group is self? or parent?
    pass


class INode(Resource):
    file_name = StringProperty('fn', verbose_name='File Name', unique=True, strip_whitespace=True, force_lowercase=True)
    key_test = ResourceUIDProperty('kt', verbose_name='Key Test', repeated=True)


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


class TestResource(unittest.TestCase):
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

    def test_resource_init(self):
        test = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')), ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])
        new_uid = test.put()
        pass


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

    def build_api(self):
        test_config = copy.deepcopy(test_def)
        return parse_api_config(api_definition=test_config)

    def test_config_parser(self):
        test_api = self.build_api()

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')

    def test_companies_methods(self):
        test_api = self.build_api()

        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')

        for api_method in test_api.companies.methods:
            signal_tester_1 = AsyncSignalTester()
            signal_tester_2 = AsyncSignalTester()
            pre_name = 'pre_{}'.format(api_method)
            hook_name = api_method
            post_name = 'post_{}'.format(api_method)

            method_instance = getattr(test_api.companies, api_method, False)
            self.assertTrue(method_instance, u'API method missing')

            signal(pre_name).connect(signal_tester_1.hook_subscriber, sender=method_instance)
            signal(pre_name).connect(signal_tester_2.hook_subscriber, sender=method_instance)
            signal(hook_name).connect(signal_tester_1.hook_subscriber, sender=method_instance)
            signal(hook_name).connect(signal_tester_2.hook_subscriber, sender=method_instance)
            signal(post_name).connect(signal_tester_1.hook_subscriber, sender=method_instance)
            signal(post_name).connect(signal_tester_2.hook_subscriber, sender=method_instance)
            result_future = method_instance(resource_uid='testresourceid', identity_uid='thisisatestidentitykey')
            result = result_future.get_result()

            self.assertEqual(signal_tester_1.hook_activations_count, 3, u'{} should trigger 3 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[pre_name][method_instance]), 1, u'{} should trigger 1 pre hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[hook_name][method_instance]), 1, u'{} should trigger 1 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_1.hook_activations[post_name][method_instance]), 1, u'{} should trigger 1 post hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[pre_name][method_instance]), 1, u'{} should trigger 1 pre hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[hook_name][method_instance]), 1, u'{} should trigger 1 hooks'.format(api_method))
            self.assertEqual(len(signal_tester_2.hook_activations[post_name][method_instance]), 1, u'{} should trigger 1 post hooks'.format(api_method))

    def test_get(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.get)
        signal('pre_get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.get)
        signal('get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.get)
        signal('get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.get)
        signal('post_get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.get)
        signal('post_get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.get)
        result_future = test_api.companies.get(resource_uid='testresourceid', identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'Get should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_get'][test_api.companies.get]), 1, u'Get should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['get'][test_api.companies.get]), 1, u'Get should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_get'][test_api.companies.get]), 1, u'Get should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_get'][test_api.companies.get]), 1, u'Get should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['get'][test_api.companies.get]), 1, u'Get should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_get'][test_api.companies.get]), 1, u'Get should trigger 1 post hooks')

    def test_insert(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_insert').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.insert)
        signal('pre_insert').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.insert)
        signal('insert').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.insert)
        signal('insert').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.insert)
        signal('post_insert').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.insert)
        signal('post_insert').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.insert)
        result_future = test_api.companies.insert(resource_object=IdentityResource(id='testresourceuid'), identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'insert should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_insert'][test_api.companies.insert]), 1, u'insert should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['insert'][test_api.companies.insert]), 1, u'insert should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_insert'][test_api.companies.insert]), 1, u'insert should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_insert'][test_api.companies.insert]), 1, u'insert should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['insert'][test_api.companies.insert]), 1, u'insert should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_insert'][test_api.companies.insert]), 1, u'insert should trigger 1 post hooks')

    def test_update(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_update').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.update)
        signal('pre_update').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.update)
        signal('update').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.update)
        signal('update').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.update)
        signal('post_update').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.update)
        signal('post_update').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.update)
        result_future = test_api.companies.update(resource_object=IdentityResource(id='testresourceuid'), identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'update should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_update'][test_api.companies.update]), 1, u'update should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['update'][test_api.companies.update]), 1, u'update should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_update'][test_api.companies.update]), 1, u'update should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_update'][test_api.companies.update]), 1, u'update should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['update'][test_api.companies.update]), 1, u'update should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_update'][test_api.companies.update]), 1, u'update should trigger 1 post hooks')

    def test_delete(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_delete').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.delete)
        signal('pre_delete').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.delete)
        signal('delete').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.delete)
        signal('delete').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.delete)
        signal('post_delete').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.delete)
        signal('post_delete').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.delete)
        result_future = test_api.companies.delete(resource_uid='testresourceid', identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'delete should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_delete'][test_api.companies.delete]), 1, u'delete should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['delete'][test_api.companies.delete]), 1, u'delete should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_delete'][test_api.companies.delete]), 1, u'delete should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_delete'][test_api.companies.delete]), 1, u'delete should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['delete'][test_api.companies.delete]), 1, u'delete should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_delete'][test_api.companies.delete]), 1, u'delete should trigger 1 post hooks')

    def test_search(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_search').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.search)
        signal('pre_search').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.search)
        signal('search').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.search)
        signal('search').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.search)
        signal('post_search').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.search)
        signal('post_search').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.search)
        result_future = test_api.companies.search(query_string='testresourceid', identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'search should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_search'][test_api.companies.search]), 1, u'search should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['search'][test_api.companies.search]), 1, u'search should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_search'][test_api.companies.search]), 1, u'search should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_search'][test_api.companies.search]), 1, u'search should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['search'][test_api.companies.search]), 1, u'search should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_search'][test_api.companies.search]), 1, u'search should trigger 1 post hooks')

    def test_query(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_query').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.query)
        signal('pre_query').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.query)
        signal('query').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.query)
        signal('query').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.query)
        signal('post_query').connect(signal_tester_1.hook_subscriber, sender=test_api.companies.query)
        signal('post_query').connect(signal_tester_2.hook_subscriber, sender=test_api.companies.query)
        result_future = test_api.companies.query(query_params='testresourceid', identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(signal_tester_1.hook_activations_count, 3, u'query should trigger 3 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['pre_query'][test_api.companies.query]), 1, u'query should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['query'][test_api.companies.query]), 1, u'query should trigger 1 hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_query'][test_api.companies.query]), 1, u'query should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_query'][test_api.companies.query]), 1, u'query should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['query'][test_api.companies.query]), 1, u'query should trigger 1 hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_query'][test_api.companies.query]), 1, u'query should trigger 1 post hooks')




