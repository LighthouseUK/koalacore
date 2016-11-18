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
from koalacore.resource import Resource, StringProperty, ResourceUIDProperty, ResourceUID, DateProperty, DateTimeProperty, TimeProperty
from koalacore.spi import NDBDatastore, GAESearch, UniqueValueRequired
from google.appengine.ext import testbed
from google.appengine.ext import deferred
import google.appengine.ext.ndb as ndb
import copy
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
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        ndb.get_context().clear_cache()

    def tearDown(self):
        self.testbed.deactivate()

    def build_api(self):
        ndb_api_config = {
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
        return parse_api_config(api_definition=ndb_api_config)

    def test_insert_resource(self):
        test_api = self.build_api()
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        result_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertIsInstance(result, ResourceUID, u'Expected instance of ResourceUID')

    def test_insert_resource_unique_colission(self):
        test_api = self.build_api()
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        result_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey')
        result = result_future.get_result()

        test_resource_2 = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])
        with self.assertRaises(UniqueValueRequired):
            result_future_2 = test_api.companies.insert(resource=test_resource_2, identity_uid='thisisatestidentitykey')
            result_2 = result_future_2.get_result()

        # TODO: test that the resource is not partially committed

    def test_get_resource(self):
        test_api = self.build_api()
        test_resource = INode(file_name='examplefilename', key_test=[ResourceUID(raw=ndb.Key(INodeResource, 'test1')),
                                                                     ResourceUID(raw=ndb.Key(INodeResource, 'test2'))])

        insert_future = test_api.companies.insert(resource=test_resource, identity_uid='thisisatestidentitykey')
        resource_uid = insert_future.get_result()

        get_future = test_api.companies.get(resource_uid=resource_uid, identity_uid='thisisatestidentitykey')
        resource = get_future.get_result()

        self.assertTrue(resource, u'Stored value mismatch')
        self.assertTrue(resource.uid, u'Stored value mismatch')

        if ResourceUID(raw=ndb.Key(INodeResource, 'test1')) == ResourceUID(raw=ndb.Key(INodeResource, 'test1')):
            pass

        for code_name in test_resource._properties:
            if INode._properties[code_name]._repeated:
                self.assertItemsEqual(getattr(resource, code_name),
                                      getattr(test_resource, code_name),
                                      u'`{}` value mismatch'.format(code_name))
            else:
                self.assertEqual(getattr(resource, code_name),
                                 getattr(test_resource, code_name),
                                 u'`{}` value mismatch'.format(code_name))



#
# class TestCompany(unittest.TestCase):
#     def setUp(self):
#         # First, create an instance of the Testbed class.
#         self.testbed = testbed.Testbed()
#         # Then activate the testbed, which prepares the service stubs for use.
#         self.testbed.activate()
#         # Next, declare which service stubs you want to use.
#         self.testbed.init_datastore_v3_stub()
#         self.testbed.init_memcache_stub()
#         self.testbed.init_search_stub()
#         self.testbed.init_taskqueue_stub(root_path='.')
#         self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
#         # Remaining setup needed for test cases
#         self.test_company_with_spaces = {
#             'company_name': '  test_company_name  ',
#             'contact_first_name': '  test_contact_first_name  ',
#             'contact_last_name': '  test_contact_last_name  ',
#             'contact_email': '  test_contact_email  ',
#             'contact_phone': '  test_contact_phone  ',
#             'contact_mobile': '  test_contact_mobile  ',
#             'delivery_address_1': '  test_delivery_address_1  ',
#             'delivery_address_2': '  test_delivery_address_2  ',
#             'delivery_address_3': '  test_delivery_address_3  ',
#             'delivery_city': '  test_delivery_city  ',
#             'delivery_county': '  test_delivery_county  ',
#             'delivery_state': '  test_delivery_state  ',
#             'delivery_post_code': '  test_delivery_post_code  ',
#             'delivery_country': '  test_delivery_country  ',
#             'billing_address_1': '  test_billing_address_1  ',
#             'billing_address_2': '  test_billing_address_2  ',
#             'billing_address_3': '  test_billing_address_3  ',
#             'billing_city': '  test_billing_city  ',
#             'billing_county': '  test_billing_county  ',
#             'billing_state': '  test_billing_state  ',
#             'billing_post_code': '  test_billing_post_code  ',
#             'billing_country': '  test_billing_country  ',
#         }
#         self.test_company = {
#             'company_name': 'test_company_name',
#             'contact_first_name': 'test_contact_first_name',
#             'contact_last_name': 'test_contact_last_name',
#             'contact_email': 'test_contact_email',
#             'contact_phone': 'test_contact_phone',
#             'contact_mobile': 'test_contact_mobile',
#             'delivery_address_1': 'test_delivery_address_1',
#             'delivery_address_2': 'test_delivery_address_2',
#             'delivery_address_3': 'test_delivery_address_3',
#             'delivery_city': 'test_delivery_city',
#             'delivery_county': 'test_delivery_county',
#             'delivery_state': 'test_delivery_state',
#             'delivery_post_code': 'test_delivery_post_code',
#             'delivery_country': 'test_delivery_country',
#             'billing_address_1': 'test_billing_address_1',
#             'billing_address_2': 'test_billing_address_2',
#             'billing_address_3': 'test_billing_address_3',
#             'billing_city': 'test_billing_city',
#             'billing_county': 'test_billing_county',
#             'billing_state': 'test_billing_state',
#             'billing_post_code': 'test_billing_post_code',
#             'billing_country': 'test_billing_country',
#         }
#
#     def tearDown(self):
#         self.testbed.deactivate()
#
#     def test_insert_company(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#         self.assertTrue(company_uid)
#
#     def test_get_company(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#
#         retrieved_company = koalacompanies.Companies.get(resource_uid=company_uid)
#         self.assertTrue(retrieved_company, u'Stored value mismatch')
#         self.assertTrue(retrieved_company.uid, u'Stored value mismatch')
#         self.assertTrue(isinstance(retrieved_company.created, datetime), u'Stored value mismatch')
#         self.assertTrue(isinstance(retrieved_company.updated, datetime), u'Stored value mismatch')
#         self.assertEqual(retrieved_company.company_name, self.test_company['company_name'], u'Stored company_name value mismatch')
#         self.assertEqual(retrieved_company.contact_first_name, self.test_company['contact_first_name'], u'Stored contact_first_name value mismatch')
#         self.assertEqual(retrieved_company.contact_last_name, self.test_company['contact_last_name'], u'Stored contact_last_name value mismatch')
#         self.assertEqual(retrieved_company.contact_email, self.test_company['contact_email'], u'Stored contact_email value mismatch')
#         self.assertEqual(retrieved_company.contact_phone, self.test_company['contact_phone'], u'Stored contact_phone value mismatch')
#         self.assertEqual(retrieved_company.contact_mobile, self.test_company['contact_mobile'], u'Stored contact_mobile value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_1, self.test_company['delivery_address_1'], u'Stored delivery_address_1 value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_2, self.test_company['delivery_address_2'], u'Stored delivery_address_2 value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_3, self.test_company['delivery_address_3'], u'Stored delivery_address_3 value mismatch')
#         self.assertEqual(retrieved_company.delivery_city, self.test_company['delivery_city'], u'Stored delivery_city value mismatch')
#         self.assertEqual(retrieved_company.delivery_county, self.test_company['delivery_county'], u'Stored delivery_county value mismatch')
#         self.assertEqual(retrieved_company.delivery_state, self.test_company['delivery_state'], u'Stored delivery_state value mismatch')
#         self.assertEqual(retrieved_company.delivery_post_code, self.test_company['delivery_post_code'], u'Stored delivery_post_code value mismatch')
#         self.assertEqual(retrieved_company.delivery_country, self.test_company['delivery_country'], u'Stored delivery_country value mismatch')
#         self.assertEqual(retrieved_company.billing_address_1, self.test_company['billing_address_1'], u'Stored billing_address_1 value mismatch')
#         self.assertEqual(retrieved_company.billing_address_2, self.test_company['billing_address_2'], u'Stored billing_address_2 value mismatch')
#         self.assertEqual(retrieved_company.billing_address_3, self.test_company['billing_address_3'], u'Stored billing_address_3 value mismatch')
#         self.assertEqual(retrieved_company.billing_city, self.test_company['billing_city'], u'Stored billing_city value mismatch')
#         self.assertEqual(retrieved_company.billing_county, self.test_company['billing_county'], u'Stored billing_county value mismatch')
#         self.assertEqual(retrieved_company.billing_state, self.test_company['billing_state'], u'Stored billing_state value mismatch')
#         self.assertEqual(retrieved_company.billing_post_code, self.test_company['billing_post_code'], u'Stored billing_post_code value mismatch')
#         self.assertEqual(retrieved_company.billing_country, self.test_company['billing_country'], u'Stored billing_country value mismatch')
#
#     def test_insert_company_strip_filter(self):
#         company = koalacompanies.Companies.new(**self.test_company_with_spaces)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#
#         retrieved_company = koalacompanies.Companies.get(resource_uid=company_uid)
#
#         self.assertEqual(retrieved_company.company_name, self.test_company['company_name'], u'Stored company_name value mismatch')
#         self.assertEqual(retrieved_company.contact_first_name, self.test_company['contact_first_name'], u'Stored contact_first_name value mismatch')
#         self.assertEqual(retrieved_company.contact_last_name, self.test_company['contact_last_name'], u'Stored contact_last_name value mismatch')
#         self.assertEqual(retrieved_company.contact_email, self.test_company['contact_email'], u'Stored contact_email value mismatch')
#         self.assertEqual(retrieved_company.contact_phone, self.test_company['contact_phone'], u'Stored contact_phone value mismatch')
#         self.assertEqual(retrieved_company.contact_mobile, self.test_company['contact_mobile'], u'Stored contact_mobile value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_1, self.test_company['delivery_address_1'], u'Stored delivery_address_1 value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_2, self.test_company['delivery_address_2'], u'Stored delivery_address_2 value mismatch')
#         self.assertEqual(retrieved_company.delivery_address_3, self.test_company['delivery_address_3'], u'Stored delivery_address_3 value mismatch')
#         self.assertEqual(retrieved_company.delivery_city, self.test_company['delivery_city'], u'Stored delivery_city value mismatch')
#         self.assertEqual(retrieved_company.delivery_county, self.test_company['delivery_county'], u'Stored delivery_county value mismatch')
#         self.assertEqual(retrieved_company.delivery_state, self.test_company['delivery_state'], u'Stored delivery_state value mismatch')
#         self.assertEqual(retrieved_company.delivery_post_code, self.test_company['delivery_post_code'], u'Stored delivery_post_code value mismatch')
#         self.assertEqual(retrieved_company.delivery_country, self.test_company['delivery_country'], u'Stored delivery_country value mismatch')
#         self.assertEqual(retrieved_company.billing_address_1, self.test_company['billing_address_1'], u'Stored billing_address_1 value mismatch')
#         self.assertEqual(retrieved_company.billing_address_2, self.test_company['billing_address_2'], u'Stored billing_address_2 value mismatch')
#         self.assertEqual(retrieved_company.billing_address_3, self.test_company['billing_address_3'], u'Stored billing_address_3 value mismatch')
#         self.assertEqual(retrieved_company.billing_city, self.test_company['billing_city'], u'Stored billing_city value mismatch')
#         self.assertEqual(retrieved_company.billing_county, self.test_company['billing_county'], u'Stored billing_county value mismatch')
#         self.assertEqual(retrieved_company.billing_state, self.test_company['billing_state'], u'Stored billing_state value mismatch')
#         self.assertEqual(retrieved_company.billing_post_code, self.test_company['billing_post_code'], u'Stored billing_post_code value mismatch')
#         self.assertEqual(retrieved_company.billing_country, self.test_company['billing_country'], u'Stored billing_country value mismatch')
#
#     def test_update_company(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#         retrieved_company = koalacompanies.Companies.get(resource_uid=company_uid)
#
#         retrieved_company.company_name = 'updated_company_name'
#         retrieved_company.contact_first_name = 'updated_contact_first_name'
#         retrieved_company.contact_last_name = 'updated_contact_last_name'
#         retrieved_company.contact_email = 'updated_contact_email'
#         retrieved_company.contact_phone = 'updated_contact_phone'
#         retrieved_company.contact_mobile = 'updated_contact_mobile'
#         retrieved_company.delivery_address_1 = 'updated_delivery_address_1'
#         retrieved_company.delivery_address_2 = 'updated_delivery_address_2'
#         retrieved_company.delivery_address_3 = 'updated_delivery_address_3'
#         retrieved_company.delivery_city = 'updated_delivery_city'
#         retrieved_company.delivery_county = 'updated_delivery_county'
#         retrieved_company.delivery_state = 'updated_delivery_state'
#         retrieved_company.delivery_post_code = 'updated_delivery_post_code'
#         retrieved_company.delivery_country = 'updated_delivery_country'
#         retrieved_company.billing_address_1 = 'updated_billing_address_1'
#         retrieved_company.billing_address_2 = 'updated_billing_address_2'
#         retrieved_company.billing_address_3 = 'updated_billing_address_3'
#         retrieved_company.billing_city = 'updated_billing_city'
#         retrieved_company.billing_county = 'updated_billing_county'
#         retrieved_company.billing_state = 'updated_billing_state'
#         retrieved_company.billing_post_code = 'updated_billing_post_code'
#         retrieved_company.billing_country = 'updated_billing_country'
#
#         koalacompanies.Companies.update(resource_object=retrieved_company)
#         updated_company = koalacompanies.Companies.get(resource_uid=company_uid)
#
#         self.assertEqual(retrieved_company.uid, updated_company.uid, u'UID mismatch')
#         self.assertEqual(retrieved_company.created, updated_company.created, u'Created date has changed')
#         self.assertNotEqual(retrieved_company.updated, updated_company.updated, u'Updated date not changed')
#         self.assertEqual(updated_company.company_name, 'updated_company_name', u'Stored company_name value mismatch')
#         self.assertEqual(updated_company.contact_first_name, 'updated_contact_first_name', u'Stored contact_first_name value mismatch')
#         self.assertEqual(updated_company.contact_last_name, 'updated_contact_last_name', u'Stored contact_last_name value mismatch')
#         self.assertEqual(updated_company.contact_email, 'updated_contact_email', u'Stored contact_email value mismatch')
#         self.assertEqual(updated_company.contact_phone, 'updated_contact_phone', u'Stored contact_phone value mismatch')
#         self.assertEqual(updated_company.contact_mobile, 'updated_contact_mobile', u'Stored contact_mobile value mismatch')
#         self.assertEqual(updated_company.delivery_address_1, 'updated_delivery_address_1', u'Stored delivery_address_1 value mismatch')
#         self.assertEqual(updated_company.delivery_address_2, 'updated_delivery_address_2', u'Stored delivery_address_2 value mismatch')
#         self.assertEqual(updated_company.delivery_address_3, 'updated_delivery_address_3', u'Stored delivery_address_3 value mismatch')
#         self.assertEqual(updated_company.delivery_city, 'updated_delivery_city', u'Stored delivery_city value mismatch')
#         self.assertEqual(updated_company.delivery_county, 'updated_delivery_county', u'Stored delivery_county value mismatch')
#         self.assertEqual(updated_company.delivery_state, 'updated_delivery_state', u'Stored delivery_state value mismatch')
#         self.assertEqual(updated_company.delivery_post_code, 'updated_delivery_post_code', u'Stored delivery_post_code value mismatch')
#         self.assertEqual(updated_company.delivery_country, 'updated_delivery_country', u'Stored delivery_country value mismatch')
#         self.assertEqual(updated_company.billing_address_1, 'updated_billing_address_1', u'Stored billing_address_1 value mismatch')
#         self.assertEqual(updated_company.billing_address_2, 'updated_billing_address_2', u'Stored billing_address_2 value mismatch')
#         self.assertEqual(updated_company.billing_address_3, 'updated_billing_address_3', u'Stored billing_address_3 value mismatch')
#         self.assertEqual(updated_company.billing_city, 'updated_billing_city', u'Stored billing_city value mismatch')
#         self.assertEqual(updated_company.billing_county, 'updated_billing_county', u'Stored billing_county value mismatch')
#         self.assertEqual(updated_company.billing_state, 'updated_billing_state', u'Stored billing_state value mismatch')
#         self.assertEqual(updated_company.billing_post_code, 'updated_billing_post_code', u'Stored billing_post_code value mismatch')
#         self.assertEqual(updated_company.billing_country, 'updated_billing_country', u'Stored billing_country value mismatch')
#
#     def test_delete_company(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#         koalacompanies.Companies.delete(resource_uid=company_uid)
#         retrieved_company = koalacompanies.Companies.get(resource_uid=company_uid)
#         self.assertFalse(retrieved_company)
#
#     def test_insert_search(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         koalacompanies.Companies.insert(resource_object=company)
#
#         tasks = self.task_queue.get_filtered_tasks()
#         self.assertEqual(len(tasks), 1, u'Deferred task missing')
#
#         deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test
#
#         search_result = koalacompanies.Companies.search(
#             query_string='company_name: {}'.format(self.test_company['company_name']))
#         self.assertEqual(search_result.results_count, 1, u'Query returned incorrect count')
#         self.assertEqual(len(search_result.results), 1, u'Query returned incorrect number of results')
#
#     def test_update_search(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#
#         tasks = self.task_queue.get_filtered_tasks()
#         self.assertEqual(len(tasks), 1, u'Invalid number of Deferred tasks')
#
#         deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test
#
#         retrieved_company = koalacompanies.Companies.get(resource_uid=company_uid)
#         retrieved_company.company_name = 'updated_company_name'
#         koalacompanies.Companies.update(resource_object=retrieved_company)
#
#         tasks = self.task_queue.get_filtered_tasks()
#         self.assertEqual(len(tasks), 2, u'Invalid number of Deferred tasks')
#
#         deferred.run(tasks[1].payload)  # Doesn't return anything so nothing to test
#
#         search_result = koalacompanies.Companies.search(query_string='company_name: {}'.format('updated_company_name'))
#         self.assertEqual(search_result.results_count, 1, u'Query returned incorrect count')
#         self.assertEqual(len(search_result.results), 1, u'Query returned incorrect number of results')
#
#     def test_delete_search(self):
#         company = koalacompanies.Companies.new(**self.test_company)
#         company_uid = koalacompanies.Companies.insert(resource_object=company)
#
#         tasks = self.task_queue.get_filtered_tasks()
#         self.assertEqual(len(tasks), 1, u'Invalid number of Deferred tasks')
#
#         deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test
#
#         koalacompanies.Companies.delete(resource_uid=company_uid)
#
#         tasks = self.task_queue.get_filtered_tasks()
#         self.assertEqual(len(tasks), 2, u'Invalid number of Deferred tasks')
#
#         deferred.run(tasks[1].payload)  # Doesn't return anything so nothing to test
#
#         search_result = koalacompanies.Companies.search(
#             query_string='company_name: {}'.format(self.test_company['company_name']))
#         self.assertEqual(search_result.results_count, 0, u'Query returned incorrect count')
#         self.assertEqual(len(search_result.results), 0, u'Query returned incorrect number of results')

