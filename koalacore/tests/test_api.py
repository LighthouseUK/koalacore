# -*- coding: utf-8 -*-
"""
    koalacore.test_api
    ~~~~~~~~~~~~~~~~~~


    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import unittest
from blinker import signal
from koalacore.api import parse_api_config, GAEAPI, GAEDatastoreAPIAsync, BaseAPI, Resource
from koalacore.datastore import DatastoreMock, NDBResource
from koalacore.search import SearchMock
from google.appengine.ext import testbed
from google.appengine.ext import deferred
import google.appengine.ext.ndb as ndb

__author__ = 'Matt Badger'


class Security(BaseAPI):
    def __init__(self, **kwargs):
        # Init the inode and securityid apis. The default values will be ok here. We need to rely on the NDB datastore
        # because of the implementation. It's ok if the resources themselves are kept elsewhere.

        # TODO: for each op, take the resource uid. Should be string. Try to parse key. If fail then build one what we
        # can use as a parent key in the datastore

        # TODO: auto add receivers for the each of the base api methods that go to the authorize method

        # TODO: auto add receivers for create and delete methods so that we can generate inodes for each resource. Do
        # in transaction?

        super(Security, self).__init__(**kwargs)

    def chmod(self):
        # Need to check that the user is authorized to perform this op
        pass

    def chflags(self):
        # Need to check that the user is authorized to perform this op
        pass

    def authorize(self, identity_uid, resource_uid, action):
        """
        identity_uid will generally be the uid for a user, but it could also apply to other 'users' such as client
        credentials in an OAuth2 authentication flow.

        resource_uid is the uid of the resource that the action is to be performed on.

        action is the name of the action that is to be performed on the resource. Might change this to permission.

        :param identity_uid:
        :param resource_uid:
        :param action:
        :return:
        """
        pass


class INodeResource(Resource):
    # TODO: store owner (user), owner group(s) [list]
    # TODO: store permissions for owner, group, global
    # TODO: store additional ACL rules
    # TODO: store additional attributes e.g. read-only
    # TODO: store last access timestamp?
    # TODO: default owner group is self? or parent?
    pass


class INode(NDBResource):
    pass


class IdentityResource(Resource):
    # TODO: store the identity name e.g. username or the client name
    # TODO: contains additional ACL/unix permissions granted to the user
    # TODO: contains all of the groups the user is a member of
    # TODO: contains a map of groups to names
    # TODO: contains a map of groups to permissions

    pass


class Identity(NDBResource):
    pass


def create_user_identity(sender, **kwargs):
    # TODO: hook into user.create method and generate an identity for them automatically. Must do in transaction.
    pass


@ndb.tasklet
def check_permissions(sender, identity_uid, resource_uid, **kwargs):
    # TODO: call the authorize method
    # how do we know if it is strict parent or not?
    # Don't want to pass the api instance around if we can avoid it.
    pass


@ndb.tasklet
def get_uid_and_check_permissions(sender, identity_uid, resource_object, **kwargs):
    if not resource_object.uid:
        raise ValueError('Resource object does not have a valid UID')
    yield check_permissions(sender=sender, identity_uid=identity_uid, resource_uid=resource_object.uid, **kwargs)


SECURITY_API_CONFIG = {
    'type': Security,
    'strict_parent': False,
    'create_cache': True,
    'sub_apis': {
        'inode': {
            'type': GAEDatastoreAPIAsync,
            'resource_model': INodeResource,
            'strict_parent': False,
            'datastore_config': {
                'type': INode,
                'datastore_model': 'model',
                'resource_model': 'model',
            },
            'search_config': {
                'type': SearchMock,
            },
        },
        'identity': {
            'resource_model': 'model',
            'strict_parent': False,
            'datastore_config': {
                'type': DatastoreMock,
                'datastore_model': 'model',
                'resource_model': 'model',
            },
            'search_config': {
                'type': SearchMock,
            },
        },
    }
}


test_def = {
    'security': SECURITY_API_CONFIG,
    'companies': {
        'type': GAEAPI,
        'resource_model': 'model',
        'strict_parent': False,
        'datastore_config': {
            'type': DatastoreMock,
            'datastore_model': 'model',
            'resource_model': 'model',
        },
        'search_config': {
            'type': SearchMock,
        },
        'sub_apis': {
            'users': {
                'resource_model': 'model',
                'strict_parent': True,
                'datastore_config': {
                    'type': DatastoreMock,
                    'datastore_model': 'model',
                    'resource_model': 'model',
                },
                'search_config': {
                    'type': SearchMock,
                },
                'sub_apis': {

                },
            },
            'entries': {
                'resource_model': 'model',
                'strict_parent': True,
                'datastore_config': {
                    'type': DatastoreMock,
                    'datastore_model': 'model',
                    'resource_model': 'model',
                },
                'search_config': {
                    'type': SearchMock,
                },
                'sub_apis': {
                    'results': {
                        'resource_model': 'model',
                        'strict_parent': True,
                        'datastore_config': {
                            'type': DatastoreMock,
                            'datastore_model': 'model',
                            'resource_model': 'model',
                        },
                        'search_config': {
                            'type': SearchMock,
                        },
                        'sub_apis': {

                        },
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
        self.filter_activations = {}

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

    def build_api(self):
        return parse_api_config(api_definition=test_def)

    def test_config_parser(self):
        test_api = self.build_api()

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')

    def test_get(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.get(resource_uid='testresourceid', id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_get'][test_api.companies]), 1, u'Get should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_get'][test_api.companies]), 1, u'Get should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_get'][test_api.companies]), 1, u'Get should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_get'][test_api.companies]), 1, u'Get should trigger 1 post hooks')

    def test_insert(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_insert').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_insert').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_insert').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_insert').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.insert(resource_object=IdentityResource(), id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_insert'][test_api.companies]), 1, u'Insert should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_insert'][test_api.companies]), 1, u'Insert should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_insert'][test_api.companies]), 1, u'Insert should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_insert'][test_api.companies]), 1, u'Insert should trigger 1 post hooks')

        tasks = self.task_queue.get_filtered_tasks()
        self.assertEqual(len(tasks), 1, u'Deferred task missing')
        # deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test

    def test_update(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_update').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_update').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_update').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_update').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.update(resource_object=IdentityResource(), id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_update'][test_api.companies]), 1, u'Update should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_update'][test_api.companies]), 1, u'Update should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_update'][test_api.companies]), 1, u'Update should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_update'][test_api.companies]), 1, u'Update should trigger 1 post hooks')

        tasks = self.task_queue.get_filtered_tasks()
        self.assertEqual(len(tasks), 1, u'Deferred task missing')
        # deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test

    def test_delete(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_delete').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_delete').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_delete').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_delete').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.delete(resource_uid='testresourceid', id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_delete'][test_api.companies]), 1, u'Delete should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_delete'][test_api.companies]), 1, u'Delete should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_delete'][test_api.companies]), 1, u'Delete should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_delete'][test_api.companies]), 1, u'Delete should trigger 1 post hooks')

        tasks = self.task_queue.get_filtered_tasks()
        self.assertEqual(len(tasks), 1, u'Deferred task missing')
        # deferred.run(tasks[0].payload)  # Doesn't return anything so nothing to test

    def test_search(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_search').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_search').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_search').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_search').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.search(query_string='testresourceid', id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_search'][test_api.companies]), 1, u'Search should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_search'][test_api.companies]), 1, u'Search should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_search'][test_api.companies]), 1, u'Search should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_search'][test_api.companies]), 1, u'Search should trigger 1 post hooks')

    def test_query(self):
        test_api = self.build_api()
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        signal('pre_query').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_query').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        signal('post_query').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('post_query').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)
        result_future = test_api.companies.query(query_params='testresourceid', id='thisisatestidentitykey')
        result = result_future.get_result()

        self.assertEqual(len(signal_tester_1.hook_activations['pre_query'][test_api.companies]), 1, u'Query should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_1.hook_activations['post_query'][test_api.companies]), 1, u'Query should trigger 1 post hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['pre_query'][test_api.companies]), 1, u'Query should trigger 1 pre hooks')
        self.assertEqual(len(signal_tester_2.hook_activations['post_query'][test_api.companies]), 1, u'Query should trigger 1 post hooks')




