# -*- coding: utf-8 -*-
"""
    koalacore.test_api
    ~~~~~~~~~~~~~~~~~~


    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import unittest
from blinker import signal
from koalacore.api import parse_api_config, GAEAPI, GAEDatastoreAPI, BaseAPI, Resource
from koalacore.datastore import DatastoreMock, NDBResource
from koalacore.search import SearchMock
from google.appengine.ext import testbed
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


SECURITY_API_CONFIG = {
    'type': Security,
    'strict_parent': False,
    'create_cache': True,
    'sub_apis': {
        'inode': {
            'type': GAEDatastoreAPI,
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
        self.hook_activations[sender] = kwargs


class AsyncSignalTester(object):

    def __init__(self):
        self.hook_activations = {}
        self.filter_activations = {}

    @ndb.tasklet
    def hook_subscriber(self, sender, **kwargs):
        self.hook_activations[sender] = kwargs


class TestAPIConfigParser(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

    def tearDown(self):
        self.testbed.deactivate()

    def test_config_parser(self):
        test_api = parse_api_config(api_definition=test_def)
        signal_tester_1 = AsyncSignalTester()
        signal_tester_2 = AsyncSignalTester()

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')

        signal('pre_get').connect(signal_tester_1.hook_subscriber, sender=test_api.companies)
        signal('pre_get').connect(signal_tester_2.hook_subscriber, sender=test_api.companies)

        result_future = test_api.companies.get(resource_uid='testresourceid', id='thisisatestidentitykey')
        result = result_future.get_result()
        self.assertTrue(signal_tester_1.hook_activations[test_api.companies], u'Read should trigger 1 hooks')
        self.assertTrue(signal_tester_2.hook_activations[test_api.companies], u'Read should trigger 1 hooks')




