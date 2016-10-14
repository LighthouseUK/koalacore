# -*- coding: utf-8 -*-
"""
    koalacore.test_api
    ~~~~~~~~~~~~~~~~~~


    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import unittest
from koalacore.api import parse_api_config, GAEAPI
from koalacore.datastore import DatastoreMock
from koalacore.search import SearchMock
from google.appengine.ext import testbed

__author__ = 'Matt Badger'


test_def = {
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


class TestAPIConfigParser(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()

    def tearDown(self):
        self.testbed.deactivate()

    def test_config_parser(self):
        test_api = parse_api_config(api_definition=test_def)

        self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
        self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
        self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
        self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')
