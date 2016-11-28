# -*- coding: utf-8 -*-
"""
    koalacore.test_task
    ~~~~~~~~~~~~~~~~~~
    Very simple task handler that will respond to internal tasks only (only tasks added via admin users/the system).

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
from webtest import TestApp
from koalacore.task import TaskHandler
from blinker import signal
from koalacore.api import parse_api_config, parse_api_path
from google.appengine.ext import testbed
from google.appengine.api import users

__author__ = 'Matt Badger'


class TestTaskHandler(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self.testbed.init_user_stub()
        self.testbed.init_taskqueue_stub(root_path='./koalacore/tests')
        self.task_queue = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

        self.test_api = self.build_api()
        self.testapp = TestApp(TaskHandler(api_instance=self.test_api))

    def tearDown(self):
        self.testbed.deactivate()

    def loginUser(self, email='user@example.com', id='123', is_admin=False):
        self.testbed.setup_env(
            user_email=email,
            user_id=id,
            user_is_admin='1' if is_admin else '0',
            overwrite=True)

    def build_api(self):
        test_config = {
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
        return parse_api_config(api_definition=test_config)

    def test_admin_only(self):
        # Note that `identity_uid` will be stripped from the request by the handler. We include it here to test that
        # the parameter stripping works correctly.
        test_params = {
            'api_method': 'companies.get',
            'identity_uid': 'shouldnevervalidate',
            'resource_uid': 'sdoigsfhgijdgjadjdgjsgfj',
        }

        self.assertFalse(users.get_current_user())
        # Send request an make sure fails
        response = self.testapp.post('/_taskhandler', params=test_params, expect_errors=True)
        self.assertEqual(response.status_int, 401)

        # Send request and make sure fails -- user but not admin
        self.loginUser()
        response = self.testapp.post('/_taskhandler', params=test_params, expect_errors=True)
        self.assertEqual(response.status_int, 403)

        # Send request and make sure passes
        self.loginUser(is_admin=True)
        self.assertTrue(users.is_current_user_admin())
        response = self.testapp.post('/_taskhandler', params=test_params)
        self.assertEqual(response.status_int, 200)

    # def test_request_received(self):
    #     self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
    #     self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
    #     self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
    #     self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')
    #
    # def test_api_called(self):
    #     # Connect to receiver in api method to ensure that it is called
    #     self.assertTrue(hasattr(test_api, 'companies'), 'Companies API is missing')
    #     self.assertTrue(hasattr(test_api.companies, 'users'), 'Companies Users API is missing')
    #     self.assertTrue(hasattr(test_api.companies, 'entries'), 'Companies Entries API is missing')
    #     self.assertTrue(hasattr(test_api.companies.entries, 'results'), 'Companies Entries Results API is missing')
