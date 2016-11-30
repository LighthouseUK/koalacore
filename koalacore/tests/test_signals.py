# -*- coding: utf-8 -*-
"""
    koalacore.test_signals
    ~~~~~~~~~~~~~~~~~~
    Testing blinker signals for use in koala api components.

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
from google.appengine.ext import testbed

__author__ = 'Matt Badger'


class SignalTester(object):

    def __init__(self):
        self.hook_activations = {}
        self.hook_activations_count = 0
        self.filter_activations = {}
        self.filter_activations_count = 0

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


class TestSignals(unittest.TestCase):
    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    def test_lambda_receiver(self):
        sender = 'test_sender'
        signal_name = 'lambda_test'
        test_signal = signal(signal_name)
        params = {
            'example1': 'test1',
            'example2': 'test2',
            'hook_name': signal_name
        }

        signal_tester_1 = SignalTester()

        test_signal.connect(lambda s, **k: signal_tester_1.hook_subscriber(s, **k), sender=sender, weak=False)

        test_signal.send('test_sender', **params)

        self.assertEqual(signal_tester_1.hook_activations_count, 1, u'{} should trigger 1 hook(s)'.format(sender))
        self.assertEqual(len(signal_tester_1.hook_activations[signal_name][sender]), 1, u'{} should trigger 1 hook(s)'.format(sender))
