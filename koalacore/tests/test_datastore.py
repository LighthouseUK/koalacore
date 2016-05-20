# -*- coding: utf-8 -*-
"""
    koala.test_datastore
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import unittest
import koala
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from blinker import signal

__author__ = 'Matt Badger'

# name=None, default=None, title='', immutable=False, unique=False, track_revisions=True, strip_whitespace=True, force_lowercase=False


class TestModel(koala.Resource):
    example = koala.ResourceProperty(title='Example')
    random_property = koala.ResourceProperty(title='Random')
    computed = koala.ComputedResourceProperty(title='Computed', compute_function=lambda entity: u'{}{}'.format((entity.example or ''), (entity.random_property or '')))


class NDBTestModel(koala.NDBResource):
    example = ndb.PickleProperty('tme', indexed=False)
    random_property = ndb.StringProperty('tmr', indexed=False)


class SignalTester(object):

    def __init__(self):
        self.hook_activations = {}
        self.filter_activations = {}

    def hook_subscriber(self, sender, **kwargs):
        self.hook_activations[sender] = kwargs


class TestEventedNDBDatastore(unittest.TestCase):
    """
    Test the datastore common API.
    """
    def setUp(self):
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    @staticmethod
    def signal_subscriber(sender, **kwargs):
        return u'Received! kwargs: {}'.format(kwargs)

    def test_signal_inactive(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        # Verify core event flags are active
        self.assertFalse(TestEventedNDB._hook_pre_insert_enabled, u'INSERT pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_insert_enabled, u'INSERT post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_get_enabled, u'GET pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_get_enabled, u'GET post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_update_enabled, u'UPDATE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_update_enabled, u'UPDATE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_patch_enabled, u'PATCH pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_patch_enabled, u'PATCH post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_delete_enabled, u'DELETE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_delete_enabled, u'DELETE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_list_enabled, u'LIST pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_list_enabled, u'LIST post hook should be inactive')

        # Verify NDB event flags are active
        self.assertFalse(TestEventedNDB._hook_transaction_pre_insert_enabled, u'TRANSACTIONAL INSERT pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_insert_enabled, u'TRANSACTIONAL INSERT post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_get_enabled, u'TRANSACTIONAL GET pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_get_enabled, u'TRANSACTIONAL GET post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_update_enabled, u'TRANSACTIONAL UPDATE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_update_enabled, u'TRANSACTIONAL UPDATE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_delete_enabled, u'TRANSACTIONAL DELETE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_delete_enabled, u'TRANSACTIONAL DELETE post hook should be inactive')

    def test_signal_inactive_until_setup_method_called(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        # Create signal refs
        hook_pre_insert = signal(TestEventedNDB.HOOK_PRE_INSERT)
        hook_post_insert = signal(TestEventedNDB.HOOK_POST_INSERT)
        hook_pre_get = signal(TestEventedNDB.HOOK_PRE_GET)
        hook_post_get = signal(TestEventedNDB.HOOK_POST_GET)
        hook_pre_update = signal(TestEventedNDB.HOOK_PRE_UPDATE)
        hook_post_update = signal(TestEventedNDB.HOOK_POST_UPDATE)
        hook_pre_patch = signal(TestEventedNDB.HOOK_PRE_PATCH)
        hook_post_patch = signal(TestEventedNDB.HOOK_POST_PATCH)
        hook_pre_delete = signal(TestEventedNDB.HOOK_PRE_DELETE)
        hook_post_delete = signal(TestEventedNDB.HOOK_POST_DELETE)
        hook_pre_list = signal(TestEventedNDB.HOOK_PRE_LIST)
        hook_post_list = signal(TestEventedNDB.HOOK_POST_LIST)
        # NDB specific hooks
        hook_transaction_pre_insert = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_INSERT)
        hook_transaction_post_insert = signal(TestEventedNDB.HOOK_TRANSACTION_POST_INSERT)
        hook_transaction_pre_get = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_GET)
        hook_transaction_post_get = signal(TestEventedNDB.HOOK_TRANSACTION_POST_GET)
        hook_transaction_pre_update = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_UPDATE)
        hook_transaction_post_update = signal(TestEventedNDB.HOOK_TRANSACTION_POST_UPDATE)
        hook_transaction_pre_delete = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_DELETE)
        hook_transaction_post_delete = signal(TestEventedNDB.HOOK_TRANSACTION_POST_DELETE)

        # Subscribe to signals
        hook_pre_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_patch.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_patch.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_list.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_list.connect(self.signal_subscriber, sender=TestEventedNDB)
        # NDB specific subscriptions
        hook_transaction_pre_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_delete.connect(self.signal_subscriber, sender=TestEventedNDB)

        # DO NOT trigger the setup method. All signal flags should remain False
        # TestEventedNDB.parse_signal_receivers()

        # Verify core event flags are active
        self.assertFalse(TestEventedNDB._hook_pre_insert_enabled, u'INSERT pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_insert_enabled, u'INSERT post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_get_enabled, u'GET pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_get_enabled, u'GET post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_update_enabled, u'UPDATE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_update_enabled, u'UPDATE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_patch_enabled, u'PATCH pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_patch_enabled, u'PATCH post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_delete_enabled, u'DELETE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_delete_enabled, u'DELETE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_pre_list_enabled, u'LIST pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_post_list_enabled, u'LIST post hook should be inactive')

        # Verify NDB event flags are active
        self.assertFalse(TestEventedNDB._hook_transaction_pre_insert_enabled, u'TRANSACTIONAL INSERT pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_insert_enabled, u'TRANSACTIONAL INSERT post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_get_enabled, u'TRANSACTIONAL GET pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_get_enabled, u'TRANSACTIONAL GET post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_update_enabled, u'TRANSACTIONAL UPDATE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_update_enabled, u'TRANSACTIONAL UPDATE post hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_pre_delete_enabled, u'TRANSACTIONAL DELETE pre hook should be inactive')
        self.assertFalse(TestEventedNDB._hook_transaction_post_delete_enabled, u'TRANSACTIONAL DELETE post hook should be inactive')

    def test_signal_activation(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        # Create signal refs
        hook_pre_insert = signal(TestEventedNDB.HOOK_PRE_INSERT)
        hook_post_insert = signal(TestEventedNDB.HOOK_POST_INSERT)
        hook_pre_get = signal(TestEventedNDB.HOOK_PRE_GET)
        hook_post_get = signal(TestEventedNDB.HOOK_POST_GET)
        hook_pre_update = signal(TestEventedNDB.HOOK_PRE_UPDATE)
        hook_post_update = signal(TestEventedNDB.HOOK_POST_UPDATE)
        hook_pre_patch = signal(TestEventedNDB.HOOK_PRE_PATCH)
        hook_post_patch = signal(TestEventedNDB.HOOK_POST_PATCH)
        hook_pre_delete = signal(TestEventedNDB.HOOK_PRE_DELETE)
        hook_post_delete = signal(TestEventedNDB.HOOK_POST_DELETE)
        hook_pre_list = signal(TestEventedNDB.HOOK_PRE_LIST)
        hook_post_list = signal(TestEventedNDB.HOOK_POST_LIST)
        # NDB specific hooks
        hook_transaction_pre_insert = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_INSERT)
        hook_transaction_post_insert = signal(TestEventedNDB.HOOK_TRANSACTION_POST_INSERT)
        hook_transaction_pre_get = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_GET)
        hook_transaction_post_get = signal(TestEventedNDB.HOOK_TRANSACTION_POST_GET)
        hook_transaction_pre_update = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_UPDATE)
        hook_transaction_post_update = signal(TestEventedNDB.HOOK_TRANSACTION_POST_UPDATE)
        hook_transaction_pre_delete = signal(TestEventedNDB.HOOK_TRANSACTION_PRE_DELETE)
        hook_transaction_post_delete = signal(TestEventedNDB.HOOK_TRANSACTION_POST_DELETE)

        # Subscribe to signals
        hook_pre_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_patch.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_patch.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_pre_list.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_post_list.connect(self.signal_subscriber, sender=TestEventedNDB)
        # NDB specific subscriptions
        hook_transaction_pre_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_insert.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_get.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_update.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_pre_delete.connect(self.signal_subscriber, sender=TestEventedNDB)
        hook_transaction_post_delete.connect(self.signal_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        # Verify core event flags are active
        self.assertTrue(TestEventedNDB._hook_pre_insert_enabled, u'INSERT pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_insert_enabled, u'INSERT post hook should be active')
        self.assertTrue(TestEventedNDB._hook_pre_get_enabled, u'GET pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_get_enabled, u'GET post hook should be active')
        self.assertTrue(TestEventedNDB._hook_pre_update_enabled, u'UPDATE pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_update_enabled, u'UPDATE post hook should be active')
        self.assertTrue(TestEventedNDB._hook_pre_patch_enabled, u'PATCH pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_patch_enabled, u'PATCH post hook should be active')
        self.assertTrue(TestEventedNDB._hook_pre_delete_enabled, u'DELETE pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_delete_enabled, u'DELETE post hook should be active')
        self.assertTrue(TestEventedNDB._hook_pre_list_enabled, u'LIST pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_post_list_enabled, u'LIST post hook should be active')

        # Verify NDB event flags are active
        self.assertTrue(TestEventedNDB._hook_transaction_pre_insert_enabled, u'TRANSACTIONAL INSERT pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_post_insert_enabled, u'TRANSACTIONAL INSERT post hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_pre_get_enabled, u'TRANSACTIONAL GET pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_post_get_enabled, u'TRANSACTIONAL GET post hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_pre_update_enabled, u'TRANSACTIONAL UPDATE pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_post_update_enabled, u'TRANSACTIONAL UPDATE post hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_pre_delete_enabled, u'TRANSACTIONAL DELETE pre hook should be active')
        self.assertTrue(TestEventedNDB._hook_transaction_post_delete_enabled, u'TRANSACTIONAL DELETE post hook should be active')

    def test_computed_properties_blank(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_resource = TestModel(example=u'This is a test string')

        self.assertEquals(test_resource.computed, u'This is a test string', u'Computed Property Failed')

    def test_insert_async(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_new = TestModel(example=u'This is a test string')

        signal_tester = SignalTester()

        # Create signal refs
        hook_pre_insert = signal(TestEventedNDB.HOOK_PRE_INSERT)
        hook_post_insert = signal(TestEventedNDB.HOOK_POST_INSERT)

        # Subscribe to signals
        hook_pre_insert.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)
        hook_post_insert.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        future = TestEventedNDB.insert_async(test_new)
        future_result = TestEventedNDB.get_future_result(future=future)

        self.assertTrue(isinstance(future_result, str), u'Insert async should result in string instance.')
        self.assertEquals(len(signal_tester.hook_activations[TestEventedNDB]), 2, u'Insert should trigger 2 hooks')

    def test_get_async(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_new = TestModel(example=u'This is a test string')

        signal_tester = SignalTester()

        # Create signal refs
        hook_pre_get = signal(TestEventedNDB.HOOK_PRE_GET)
        hook_post_get = signal(TestEventedNDB.HOOK_POST_GET)

        # Subscribe to signals
        hook_pre_get.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)
        hook_post_get.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        insert_future = TestEventedNDB.insert_async(resource_object=test_new)
        insert_result = TestEventedNDB.get_future_result(future=insert_future)

        get_future = TestEventedNDB.get_async(resource_uid=insert_result)
        get_result = TestEventedNDB.get_future_result(future=get_future)

        self.assertEquals(get_result.example, u'This is a test string', u'Get async property mismatch.')
        self.assertEquals(len(signal_tester.hook_activations[TestEventedNDB]), 2, u'Get should trigger 2 hooks')

    def test_update_async(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_new = TestModel(example=u'This is a test string')

        signal_tester = SignalTester()

        # Create signal refs
        hook_pre_update = signal(TestEventedNDB.HOOK_PRE_UPDATE)
        hook_post_update = signal(TestEventedNDB.HOOK_POST_UPDATE)

        # Subscribe to signals
        hook_pre_update.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)
        hook_post_update.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        insert_future = TestEventedNDB.insert_async(resource_object=test_new)
        insert_result = TestEventedNDB.get_future_result(future=insert_future)

        get_future = TestEventedNDB.get_async(resource_uid=insert_result)
        get_result = TestEventedNDB.get_future_result(future=get_future)

        get_result.example = u'Edited example property'
        self.assertEquals(get_result._history, {'example': (u'This is a test string', u'Edited example property')}, u'Update history mismatch')
        update_future = TestEventedNDB.update_async(resource_object=get_result)
        update_result = TestEventedNDB.get_future_result(future=update_future)

        self.assertTrue(isinstance(update_result, str), u'Update async should result in string instance.')
        self.assertEquals(len(signal_tester.hook_activations[TestEventedNDB]), 2, u'Update should trigger 2 hooks')

        get_future_2 = TestEventedNDB.get_async(resource_uid=update_result)
        get_result_2 = TestEventedNDB.get_future_result(future=get_future_2)

        self.assertEqual(get_result_2.example, u'Edited example property', u'Updated property mismatch.')

    def test_patch_async(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_new = TestModel(example=u'This is a test string')

        signal_tester = SignalTester()

        # Create signal refs
        hook_pre_patch = signal(TestEventedNDB.HOOK_PRE_PATCH)
        hook_post_patch = signal(TestEventedNDB.HOOK_POST_PATCH)

        # Subscribe to signals
        hook_pre_patch.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)
        hook_post_patch.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        insert_future = TestEventedNDB.insert_async(resource_object=test_new)
        insert_result = TestEventedNDB.get_future_result(future=insert_future)

        delta_update = {
            'random_property': u'this_is_a_test'
        }

        patch_future = TestEventedNDB.patch_async(resource_uid=insert_result, delta_update=delta_update)
        patch_result = TestEventedNDB.get_future_result(future=patch_future)

        self.assertTrue(isinstance(patch_result, str), u'Update async should result in string instance.')
        self.assertEquals(len(signal_tester.hook_activations[TestEventedNDB]), 2, u'Update should trigger 2 hooks')

        get_future = TestEventedNDB.get_async(resource_uid=insert_result)
        get_result = TestEventedNDB.get_future_result(future=get_future)

        self.assertEqual(get_result.random_property, u'this_is_a_test', u'Updated property mismatch.')

    def test_delete_async(self):
        class TestEventedNDB(koala.NDBEventedInterface):
            _datastore_model = NDBTestModel
            _resource_object = TestModel

        test_new = TestModel(example=u'This is a test string')

        signal_tester = SignalTester()

        # Create signal refs
        hook_pre_delete = signal(TestEventedNDB.HOOK_PRE_DELETE)
        hook_post_delete = signal(TestEventedNDB.HOOK_POST_DELETE)

        # Subscribe to signals
        hook_pre_delete.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)
        hook_post_delete.connect(signal_tester.hook_subscriber, sender=TestEventedNDB)

        # Trigger the datastore class to activate signals which have subscribers
        TestEventedNDB.parse_signal_receivers()

        insert_future = TestEventedNDB.insert_async(resource_object=test_new)
        insert_result = TestEventedNDB.get_future_result(future=insert_future)

        delete_future = TestEventedNDB.delete_async(resource_uid=insert_result)
        # doesn't return anything, but we still need to get the result
        delete_result = TestEventedNDB.get_future_result(future=delete_future)

        get_future = TestEventedNDB.get_async(resource_uid=insert_result)
        get_result = TestEventedNDB.get_future_result(future=get_future)

        self.assertEquals(get_result, None, u'Delete async failed to remove entity.')
        self.assertEquals(len(signal_tester.hook_activations[TestEventedNDB]), 2, u'Delete should trigger 2 hooks')
        self.assertEquals(len(signal_tester.filter_activations), 0, u'Delete should trigger 0 filters')
