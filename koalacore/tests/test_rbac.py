import unittest
import koalacore
from google.appengine.ext import testbed
from blinker import signal

__author__ = 'Matt'


class User(koalacore.Resource):
    permissions = koalacore.ResourceProperty(title=u'Permissions')

    def __init__(self, **kwargs):
        if 'permissions' not in kwargs or kwargs['permissions'] is None:
            kwargs['permissions'] = koalacore.PermissionsStorage()

        super(User, self).__init__(**kwargs)


class TestPermissions(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        # Remaining setup needed for test cases

    def tearDown(self):
        self.testbed.deactivate()

    def test_user_permissions_defaults(self):
        user = User()
        self.assertEqual(user.permissions.roles, set(), u'Roles mismatch')
        self.assertEqual(user.permissions.acl, {}, u'ACL mismatch')
        self.assertEqual(user.permissions._cache, {}, u'Cache mismatch')

    def test_add_role(self):
        user = User()
        user.permissions.add_role(role='test_role')
        self.assertEqual(user.permissions.roles, {'test_role'}, u'Roles mismatch')

    def test_remove_role(self):
        user = User()
        user.permissions.add_role(role='test_role')
        self.assertEqual(user.permissions.roles, {'test_role'}, u'Roles mismatch')
        user.permissions.remove_role(role='test_role')
        self.assertEqual(user.permissions.roles, set(), u'Roles mismatch')

    def test_modify_role_clear_cache(self):
        user = User()
        test_cache = {'test_uid': 'test_value'}
        user.permissions._cache = test_cache
        self.assertEqual(user.permissions._cache, test_cache, u'Cache mismatch')

        user.permissions.add_role(role='test_role')
        self.assertEqual(user.permissions.roles, {'test_role'}, u'Roles mismatch')
        self.assertEqual(user.permissions._cache, {}, u'Cache mismatch')

    def test_set_acl_entry(self):
        user = User()
        user.permissions.set_acl_entry(resource_uid='test_uid', actions_set={'add', 'remove', 'delete'})
        self.assertEqual(user.permissions.acl, {'test_uid': {'add', 'remove', 'delete'}}, u'Roles mismatch')

    def test_remove_acl_entry(self):
        user = User()
        user.permissions.set_acl_entry(resource_uid='test_uid', actions_set={'add', 'remove', 'delete'})
        self.assertEqual(user.permissions.acl, {'test_uid': {'add', 'remove', 'delete'}}, u'Roles mismatch')

        user.permissions.remove_acl_entry(resource_uid='test_uid')
        self.assertEqual(user.permissions.acl, {}, u'Roles mismatch')

    def test_modify_acl_clear_cache(self):
        user = User()
        test_cache = {'test_uid': 'test_value'}
        user.permissions._cache = test_cache
        self.assertEqual(user.permissions._cache, test_cache, u'Cache mismatch')

        user.permissions.set_acl_entry(resource_uid='test_uid', actions_set={'add', 'remove', 'delete'})
        self.assertEqual(user.permissions.acl, {'test_uid': {'add', 'remove', 'delete'}}, u'Roles mismatch')
        self.assertEqual(user.permissions._cache, {}, u'Cache mismatch')


class TestRBAC(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        # Remaining setup needed for test cases
        global_acl = {
            'sysadmin': {'delete_user', 'delete_company'},
            'admin': {'update_user_password', 'update_company'},
        }
        self.rbac = koalacore.RBAC
        self.rbac.configure(global_acl=global_acl)

    def tearDown(self):
        self.testbed.deactivate()

    def test_user_is(self):
        user = User()
        user.permissions.add_role(role='admin')
        self.assertTrue(self.rbac.user_is(user=user, role='admin'), u'Roles mismatch')
        self.assertFalse(self.rbac.user_is(user=user, role='sysadmin'), u'Roles mismatch')

    def test_user_can_global_acl(self):
        user = User()
        user.permissions.add_role(role='admin')
        self.assertTrue(self.rbac.user_can(user=user, action='update_user_password'), u'Permission mismatch')
        self.assertTrue(self.rbac.user_can(user=user, action='update_company'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_user'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_company'), u'Permission mismatch')

    def test_user_can_local_acl(self):
        user = User()
        user.permissions.set_acl_entry(resource_uid='test_uid', actions_set={'add', 'remove', 'delete'})
        self.assertTrue(self.rbac.user_can(user=user, action='add', resource_uid='test_uid'), u'Permission mismatch')
        self.assertTrue(self.rbac.user_can(user=user, action='remove', resource_uid='test_uid'), u'Permission mismatch')
        self.assertTrue(self.rbac.user_can(user=user, action='delete', resource_uid='test_uid'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='add'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='remove'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='update_user_password'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='update_company'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_user'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_company'), u'Permission mismatch')

    def test_user_can_local_acl_self(self):
        user = User(uid='test_uid')
        user.permissions.set_acl_entry(resource_uid='self', actions_set={'change_password'})
        self.assertTrue(self.rbac.user_can(user=user, action='change_password', resource_uid='test_uid'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='change_password'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='add', resource_uid='test_uid'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='remove', resource_uid='test_uid'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete', resource_uid='test_uid'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='add'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='remove'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='update_user_password'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='update_company'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_user'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_company'), u'Permission mismatch')


class TestRBACSignals(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # Next, declare which service stubs you want to use.
        # Remaining setup needed for test cases
        global_acl = {
            'sysadmin': {'delete_user', 'delete_company'},
            'admin': {'update_user_password', 'update_company'},
        }
        self.rbac = koalacore.RBAC
        self.rbac.configure(global_acl=global_acl)

    def tearDown(self):
        self.testbed.deactivate()

    def _permission_denied(self, sender, **kwargs):
        raise koalacore.PermissionDenied('Test permission denied')

    def test_user_can_signal(self):
        user = User()
        user.permissions.add_role(role='admin')
        signal('user_can').connect(self._permission_denied, sender=self.rbac)
        # As the signal is connected to the method and automatically raises PermissionDenied, everything should fail
        self.assertFalse(self.rbac.user_can(user=user, action='update_user_password'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='update_company'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_user'), u'Permission mismatch')
        self.assertFalse(self.rbac.user_can(user=user, action='delete_company'), u'Permission mismatch')
