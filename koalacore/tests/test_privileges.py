# -*- coding: utf-8 -*-
"""
    koala.test_privileges
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""

__author__ = 'Matt Badger'

import unittest
import koala


READ = 'read'
WRITE = 'write'
DELETE = 'delete'
PASSWD = 'passwd'
LIST = 'list'
QUERY = 'query'


SUPPORTED_ACTIONS = {
    koala.PrivilegeConstants.APPLY_TO_RESOURCE: [
        READ,
        WRITE,
        DELETE,
        PASSWD,
    ],
    koala.PrivilegeConstants.APPLY_TO_RESOURCE_TYPE: [
        LIST,
        QUERY,
    ],
}


IMPLEMENTED_RESOURCE_ACTIONS = {
    koala.PrivilegeConstants.APPLY_ALL: [READ, WRITE, DELETE, PASSWD],
}


TEST_PRIVILEGE_SET = {
    koala.Privilege(action=PASSWD,
                    role=koala.PrivilegeConstants.PRIVILEGE_ROLE_SELF,
                    who=0,
                    privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_OBJECT,
                    related_id=0),
    koala.Privilege(action=PASSWD,
                    role=koala.PrivilegeConstants.PRIVILEGE_ROLE_SYSTEM_GROUP,
                    who=koala.PrivilegeConstants.SYSTEM_GROUP_ADMIN,
                    privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_GLOBAL,
                    related_id=0),
    koala.Privilege(action=LIST,
                    role=koala.PrivilegeConstants.PRIVILEGE_ROLE_SYSTEM_GROUP,
                    who=koala.PrivilegeConstants.SYSTEM_GROUP_ADMIN,
                    privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_RESOURCE_TYPE,
                    related_id=0),
    koala.Privilege(action=READ,
                    role=koala.PrivilegeConstants.PRIVILEGE_ROLE_SYSTEM_GROUP,
                    who=koala.PrivilegeConstants.SYSTEM_GROUP_ADMIN,
                    privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_GLOBAL,
                    related_id=0),
}

koala.AugmentedPrivilegeEvaluator.register_supported_actions(namespace='User', supported_actions=SUPPORTED_ACTIONS)
koala.AugmentedPrivilegeEvaluator.register_implemented_actions(namespace='User',
                                                               implemented_actions=IMPLEMENTED_RESOURCE_ACTIONS)
koala.AugmentedPrivilegeEvaluator.register_privileges(namespace='User', privileges=TEST_PRIVILEGE_SET)

PrivilegeEvaluator = koala.AugmentedPrivilegeEvaluator

class TestPrivileges(unittest.TestCase):
    """
    Test the global privilege system
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_system_user_privs(self):
        expected_actions = [READ, WRITE, DELETE, PASSWD, LIST, QUERY]

        sys_user_dict = {'uid': 0, 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_SYSTEM}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch.')

    def test_root_user_privs(self):
        expected_actions = [READ, WRITE, DELETE, PASSWD, LIST, QUERY]

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_ROOT}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch.')

    def test_admin_user_privs(self):
        expected_actions = [READ, PASSWD, LIST]

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_ADMIN}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

    def test_user_privs(self):
        # user should not be granted any privileges by default

        expected_actions = []

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

    def test_user_self_privs(self):
        # user should not be granted any privileges by default

        expected_actions = [PASSWD]

        sys_user_dict = {'uid': 'mhcfhxdgfssjgfk', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER}
        entity_sec_dict = {'uid': 'mhcfhxdgfssjgfk', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

    def test_augmented_user_privs_attribute_setting(self):
        # user should not be granted any privileges by default, but the augmented privs should grant specific privileges
        # Need to test an entity level and entity_type level priv

        expected_actions = [LIST, WRITE]

        augmented_privileges = {
            koala.Privilege(action=LIST,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_RESOURCE_TYPE,
                            related_id=0),
            koala.Privilege(action=WRITE,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_OBJECT,
                            related_id='apdmbdfobninrounbsodibn')
        }

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER, 'augmented_privileges': {'User': augmented_privileges}}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

    def test_augmented_user_privs_invalid(self):
        # WRITE priv is defined but incorrectly; only LIST should be granted

        expected_actions = [LIST]

        augmented_privileges = {
            koala.Privilege(action=LIST,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_RESOURCE_TYPE,
                            related_id=0),
            koala.Privilege(action=WRITE,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_OBJECT,
                            related_id=0)
        }

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER, 'augmented_privileges': {'User': augmented_privileges}}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

    def test_augmented_user_privs_method_setting(self):
        # user should not be granted any privileges by default, but the augmented privs should grant specific privileges
        # Need to test an entity level and entity_type level priv

        expected_actions = [LIST, WRITE]

        augmented_privileges = {
            koala.Privilege(action=LIST,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_RESOURCE_TYPE,
                            related_id=0),
            koala.Privilege(action=WRITE,
                            role=koala.PrivilegeConstants.PRIVILEGE_ROLE_USER,
                            who='sdkjgnsdgjnasgl',
                            privilege_type=koala.PrivilegeConstants.PRIVILEGE_TYPE_OBJECT,
                            related_id='apdmbdfobninrounbsodibn')
        }

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        sys_user.add_augmented_privileges(namespace='User', privileges=augmented_privileges)

        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        actions = PrivilegeEvaluator.get_resource_privileges(credentials=sys_user, resource=entity_sec_ob)
        self.assertEqual(set(actions), set(expected_actions), 'Action list mistmatch - expected: {0} | got: {1}'.format(expected_actions, actions))

        sys_user.remove_augmented_privileges(namespace='User', privileges=augmented_privileges)
        self.assertEqual(sys_user.augmented_privileges, {'User': set()}, 'Augmented privilege mismatch')

    def test_authorise_decorator_as_admin(self):
        expected_actions = [READ, PASSWD, LIST]

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_ADMIN}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        @koala.authorise(action=koala.PrivilegeConstants.READ)
        def test_func(credentials, resource, **kargs):
            return True

        returned = test_func(credentials=sys_user, resource=entity_sec_ob)
        self.assertTrue(returned, 'Decorated function should return True')

    def test_authorise_decorator_as_user(self):
        expected_actions = []

        sys_user_dict = {'uid': 'sdkjgnsdgjnasgl', 'resource_type': 'User', 'system_groups': koala.PrivilegeConstants.SYSTEM_GROUP_USER}
        entity_sec_dict = {'uid': 'apdmbdfobninrounbsodibn', 'resource_type': 'User'}

        sys_user = koala.AugmentedSecurityObject(**sys_user_dict)
        entity_sec_ob = koala.AugmentedSecurityObject(**entity_sec_dict)

        @koala.authorise(action=koala.PrivilegeConstants.READ)
        def test_func(credentials, resource, **kargs):
            return True

        with self.assertRaises(koala.UnauthorisedCredentials):
            test_func(credentials=sys_user, resource=entity_sec_ob)
