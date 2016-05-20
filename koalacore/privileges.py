# -*- coding: utf-8 -*-
"""
    .privileges
    ~~~~~~~~~~~~~~~~~~

    Standalone privilege evaluation system. Using predefined object base classes, add unix style permissions and
    security meta. Also provides a system for defining system privileges on object types for further access control.
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
import collections
from .tools import random_string

__author__ = 'Matt Badger'

APPLY_ALL = 0

SYSTEM_GROUP_SYSTEM = 1
SYSTEM_GROUP_ROOT = 2
SYSTEM_GROUP_ADMIN = 4
SYSTEM_GROUP_SUPERVISOR = 8
SYSTEM_GROUP_USER = 16
SYSTEM_GROUP_ENTITY = 32

STATUS_DELETED = 1
STATUS_MOCKED = 2
STATUS_INACTIVE = 4
STATUS_ACTIVE = 8

READ = 'read'
WRITE = 'write'
DELETE = 'delete'
GRANT = 'grant'
LIST = 'list'
QUERY = 'query'

APPLY_TO_RESOURCE = 'apply_to_resource'
APPLY_TO_RESOURCE_TYPE = 'apply_to_resource_type'
APPLY_TO_ALL_RESOURCES_OF_TYPE = 'apply_to_all_resources_of_type'

SYSTEM_USER_ID = 'systemuser'

PrivilegeConstants = collections.namedtuple('PrivilegeConstants', [
    'APPLY_ALL',
    'UNIX_OWNER_READ',
    'UNIX_OWNER_WRITE',
    'UNIX_OWNER_DELETE',
    'UNIX_GROUP_READ',
    'UNIX_GROUP_WRITE',
    'UNIX_GROUP_DELETE',
    'UNIX_OTHER_READ',
    'UNIX_OTHER_WRITE',
    'UNIX_OTHER_DELETE',
    'DEFAULT_UNIX_PERMISSIONS',
    'SYSTEM_GROUP_SYSTEM',
    'SYSTEM_GROUP_ROOT',
    'SYSTEM_GROUP_ADMIN',
    'SYSTEM_GROUP_SUPERVISOR',
    'SYSTEM_GROUP_USER',
    'SYSTEM_GROUP_ENTITY',
    'SYSTEM_USER_GROUPS',
    'STATUS_DELETED',
    'STATUS_MOCKED',
    'STATUS_INACTIVE',
    'STATUS_ACTIVE',
    'SUPPORTED_STATUSES',
    'APPLY_TO_RESOURCE',
    'APPLY_TO_RESOURCE_TYPE',
    'APPLY_TO_ALL_RESOURCES_OF_TYPE',
    'SUPPORTED_SCOPES',
    'READ',
    'WRITE',
    'DELETE',
    'GRANT',
    'LIST',
    'QUERY',
    'UNIX_ACTIONS',
    'DEFAULT_SUPPORTED_ACTIONS',
    'DEFAULT_IMPLEMENTED_RESOURCE_ACTIONS',
    'PRIVILEGE_ROLE_SELF',
    'PRIVILEGE_ROLE_USER',
    'PRIVILEGE_ROLE_OWNER',
    'PRIVILEGE_ROLE_OWNER_GROUP',
    'PRIVILEGE_ROLE_SYSTEM_GROUP',
    'PRIVILEGE_ROLE_MEMBERSHIP_GROUP',
    'PRIVILEGE_TYPE_OBJECT',
    'PRIVILEGE_TYPE_GLOBAL',
    'PRIVILEGE_TYPE_RESOURCE_TYPE',
    'PRIVILEGE_NONE_VALUE',
    'SYSTEM_USER_ID',
])(
    APPLY_ALL=APPLY_ALL,
    UNIX_OWNER_READ=256,
    UNIX_OWNER_WRITE=128,
    UNIX_OWNER_DELETE=64,
    UNIX_GROUP_READ=32,
    UNIX_GROUP_WRITE=16,
    UNIX_GROUP_DELETE=8,
    UNIX_OTHER_READ=4,
    UNIX_OTHER_WRITE=2,
    UNIX_OTHER_DELETE=1,
    DEFAULT_UNIX_PERMISSIONS=0700,
    SYSTEM_GROUP_SYSTEM=SYSTEM_GROUP_SYSTEM,
    SYSTEM_GROUP_ROOT=SYSTEM_GROUP_ROOT,
    SYSTEM_GROUP_ADMIN=SYSTEM_GROUP_ADMIN,
    SYSTEM_GROUP_SUPERVISOR=SYSTEM_GROUP_SUPERVISOR,
    SYSTEM_GROUP_USER=SYSTEM_GROUP_USER,
    SYSTEM_GROUP_ENTITY=SYSTEM_GROUP_ENTITY,
    SYSTEM_USER_GROUPS={
        "system":        SYSTEM_GROUP_SYSTEM,
        "root":          SYSTEM_GROUP_ROOT,
        "admin":         SYSTEM_GROUP_ADMIN,
        "supervisor":    SYSTEM_GROUP_SUPERVISOR,
        "user":          SYSTEM_GROUP_USER,
        "entity":          SYSTEM_GROUP_ENTITY,
    },
    STATUS_DELETED=STATUS_DELETED,
    STATUS_MOCKED=STATUS_MOCKED,
    STATUS_INACTIVE=STATUS_INACTIVE,
    STATUS_ACTIVE=STATUS_ACTIVE,
    SUPPORTED_STATUSES={
        "deleted":       STATUS_DELETED,
        "mocked":        STATUS_MOCKED,
        "inactive":      STATUS_INACTIVE,
        "active":        STATUS_ACTIVE,
    },
    APPLY_TO_RESOURCE=APPLY_TO_RESOURCE,
    APPLY_TO_RESOURCE_TYPE=APPLY_TO_RESOURCE_TYPE,
    APPLY_TO_ALL_RESOURCES_OF_TYPE=APPLY_TO_ALL_RESOURCES_OF_TYPE,
    SUPPORTED_SCOPES=[APPLY_TO_RESOURCE, APPLY_TO_RESOURCE_TYPE, APPLY_TO_ALL_RESOURCES_OF_TYPE],
    READ=READ,
    WRITE=WRITE,
    DELETE=DELETE,
    GRANT=GRANT,
    LIST=LIST,
    QUERY=QUERY,
    UNIX_ACTIONS=[READ, WRITE, DELETE],
    DEFAULT_SUPPORTED_ACTIONS={
        APPLY_TO_RESOURCE: [
            READ,
            WRITE,
            DELETE,
        ],
        APPLY_TO_RESOURCE_TYPE: [
            LIST,
            QUERY,
        ],
    },
    DEFAULT_IMPLEMENTED_RESOURCE_ACTIONS={
        APPLY_ALL: [READ, WRITE, DELETE],
    },
    PRIVILEGE_ROLE_SELF='self',
    PRIVILEGE_ROLE_USER='user',
    PRIVILEGE_ROLE_OWNER='owner',
    PRIVILEGE_ROLE_OWNER_GROUP='owner_group',
    PRIVILEGE_ROLE_SYSTEM_GROUP='group',
    PRIVILEGE_ROLE_MEMBERSHIP_GROUP='membership_group',
    PRIVILEGE_TYPE_OBJECT='object',
    PRIVILEGE_TYPE_GLOBAL='global',
    PRIVILEGE_TYPE_RESOURCE_TYPE='resource_type',
    PRIVILEGE_NONE_VALUE=0,
    SYSTEM_USER_ID=SYSTEM_USER_ID,
)

"""
Privilege object. Used to define resource privileges.

- Action should match those registered as supported/implemented actions in the privilege evaluator
- Role can be: user, system_group, self, owner_group. Basically 'applies to'.
- Who can be: the id of the role that the priv applies to. 0 can be used for self
- Type can be: resource, global, resource_type. Basically the scope of the privilege. 'resource' applies to a single
    resource (and related_uid should be set), 'resource_type' applies to operations on an resource type (e.g. 'create'
    cannot be applied to an resource, because a resource has to exist for a privilege to apply to it. 'global' applies
    to all resources of a given type (e.g. admin users may have 'read' privileges on all 'User' resources).
- RelatedID can be: the id of the resource that the priv applies to. Mostly this will be 0 unless setting user level
  privileges.

"""
Privilege = collections.namedtuple('Privilege', 'action role who privilege_type related_uid')


class SecurityObject(object):
    uid = None
    # created = None
    # modified = None
    # modified_by = None
    resource_type = None
    status = PrivilegeConstants.STATUS_ACTIVE
    # Unix style permissions (these apply to the entity itself)
    owner = SYSTEM_USER_ID
    author = SYSTEM_USER_ID
    owner_group = PrivilegeConstants.SYSTEM_GROUP_SYSTEM  # system group which owns object
    unix_perms = PrivilegeConstants.DEFAULT_UNIX_PERMISSIONS  # bitmask
    system_groups = PrivilegeConstants.SYSTEM_GROUP_USER  # system group(s) which object belongs to (bitmask)

    def __init__(self, **entries):
        self.__dict__.update(entries)


class BasePrivilegeEval(object):
    """
    Base privilege evaluator class. Most of the functionality is already defined, but actions such as getting a list of
    privileges, evaluating privileges are left to implementors. Look at AugmentedPrivilegeEvaluator below for an
    example implementation which supports run time augmentation of privileges for a given resource.

    Class attributes with specific types:

    _supported_actions | type: dict of lists | default: {}
    _implemented_entity_actions | type: dict of lists | default: {}
    _privilege_definitions | type: set | default: set()

    """
    _constants = PrivilegeConstants
    _credentials_definition = SecurityObject
    _priv_cache_key = 'priv_cache'
    _supported_resource_actions = {}
    _implemented_resource_actions = {}
    _privilege_definitions = {}

    @classmethod
    def _eval_unix_permissions(cls, credentials, resource):
        unix_perms = []

        if resource.unix_perms & cls._constants.UNIX_OTHER_READ:
            unix_perms.append(cls._constants.READ)
        elif ((resource.unix_perms & cls._constants.UNIX_OWNER_READ) and
              (resource.owner == credentials.uid)):
            unix_perms.append(cls._constants.READ)
        elif ((resource.unix_perms & cls._constants.UNIX_GROUP_READ) and
              (resource.owner_group & credentials.user_system_groups)):
            unix_perms.append(cls._constants.READ)

        if resource.unix_perms & cls._constants.UNIX_OTHER_WRITE:
            unix_perms.append(cls._constants.WRITE)
        elif ((resource.unix_perms & cls._constants.UNIX_OWNER_WRITE) and
              (resource.owner == credentials.uid)):
            unix_perms.append(cls._constants.WRITE)
        elif ((resource.unix_perms & cls._constants.UNIX_GROUP_WRITE) and
              (resource.owner_group & credentials.user_system_groups)):
            unix_perms.append(cls._constants.WRITE)

        if resource.unix_perms & cls._constants.UNIX_OTHER_DELETE:
            unix_perms.append(cls._constants.DELETE)
        elif ((resource.unix_perms & cls._constants.UNIX_OWNER_DELETE) and
              (resource.owner == credentials.uid)):
            unix_perms.append(cls._constants.DELETE)
        elif ((resource.unix_perms & cls._constants.UNIX_GROUP_DELETE) and
              (resource.owner_group & credentials.user_system_groups)):
            unix_perms.append(cls._constants.DELETE)

        return unix_perms

    @classmethod
    def _filter_credential_authorized_actions(cls, credentials, resource, valid_actions, privilege_set):
        if not valid_actions:
            # Maybe raise error? There could be situations where no actions are available so leave for now
            return []

        if credentials.system_groups & cls._constants.SYSTEM_GROUP_SYSTEM:
            # TODO: log everything that happens from here

            # No further filtering is necessary; system can perform all actions (that are valid for the status)
            return valid_actions
        elif credentials.system_groups & cls._constants.SYSTEM_GROUP_ROOT:
            # TODO further checking to make doubly sure that root permissions should be active

            # No further filtering is necessary; root can perform all actions (that are valid for the status)
            return valid_actions
        else:
            granted_privs = []

            granted_privs += cls._eval_unix_permissions(credentials=credentials, resource=resource)

            # No point re-evaluating privileges if the action is already granted by the unix style permissions
            remaining_actions = set(valid_actions) - set(granted_privs)

            for privilege_def in privilege_set:
                if privilege_def.action in remaining_actions:
                    if cls._credentials_allowed_priv(priv=privilege_def, credentials=credentials, resource=resource):
                        granted_privs.append(privilege_def.action)

            return granted_privs

    @staticmethod
    def _credentials_allowed_priv(priv, credentials, resource):
        """
        Used to evaluate if the given credentials should be allowed to perform a specified action.
        :param priv:
        :param credentials:
        :param resource:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def _get_valid_actions(cls, namespace, resource):
        """
        Return a list of valid actions that can be performed on the resource_type when it is in the given status.
        :param resource:
        :raise NotImplementedError:
        :returns list of valid actions
        """
        raise NotImplementedError

    @classmethod
    def _get_resource_privileges(cls, namespace, credentials, resource):
        """
        Return a set of defined privileges for the resource
        :param credentials:
        :param resource:
        :raise NotImplementedError:
        :returns set of Privilege Definitions
        """
        raise NotImplementedError

    @classmethod
    def _get_supported_actions_for_scope(cls, namespace, scope=None):
        """
        Filter the supported actions attribute based on scope, or return all supported actions if no scope is passed.
        :param scope:
        :returns list of supported actions for scope
        """
        if not scope:
            resource_actions = cls._supported_resource_actions.get(namespace, {}).get(cls._constants.APPLY_TO_RESOURCE,
                                                                                      [])
            resource_type_actions = cls._supported_resource_actions.get(namespace, {}).get(
                cls._constants.APPLY_TO_RESOURCE_TYPE, [])

            actions = resource_actions + resource_type_actions
        else:
            actions = cls._supported_resource_actions.get(namespace, {}).get(scope, [])

        return actions

    @classmethod
    def _get_implemented_resource_actions(cls, namespace, resource):
        """
        Get implemented actions for a resource based on its status.

        We don't need to check the actions in the scope because 'implemented_actions' only applies to resources
        Maybe there should be a check to make sure that actions in 'implemented_actions' actually match the 'actions'
        dict for the given scope, but it seems unnecessary at this point.

        :param resource:
        :returns list of implemented actions
        """

        universal_actions = cls._implemented_resource_actions.get(namespace, {}).get(cls._constants.APPLY_ALL, [])
        status_specific_actions = cls._implemented_resource_actions.get(namespace, {}).get(resource.status, [])

        return universal_actions + status_specific_actions

    @classmethod
    def _get_authorised_user_actions(cls, namespace, credentials, resource):
        if resource and not isinstance(resource, cls._credentials_definition):
            raise ValueError(u'Resource must be of type SecurityObject.')
        elif not isinstance(credentials, cls._credentials_definition):
            raise ValueError(u'Credentials must be of type SecurityObject.')

        if not resource.resource_type == namespace:
            raise ValueError(u'Resource type mismatch; expected: \'{}\', got: \'{}\''.format(namespace,
                                                                                             resource.resource_type))

        if not hasattr(credentials, cls._priv_cache_key):
            setattr(credentials, cls._priv_cache_key, {})
            cache = None
        else:
            cache = credentials.priv_cache.get(resource.uid)

        if cache:
            return cache
        else:
            valid_actions = cls._get_valid_actions(namespace=namespace, resource=resource)
            privilege_set = cls._get_resource_privileges(namespace=namespace,
                                                         credentials=credentials,
                                                         resource=resource)
            permissible_actions = cls._filter_credential_authorized_actions(credentials=credentials,
                                                                            resource=resource,
                                                                            valid_actions=valid_actions,
                                                                            privilege_set=privilege_set)
            # TODO: modify this to use an md5 hash of the sec_ob instead of the id
            # That way it is automatically invalid if the sec_ob changes and it avoids a situation where
            # there are multiple sec_obs with the same id
            credentials.priv_cache[resource.uid] = permissible_actions
            return permissible_actions

    @classmethod
    def register_supported_actions(cls, namespace, supported_actions):
        """
        Allows the registration of supported actions for a given namespace. This simply performs a dict.update() call.
        Care should be taken when using this method - it performs no format or duplicate checking.

        Format should be as follows:

            DEFAULT_SUPPORTED_ACTIONS={
                APPLY_TO_ENTITY: [
                    READ,
                    WRITE,
                    DELETE,
                ],
                APPLY_TO_RESOURCE: [
                    LIST,
                    QUERY,
                ],
            },

        This default definition is in the constants defined at the top of this file. APPLY_TO_ENTITY and
        APPLY_TO_RESOURCE_TYPE are constant values, also defined above. The actions in the lists are defined as constants
        here for simplicity, but they can be any string that you desire.

        :param namespace (str):
        :param supported_actions (dict of lists):
        :raises ValueError:
        """

        if not isinstance(supported_actions, dict):
            raise ValueError(u'Supported actions definitions must be in a dict')

        if cls._supported_resource_actions.get(namespace, None) is None:
            cls._supported_resource_actions[namespace] = supported_actions
        else:
            cls._supported_resource_actions[namespace].update(supported_actions)

    @classmethod
    def register_implemented_actions(cls, namespace, implemented_actions):
        """
        Allows the registration of implemented actions for a given namespace. This simply performs a dict.update() call.
        Care should be taken when using this method - it performs no format or duplicate checking.

        Format should be as follows:

            DEFAULT_IMPLEMENTED_ENTITY_ACTIONS={
                APPLY_ALL: [READ, WRITE, DELETE],
            },

        This default definition is in the constants defined at the top of this file. APPLY_ALL is a constant values,
        also defined above. The actions in the lists are defined as constants here for simplicity, but they can be any
        string that you desire.

        The dict keys refer the resource status and the value for each key is a list of supported actions for that
        status.

        :param namespace (str):
        :param implemented_actions (dict of lists):
        :raises ValueError:
        """

        if not isinstance(implemented_actions, dict):
            raise ValueError(u'Implemented actions definitions must be in a dict')

        if cls._implemented_resource_actions.get(namespace, None) is None:
            cls._implemented_resource_actions[namespace] = implemented_actions
        else:
            cls._implemented_resource_actions[namespace].update(implemented_actions)

    @classmethod
    def register_privileges(cls, namespace, privileges):
        """
        Allows the registration of privilege sets into a given namespace. This is useful for defining privileges on
        different kinds of resource e.g. users or companies.

        :param namespace (str):
        :param privileges (set of privilege objects):
        :raises ValueError:
        """

        if not isinstance(privileges, set):
            raise ValueError(u'Privilege definitions must be in a set')

        if cls._privilege_definitions.get(namespace, None) is None:
            cls._privilege_definitions[namespace] = privileges
        else:
            cls._privilege_definitions[namespace] = cls._privilege_definitions[namespace] | privileges

    @classmethod
    def get_resource_privileges(cls, credentials, resource, namespace=None):
        """
        For a given namespace, credential object and resource object, return a list of authorised actions.
        If resource is not specified then the resource type is used. This is what external code should call to get
        a list of valid actions.

        :param credentials (SecurityObject):
        :param resource (SecurityObject):
        :param namespace (str):
        :returns action list (list of str):
        """
        if namespace is None:
            namespace = resource.resource_type

        return cls._get_authorised_user_actions(namespace=namespace, credentials=credentials, resource=resource)


class AugmentedSecurityObject(SecurityObject):
    augmented_privileges = {}  # Similar to access_control_list but ephemeral and not persisted

    def add_augmented_privilege(self, namespace, privilege):
        if not isinstance(privilege, Privilege):
            raise ValueError(u'Privilege definitions must be of type \'Privilege\'')

        if self.augmented_privileges.get(namespace, None) is None:
            self.augmented_privileges[namespace] = set()

        self.augmented_privileges[namespace].add(privilege)

    def remove_augmented_privilege(self, namespace, privilege):
        if not isinstance(privilege, Privilege):
            raise ValueError(u'Privilege definitions must be of type \'Privilege\'')

        if isinstance(self.augmented_privileges.get(namespace, None), set):
            self.augmented_privileges[namespace].discard(privilege)

    def grant_augmented_privileges(self, namespace, privileges):
        if not isinstance(privileges, set):
            raise ValueError(u'Privilege definitions must be in a set')

        if self.augmented_privileges.get(namespace, None) is None:
            self.augmented_privileges[namespace] = privileges
        else:
            self.augmented_privileges[namespace] = self.augmented_privileges[namespace] | privileges

    def revoke_augmented_privileges(self, namespace, privileges):
        if not isinstance(privileges, set):
            raise ValueError(u'Privilege definitions must be in a set')

        if isinstance(self.augmented_privileges.get(namespace, None), set):
            self.augmented_privileges[namespace] = self.augmented_privileges[namespace] - privileges

    def get_augmented_privileges(self, namespace):
        return self.augmented_privileges.get(namespace, set())

    def set_augmented_privileges(self, privilege_dict):
        self.augmented_privileges = privilege_dict


class AugmentedPrivilegeEvaluator(BasePrivilegeEval):
    _credentials_definition = AugmentedSecurityObject

    @classmethod
    def _get_valid_actions(cls, namespace, resource):
        supported_resource_level = cls._get_supported_actions_for_scope(namespace=namespace,
                                                                        scope=cls._constants.APPLY_TO_RESOURCE)
        supported_resource_type_level = cls._get_supported_actions_for_scope(namespace=namespace,
                                                                             scope=cls._constants.APPLY_TO_RESOURCE_TYPE)
        implemented_resource_level = cls._get_implemented_resource_actions(namespace=namespace, resource=resource)

        # Filter for actions that are both supported and implemented
        implemented = set(supported_resource_level) & set(implemented_resource_level)

        return list(implemented) + supported_resource_type_level

    @classmethod
    def _get_resource_privileges(cls, namespace, credentials, resource):
        credential_privileges = set()
        resource_privileges = set()

        if credentials.augmented_privileges:
            user_augmented_privileges = credentials.get_augmented_privileges(namespace=credentials.resource_type)
            if user_augmented_privileges is not None:
                # we are assuming that the value is a set. If not, the return statement will throw an exception
                credential_privileges = user_augmented_privileges

        if resource.augmented_privileges:
            # Generally a resource would only ever augment privileges on itself, but this keeps the attribute access
            # unified.
            resource_augmented_privileges = resource.get_augmented_privileges(namespace=resource.resource_type)
            if resource_augmented_privileges is not None:
                # we are assuming that the value is a set. If not, the return statement will throw an exception
                resource_privileges = resource_augmented_privileges

        return cls._privilege_definitions.get(namespace, set()) | credential_privileges | resource_privileges

    @staticmethod
    def _credentials_allowed_priv(priv, credentials, resource):
        type_check_passed = False

        if ((priv.privilege_type == PrivilegeConstants.PRIVILEGE_TYPE_OBJECT and priv.related_uid == resource.uid) or
                priv.privilege_type == PrivilegeConstants.PRIVILEGE_TYPE_RESOURCE_TYPE or
                priv.privilege_type == PrivilegeConstants.PRIVILEGE_TYPE_GLOBAL):
            type_check_passed = True

        if not type_check_passed and priv.role == 'self':
            if credentials.uid == resource.uid:
                return True
        elif not type_check_passed:
            return False
        else:
            if priv.role == 'user' and priv.who == credentials.uid:
                return True
            elif priv.role == 'owner' and resource.owner == credentials.uid:
                return True
            elif priv.role == 'owner_group' and (resource.owner_group & credentials.user_system_groups):
                return True
            # Deprecated
            # elif priv.role == 'member_group' and (resource.member_group in credentials.membership_groups):
            #     return True
            elif priv.role == 'group' and (int(priv.who) & credentials.system_groups):
                return True


def get_system_group_list_from_bitmask(bitmask, capitalize=True):
    """
    Returns a list of system group names that the given credentials are allowed to change.
    :param bitmask:
    :param capitalize:
    :return list of system group names:
    """
    groups = []
    for group_name, group_bitmask in PrivilegeConstants.SYSTEM_USER_GROUPS.iteritems():
        if group_bitmask & bitmask:
            if capitalize:
                groups.append(group_name.capitalize())
            else:
                groups.append(group_name)
    return groups


def get_credential_group_list_from_bitmask(credentials, capitalize=False):
    """
    Returns a tuple of system groups that the given credentials are allowed to change
    :param credentials:
    :param capitalize:
    :return tuple of value=>name pairs:
    """
    groups = []
    for group_name, group_bitmask in PrivilegeConstants.SYSTEM_USER_GROUPS.iteritems():
        if ((group_bitmask & credentials.system_groups or credentials.system_groups & PrivilegeConstants.SYSTEM_GROUP_ROOT) and
                group_bitmask != PrivilegeConstants.SYSTEM_GROUP_SYSTEM and
                group_bitmask != PrivilegeConstants.SYSTEM_GROUP_ROOT and
                group_bitmask != PrivilegeConstants.SYSTEM_GROUP_USER and
                group_bitmask != PrivilegeConstants.SYSTEM_GROUP_SUPERVISOR):
            if capitalize:
                groups.append(group_name.capitalize())
            else:
                groups.append(group_name)
    return groups


def update_credential_system_groups(credentials, role_dict):
    """
    Takes a dict of role names and boolean values and updates the given credentials
    :param credentials:
    :param role_dict:
    :return tuple of value=>name pairs:
    :raises KeyError:
    """
    for role_name, active in role_dict.iteritems():
        role_name = role_name.lower()
        if role_name != 'system' and role_name != 'root' and role_name != 'user':
            if active:
                credentials.system_groups |= PrivilegeConstants.SYSTEM_USER_GROUPS[role_name]
            else:
                credentials.system_groups &= ~PrivilegeConstants.SYSTEM_USER_GROUPS[role_name]


DEFAULT_EVALUATOR = AugmentedPrivilegeEvaluator


def mock_credentials(resource_type, mock_type=SecurityObject, config=None):
    """
    Creates a new mock security object which can be used for privilege checking. The type of mocked Security Object can
    be specified if you don't want to use the default 'SecurityObject' mock.

    Regardless of the supplied config, the resource_type, uid and status will be overridden.

    :param resource_type:
    :param mock_type (class which inherits from SecurityObject):
    :param config (optional dict of property=> value pairs to set on the security object):
    :return:
    """

    if config:
        mock = mock_type(**config)
    else:
        mock = mock_type()
    mock.resource_type = resource_type
    random_token = random_string()
    mock.uid = 'Mocked{0}_{1}'.format(mock.resource_type, random_token)
    mock.status = PrivilegeConstants.STATUS_MOCKED
    return mock


def user_can(credentials, resource, action, evaluator=DEFAULT_EVALUATOR):
    """
    Gets a list of valid actions from the privilege evaluator. If the specified action is in the returned list then
    return True, else False.

    :param credentials:
    :param resource:
    :param action:
    :param evaluator:
    :return:
    """
    permissible_actions = evaluator.get_resource_privileges(credentials=credentials, resource=resource)
    return action in permissible_actions


class UnauthorisedCredentials(Exception):
    """
    User set but does not have permission to perform the requested action
    """
    pass


def authorise(action, resource_arg_name='resource', credentials_arg_name='credentials',
              resource_security_object_attribute=None, credentials_security_object_attribute=None):
    """
        Decorator to invoke a calls to a privilege evaluator in order to authorise a set of credentials to perform
        an action on a given resource.

        If the credential object does not have the required privileges on a given resource then UnauthorisedCredentials
        will be raised.

        As the decorator will not know ahead of time which of the function arguments should be evaluated, you can
        explicitly specify which kwargs to use for the credentials and resource objects (resource_arg_name,
        credentials_arg_name).

        It is very common that the function arguments will be objects which contain the necessary resource/credential
        objects. In these cases, you may specify the relevant attribute to use instead of the raw arg
        (resource_security_object_attribute, credentials_security_object_attribute).

        :param action (str):
        :param resource_arg_name (str):
        :param credentials_arg_name (str):
        :param resource_security_object_attribute (str):
        :param credentials_security_object_attribute (str):
        :raises UnauthorisedCredentials:
    """

    def privilege_required(fnc):

        def check_permissions(*args, **kwargs):
            resource = kwargs[resource_arg_name]
            credentials = kwargs[credentials_arg_name]

            if resource_security_object_attribute is not None:
                resource = getattr(resource, resource_security_object_attribute)

            if credentials_security_object_attribute is not None:
                credentials = getattr(credentials, credentials_security_object_attribute)

            if not user_can(credentials=credentials, resource=resource, action=action):
                raise UnauthorisedCredentials(
                    u'Credentials are not authorised to \'{}\' \'{}\' resource.'.format(action, resource.resource_type))

            return fnc(*args, **kwargs)

        return check_permissions
    return privilege_required
