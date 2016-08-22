# -*- coding: utf-8 -*-
"""
    koalarbac.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Simple rbac setup. Accepts a 'user' object which contains a 'permissions' property. The permissions property
    should be an object with 'roles', and 'acl' properties. Roles is a set of strings which are the role code names.
    ACL is a dict of sets. The dict keys should be a resource_uid and the set values should be a list of allowed
    actions.

    global_acl needs to be a dict mapping roles to a set of actions e.g.

    global_acl = {
        'sysadmin': {'delete_user', 'delete_company'},
        'admin': {'update_user_password', 'update_company'},
    }

    The user_can method can be hooked into by other functions. If you want to deny an action then raise
    PermissionDenied with an appropriate error message (for debugging purposes; user won't see).

    User roles are not hierarchical - if you want an admin role to be able to do everything that normal users can,
    you need to grant multiple roles to the user

    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""

import logging
from .api import Resource
from blinker import signal
from google.appengine.ext import ndb


__author__ = 'Matt Badger'


class PermissionDenied(Exception):
    pass


class PermissionsStorage(object):
    """
    Simple permissions storage designed for use with the koala RBAC class. You can extend this or even replace it
    entirely depending on your needs.
    """
    def __init__(self, roles=None, acl=None, cache=None, **kwargs):
        if roles is None:
            self.roles = set()
        else:
            self.roles = roles

        if acl is None:
            self.acl = {}
        else:
            self.acl = acl

        if cache is None:
            self._cache = {}
        else:
            self._cache = cache

    def __repr__(self):
        args = 'roles={!r}, acl={!r}'.format(self.roles, self.acl)
        return '{}({})'.format(self.__class__.__name__, args)

    def __str__(self):
        return '{}\n  Roles: {}\n  ACL: {}'.format(self.__class__.__name__, self.roles, self.acl)

    def add_role(self, role):
        self.roles.add(role)
        self.clear_cache()

    def remove_role(self, role):
        self.roles.remove(role)
        self.clear_cache()

    def set_acl_entry(self, resource_uid, actions_set):
        self.acl[resource_uid] = actions_set
        self.clear_cache()

    def remove_acl_entry(self, resource_uid):
        self.acl.pop(resource_uid, None)
        self.clear_cache()

    def clear_cache(self):
        self._cache = {}


class RBAC(Resource):
    _global_acl = None
    _user_valid_actions_cache_key = 'valid_actions'

    @classmethod
    def configure(cls, global_acl):
        # Needs to be a dict mapping roles to actions. Internally we will build a global action list, if needed
        cls._global_acl = global_acl

    @classmethod
    def _internal_user_can(cls, user, action, resource_uid=None):
        if resource_uid:
            # We are looking for a resource specific action. We check the user's acl to see if it is there
            try:
                resource_acl = user.permissions.acl[resource_uid]
            except KeyError:
                return False
            else:
                valid = action in resource_acl

                if not valid:
                    return False

                if signal('user_can').has_receivers_for(cls):
                    try:
                        signal('user_can').send(cls, user=user, action=action, resource_uid=resource_uid)
                    except PermissionDenied, e:
                        logging.debug(u'\'{}\' denied to {} because {}'.format(action, user.uid, e.message))
                        return False
                return True
        else:
            # We aren't looking for resource specific actions, so check the global rbac
            if not user.permissions._cache or cls._user_valid_actions_cache_key not in user.permissions._cache:
                # We need to get all of the user's roles and then compile a list of valid actions.
                valid_user_actions = set()
                for user_role in user.permissions.roles:
                    try:
                        valid_user_actions = valid_user_actions | cls._global_acl[user_role]
                    except KeyError:
                        # The role is not defined in the global acl; skip. We should probably raise an exception though!
                        pass
                # Add the compiled action list to cache - it's a fairly expensive operation to be doing multiple times
                # per request.
                user.permissions._cache[cls._user_valid_actions_cache_key] = valid_user_actions
            else:
                valid_user_actions = user.permissions._cache[cls._user_valid_actions_cache_key]

            valid = action in valid_user_actions

            if not valid:
                return False

            if signal('user_can').has_receivers_for(cls):
                try:
                    signal('user_can').send(cls, user=user, action=action, resource_uid=resource_uid)
                except PermissionDenied, e:
                    logging.debug(u'\'{}\' denied to {} because {}'.format(action, user.uid, e.message))
                    return False
            return True

    @classmethod
    def user_can(cls, user, action, resource_uid=None):
        # This allows us to grant permissions to the user without knowing the user uid (which won't be available until
        # after insert if you're using Google NDB). Simply set an acl entry on the user with resource_uid of 'self'
        if resource_uid is not None and user.uid == resource_uid:
            resource_uid = 'self'

        return cls._internal_user_can(user=user, action=action, resource_uid=resource_uid)

    @classmethod
    def user_is(cls, user, role):
        return role in user.permissions.roles
