# -*- coding: utf-8 -*-
"""
    koala
    ~~~~~~~~

    Koala is a set of tools and extensions to aid in developing on Google App Engine

    :copyright: (c) 2015 by Lighthouse
    :license: LGPL
"""
from types import ModuleType
import sys

__version__ = '0.1.1-alpha'
__author__ = 'Matt Badger'

PACKAGE_NAME = 'koalacore'

if sys.version_info < (2, 7):  # pragma: no cover
    raise Exception('Koala requires Python versions 2.7 or later.')

STATICA_HACK = True
if STATICA_HACK:  # pragma: no cover
    # This is never executed, but tricks static analyzers (PyDev, PyCharm,
    # pylint, etc.) into knowing the types of these symbols, and what
    # they contain.
    from koalacore.api import BaseAPI, BaseSubAPI, BaseResource, Resource, BaseResourceProperty, ResourceProperty, ComputedResourceProperty  # noqa
    from koalacore.tools import DictDiffer, generate_autocomplete_tokens, eval_boolean_string, convert_to_unicode, csv_item_convert  # noqa
    from koalacore.search import GAESearchInterface, Result  # noga
    from koalacore.rbac import PermissionDenied, PermissionsStorage, RBAC  # noga
    from koalacore.privileges import Privilege, PrivilegeConstants, SecurityObject, AugmentedPrivilegeEvaluator, AugmentedSecurityObject, authorise, UnauthorisedCredentials, mock_credentials  # noqa
    from koalacore.datastore import NDBEventedInterface, NDBUniques, ModelUtils, NDBResource, ResourceNotFound, ResourceException, UniqueValueRequired  # noqa
    from koalacore.security import generate_password_hash, generate_random_string, check_password_hash  # noqa
    from koalacore.exceptions import KoalaException, InvalidUser, UnauthorisedUser  # noqa
    from koalacore.validators import URIValidator, ConditionalURIValidator  # noqa


# import mapping to objects in other modules
all_by_module = {
    '{}.api'.format(PACKAGE_NAME): ['BaseAPI', 'BaseSubAPI', 'BaseResource', 'Resource', 'BaseResourceProperty', 'ResourceProperty', 'ComputedResourceProperty'],
    '{}.tools'.format(PACKAGE_NAME): ['DictDiffer', 'generate_autocomplete_tokens', 'eval_boolean_string', 'convert_to_unicode', 'csv_item_convert'],
    '{}.search'.format(PACKAGE_NAME): ['GAESearchInterface', 'Result'],
    '{}.rbac'.format(PACKAGE_NAME): ['PermissionDenied', 'PermissionsStorage', 'RBAC'],
    '{}.privileges'.format(PACKAGE_NAME): ['Privilege', 'PrivilegeConstants', 'SecurityObject',
                                           'AugmentedPrivilegeEvaluator', 'AugmentedSecurityObject', 'authorise',
                                           'UnauthorisedCredentials', 'mock_credentials'],
    '{}.datastore'.format(PACKAGE_NAME): ['NDBEventedInterface', 'NDBUniques', 'ModelUtils', 'NDBResource', 'ResourceNotFound', 'ResourceException', 'UniqueValueRequired'],
    '{}.security'.format(PACKAGE_NAME): ['generate_password_hash', 'generate_random_string', 'check_password_hash'],
    '{}.exceptions'.format(PACKAGE_NAME): ['KoalaException', 'ResourceException', 'UniqueValueRequired', 'InvalidUser', 'UnauthorisedUser'],
    '{}.validators'.format(PACKAGE_NAME): ['URIValidator', 'ConditionalURIValidator'],
}

# modules that should be imported when accessed as attributes of koala
# attribute_modules = frozenset(['exceptions'])
attribute_modules = frozenset([])

object_origins = {}
for module, items in all_by_module.iteritems():
    for item in items:
        object_origins[item] = module


class module(ModuleType):
    """Automatically import objects from the modules."""

    def __getattr__(self, name):
        if name in object_origins:
            module = __import__(object_origins[name], None, None, [name])
            for extra_name in all_by_module[module.__name__]:
                setattr(self, extra_name, getattr(module, extra_name))
            return getattr(module, name)
        elif name in attribute_modules:
            __import__('{}.{}'.format(PACKAGE_NAME, name))
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        """Just show what we want to show."""
        result = list(new_module.__all__)
        result.extend(('__file__', '__path__', '__doc__', '__all__',
                       '__docformat__', '__name__', '__path__',
                       '__package__', '__version__'))
        return result

# keep a reference to this module so that it's not garbage collected
old_module = sys.modules[PACKAGE_NAME]


# setup the new module and patch it into the dict of loaded modules
new_module = sys.modules[PACKAGE_NAME] = module(PACKAGE_NAME)
new_module.__dict__.update({
    '__file__': __file__,
    '__package__': PACKAGE_NAME,
    '__path__': __path__,
    '__doc__': __doc__,
    '__version__': __version__,
    '__all__': tuple(object_origins) + tuple(attribute_modules),
    '__docformat__': 'restructuredtext en'
})
