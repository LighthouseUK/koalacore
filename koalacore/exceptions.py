# -*- coding: utf-8 -*-
"""
    koalacore.exceptions
    ~~~~~~~~~~~~~~~~~~

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

__author__ = 'Matt Badger'


class KoalaException(Exception):
    """
    Base exception class for API functions. Can be used to distinguish API errors
    """
    pass


class InvalidUser(KoalaException):
    """
    User not set or invalid
    """
    pass


class UnauthorisedUser(KoalaException):
    """
    User set but does not have permission to perform the requested action
    """
    pass


class ResourceNotFound(KoalaException):
    """
    Raised when a datastore method that requires a resource cannot find said resource. Usually because the supplied uid
    does not exist.
    """
    pass


class ResourceException(KoalaException):
    """
    Used when there was a problem persisting changes to a resource. Generally this is the base exception; more granular
    exceptions would be useful, but it provides a catch all fallback.
    """
    pass


class UniqueValueRequired(ResourceException, ValueError):
    """
    Raised during the insert, update operations in the datastore interfaces. If a lock on the unique value cannot be
    obtained then this exception is raised. It should detail the reason for failure by listing the values that locks
    could not be obtained for.
    """

    def __init__(self, errors, message=u'Unique resource values already exist in the datastore'):
        super(UniqueValueRequired, self).__init__(message)



