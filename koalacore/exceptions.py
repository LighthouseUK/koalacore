# -*- coding: utf-8 -*-
"""
    koala.exceptions
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
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



