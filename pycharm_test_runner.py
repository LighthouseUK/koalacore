"""
This file is just a helper for running unittests in PyCharm.
As the module test files are not in the root of the project the 'vendor' setups fom *.yaml break.
Subsequently the unittest will fail to load as the dependencies are missing.

In addition, because of some quirks with the app engine SDK loader, we have to fix the system path and use a non-default
version of webob (webtest will not work without a version > 1.2)
"""
import os
import sys

def fix_sys_path():
    try:
        from dev_appserver import fix_sys_path, _DIR_PATH
        fix_sys_path()
        # must be after fix_sys_path
        # uses non-default version of webob
        webob_path = os.path.join(_DIR_PATH, 'lib', 'webob-1.2.3')
        sys.path = [webob_path] + sys.path
    except ImportError, e:
        print 'fix failed: {}'.format(e.message)

fix_sys_path()

import unittest
from koalacore.tests.test_task import TestTaskHandler
# from koalacore.tests.test_api import TestResource
# from koalacore.tests.test_api import TestGaeApi
# from koalacore.tests.test_api import TestAPIConfigParserDefaults
# from koalacore.tests.test_api import TestAPIConfigParser
# from koalacore.tests.test_api import *
# from koalacore.tests.test_search import *

# TODO: Import your tests so that they run automatically when we invoke this file from pycharm

__author__ = 'Matt'


if __name__ == '__main__':
    unittest.main()
