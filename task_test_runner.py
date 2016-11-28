"""
This file is just a helper for running unittests in PyCharm.
As the module test files are not in the root of the project the 'vendor' setups fom *.yaml break.
Subsequently the unittest will fail to load as the dependencies are missing.
"""
import os
import sys

def fix_sys_path(sdkpath):

    sys.path.append(sdkpath)
    try:
        from dev_appserver import fix_sys_path, _DIR_PATH
        fix_sys_path()
        # must be after fix_sys_path
        # uses non-default version of webob
        webob_path = os.path.join(_DIR_PATH, 'lib', 'webob-1.2.3')
        sys.path = [webob_path] + sys.path
    except ImportError, e:
        print 'fix failed: {}'.format(e.message)

fix_sys_path(sdkpath='/Users/matt/Downloads/google-cloud-sdk/platform/google_appengine')

import unittest
from koalacore.tests.test_task import TestTaskHandler


if __name__ == '__main__':
    unittest.main()
