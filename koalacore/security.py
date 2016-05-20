# -*- coding: utf-8 -*-
"""
    koala.security
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Security related helpers such as secure password hashing tools.

    :copyright: (c) 2014 by the Werkzeug Team
    :license: BSD, see LICENSE for more details.
    :copyright: (c) 2011 Yesudeep Mangalapilly <yesudeep@gmail.com>
    :license: Apache Sotware License, see LICENSE for details.

"""

import sys
import os
import hmac
import hashlib
import posixpath
import codecs
import random
import math
from struct import Struct
from random import SystemRandom
from operator import xor
from itertools import starmap, izip


_rng = random.SystemRandom()

text_type = unicode
string_types = (str, unicode)
range_type = xrange


def to_bytes(x, charset=sys.getdefaultencoding(), errors='strict'):
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray, buffer)):
        return bytes(x)
    if isinstance(x, unicode):
        return x.encode(charset, errors)
    raise TypeError('Expected bytes')


def to_native(x, charset=sys.getdefaultencoding(), errors='strict'):
    if x is None or isinstance(x, str):
        return x
    return x.encode(charset, errors)


SALT_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
DEFAULT_PBKDF2_ITERATIONS = 1000


_pack_int = Struct('>I').pack
_builtin_safe_str_cmp = getattr(hmac, 'compare_digest', None)
_sys_rng = SystemRandom()
_os_alt_seps = list(sep for sep in [os.path.sep, os.path.altsep]
                    if sep not in (None, '/'))


def _find_hashlib_algorithms():
    algos = getattr(hashlib, 'algorithms', None)
    if algos is None:
        algos = ('md5', 'sha1', 'sha224', 'sha256', 'sha384', 'sha512')
    rv = {}
    for algo in algos:
        func = getattr(hashlib, algo, None)
        if func is not None:
            rv[algo] = func
    return rv
_hash_funcs = _find_hashlib_algorithms()


def pbkdf2_hex(data, salt, iterations=DEFAULT_PBKDF2_ITERATIONS,
               keylen=None, hashfunc=None):
    """Like :func:`pbkdf2_bin`, but returns a hex-encoded string.

    .. versionadded:: 0.9

    :param data: the data to derive.
    :param salt: the salt for the derivation.
    :param iterations: the number of iterations.
    :param keylen: the length of the resulting key.  If not provided,
                   the digest size will be used.
    :param hashfunc: the hash function to use.  This can either be the
                     string name of a known hash function, or a function
                     from the hashlib module.  Defaults to sha1.
    """
    rv = pbkdf2_bin(data, salt, iterations, keylen, hashfunc)
    return to_native(codecs.encode(rv, 'hex_codec'))


_has_native_pbkdf2 = hasattr(hashlib, 'pbkdf2_hmac')


def pbkdf2_bin(data, salt, iterations=DEFAULT_PBKDF2_ITERATIONS,
               keylen=None, hashfunc=None):
    """Returns a binary digest for the PBKDF2 hash algorithm of `data`
    with the given `salt`. It iterates `iterations` times and produces a
    key of `keylen` bytes. By default, SHA-1 is used as hash function;
    a different hashlib `hashfunc` can be provided.

    .. versionadded:: 0.9

    :param data: the data to derive.
    :param salt: the salt for the derivation.
    :param iterations: the number of iterations.
    :param keylen: the length of the resulting key.  If not provided
                   the digest size will be used.
    :param hashfunc: the hash function to use.  This can either be the
                     string name of a known hash function or a function
                     from the hashlib module.  Defaults to sha1.
    """
    if isinstance(hashfunc, string_types):
        hashfunc = _hash_funcs[hashfunc]
    elif not hashfunc:
        hashfunc = hashlib.sha1
    data = to_bytes(data)
    salt = to_bytes(salt)

    # If we're on Python with pbkdf2_hmac we can try to use it for
    # compatible digests.
    if _has_native_pbkdf2:
        _test_hash = hashfunc()
        if hasattr(_test_hash, 'name') and \
           _test_hash.name in _hash_funcs:
            return hashlib.pbkdf2_hmac(_test_hash.name,
                                       data, salt, iterations,
                                       keylen)

    mac = hmac.HMAC(data, None, hashfunc)
    if not keylen:
        keylen = mac.digest_size

    def _pseudorandom(x, mac=mac):
        h = mac.copy()
        h.update(x)
        return bytearray(h.digest())
    buf = bytearray()
    for block in range_type(1, -(-keylen // mac.digest_size) + 1):
        rv = u = _pseudorandom(salt + _pack_int(block))
        for i in range_type(iterations - 1):
            u = _pseudorandom(bytes(u))
            rv = bytearray(starmap(xor, izip(rv, u)))
        buf.extend(rv)
    return bytes(buf[:keylen])


def safe_str_cmp(a, b):
    """This function compares strings in somewhat constant time.  This
    requires that the length of at least one string is known in advance.

    Returns `True` if the two strings are equal, or `False` if they are not.

    .. versionadded:: 0.7
    """
    if isinstance(a, text_type):
        a = a.encode('utf-8')
    if isinstance(b, text_type):
        b = b.encode('utf-8')

    if _builtin_safe_str_cmp is not None:
        return _builtin_safe_str_cmp(a, b)

    if len(a) != len(b):
        return False

    rv = 0
    for x, y in izip(a, b):
        rv |= ord(x) ^ ord(y)

    return rv == 0


def gen_salt(length):
    """Generate a random string of SALT_CHARS with specified ``length``."""
    if length <= 0:
        raise ValueError('Salt length must be positive')
    return ''.join(_sys_rng.choice(SALT_CHARS) for _ in range_type(length))


def _hash_internal(method, salt, password):
    """Internal password hash helper.  Supports plaintext without salt,
    unsalted and salted passwords.  In case salted passwords are used
    hmac is used.
    """
    if method == 'plain':
        return password, method

    if isinstance(password, text_type):
        password = password.encode('utf-8')

    if method.startswith('pbkdf2:'):
        args = method[7:].split(':')
        if len(args) not in (1, 2):
            raise ValueError('Invalid number of arguments for PBKDF2')
        method = args.pop(0)
        iterations = args and int(args[0] or 0) or DEFAULT_PBKDF2_ITERATIONS
        is_pbkdf2 = True
        actual_method = 'pbkdf2:%s:%d' % (method, iterations)
    else:
        is_pbkdf2 = False
        actual_method = method

    hash_func = _hash_funcs.get(method)
    if hash_func is None:
        raise TypeError('invalid method %r' % method)

    if is_pbkdf2:
        if not salt:
            raise ValueError('Salt is required for PBKDF2')
        rv = pbkdf2_hex(password, salt, iterations,
                        hashfunc=hash_func)
    elif salt:
        if isinstance(salt, text_type):
            salt = salt.encode('utf-8')
        rv = hmac.HMAC(salt, password, hash_func).hexdigest()
    else:
        h = hash_func()
        h.update(password)
        rv = h.hexdigest()
    return rv, actual_method


def generate_password_hash(password, method='pbkdf2:sha1', salt_length=8):
    """Hash a password with the given method and salt with with a string of
    the given length.  The format of the string returned includes the method
    that was used so that :func:`check_password_hash` can check the hash.

    The format for the hashed string looks like this::

        method$salt$hash

    This method can **not** generate unsalted passwords but it is possible
    to set the method to plain to enforce plaintext passwords.  If a salt
    is used, hmac is used internally to salt the password.

    If PBKDF2 is wanted it can be enabled by setting the method to
    ``pbkdf2:method:iterations`` where iterations is optional::

        pbkdf2:sha1:2000$salt$hash
        pbkdf2:sha1$salt$hash

    :param password: the password to hash.
    :param method: the hash method to use (one that hashlib supports). Can
                   optionally be in the format ``pbkdf2:<method>[:iterations]``
                   to enable PBKDF2.
    :param salt_length: the length of the salt in letters.
    """
    salt = method != 'plain' and gen_salt(salt_length) or ''
    h, actual_method = _hash_internal(method, salt, password)
    return '%s$%s$%s' % (actual_method, salt, h)


def check_password_hash(pwhash, password):
    """check a password against a given salted and hashed password value.
    In order to support unsalted legacy passwords this method supports
    plain text passwords, md5 and sha1 hashes (both salted and unsalted).

    Returns `True` if the password matched, `False` otherwise.

    :param pwhash: a hashed string like returned by
                   :func:`generate_password_hash`.
    :param password: the plaintext password to compare against the hash.
    """
    if pwhash.count('$') < 2:
        return False
    method, salt, hashval = pwhash.split('$', 2)
    return safe_str_cmp(_hash_internal(method, salt, password)[0], hashval)


def safe_join(directory, filename):
    """Safely join `directory` and `filename`.  If this cannot be done,
    this function returns ``None``.

    :param directory: the base directory.
    :param filename: the untrusted filename relative to that directory.
    """
    filename = posixpath.normpath(filename)
    for sep in _os_alt_seps:
        if sep in filename:
            return None
    if os.path.isabs(filename) or filename.startswith('../'):
        return None
    return os.path.join(directory, filename)


def generate_random_string(length=None, entropy=None, pool=SALT_CHARS):
    """Generates a random string using the given sequence pool.

    To generate stronger passwords, use ASCII_PRINTABLE as pool.

    Entropy is:

         H = log2(N**L)

    where:

    - H is the entropy in bits.
    - N is the possible symbol count
    - L is length of string of symbols

    Entropy chart::

        -----------------------------------------------------------------
        Symbol set              Symbol Count (N)  Entropy per symbol (H)
        -----------------------------------------------------------------
        HEXADECIMAL_DIGITS      16                4.0000 bits
        DIGITS                  10                3.3219 bits
        LOWERCASE_ALPHA         26                4.7004 bits
        UPPERCASE_ALPHA         26                4.7004 bits
        PUNCTUATION             32                5.0000 bits
        LOWERCASE_ALPHANUMERIC  36                5.1699 bits
        UPPERCASE_ALPHANUMERIC  36                5.1699 bits
        ALPHA                   52                5.7004 bits
        ALPHANUMERIC            62                5.9542 bits
        ASCII_PRINTABLE         94                6.5546 bits
        ALL_PRINTABLE           100               6.6438 bits

    :param length:
        The length of the random sequence. Use this or `entropy`, not both.
    :param entropy:
        Desired entropy in bits. Use this or `length`, not both.
        Use this to generate passwords based on entropy:
        http://en.wikipedia.org/wiki/Password_strength
    :param pool:
        A sequence of characters from which random characters are chosen.
        Default to case-sensitive alpha-numeric characters.
    :returns:
        A string with characters randomly chosen from the pool.
    """
    pool = list(set(pool))

    if length and entropy:
        raise ValueError('Use length or entropy, not both.')

    if length <= 0 and entropy <= 0:
        raise ValueError('Length or entropy must be greater than 0.')

    if entropy:
        log_of_2 = 0.6931471805599453
        length = long(math.ceil((log_of_2 / math.log(len(pool))) * entropy))

    return ''.join(_rng.choice(pool) for _ in xrange(length))
