import re
import six
from urlparse import urlsplit, urlunsplit
import datetime
from decimal import Decimal

__author__ = 'Matt'


_PROTECTED_TYPES = six.integer_types + (type(None), float, Decimal, datetime.datetime, datetime.date, datetime.time)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_text(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)


def force_text(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_text, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if isinstance(s, six.text_type):
        return s
    if strings_only and is_protected_type(s):
        return s
    try:
        if not isinstance(s, six.string_types):
            if six.PY3:
                if isinstance(s, bytes):
                    s = six.text_type(s, encoding, errors)
                else:
                    s = six.text_type(s)
            elif hasattr(s, '__unicode__'):
                s = six.text_type(s)
            else:
                s = six.text_type(bytes(s), encoding, errors)
        else:
            # Note: We use .decode() here, instead of six.text_type(s, encoding,
            # errors), so that if s is a SafeBytes, it ends up being a
            # SafeText at the end.
            s = s.decode(encoding, errors)
    except UnicodeDecodeError as e:
        if not isinstance(s, Exception):
            raise ValueError(s, *e.args)
        else:
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring data without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            s = ' '.join(force_text(arg, encoding, strings_only, errors)
                         for arg in s)
    return s


class RegexValidator(object):
    regex = ''
    message = 'Enter a valid value.'
    code = 'invalid'
    inverse_match = False
    flags = 0

    def __init__(self, regex=None, message=None, code=None, inverse_match=None, flags=None):
        if regex is not None:
            self.regex = regex
        if message is not None:
            self.message = message
        if code is not None:
            self.code = code
        if inverse_match is not None:
            self.inverse_match = inverse_match
        if flags is not None:
            self.flags = flags
        if self.flags and not isinstance(self.regex, six.string_types):
            raise TypeError("If the flags are set, regex must be a regular expression string.")

        # Compile the regex if it was not passed pre-compiled.
        if isinstance(self.regex, six.string_types):
            self.regex = re.compile(self.regex, self.flags)

    def __call__(self, value):
        """
        Validates that the input matches the regular expression
        if inverse_match is False, otherwise raises ValidationError.
        """
        if not (self.inverse_match is not bool(self.regex.search(
                force_text(value)))):
            # raise ValueError(self.message, code=self.code)
            raise ValueError(self.message)

    def __eq__(self, other):
        return (
            isinstance(other, RegexValidator) and
            self.regex.pattern == other.regex.pattern and
            self.regex.flags == other.regex.flags and
            (self.message == other.message) and
            (self.code == other.code) and
            (self.inverse_match == other.inverse_match)
        )

    def __ne__(self, other):
        return not (self == other)


class URIValidator(RegexValidator):
    regex = re.compile(
        r'^(?:[a-z][a-z0-9\.\-\+]*)://'  # scheme...
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'(?!-)[A-Z\d-]{1,63}(?<!-)|'  # also cover non-dotted domain
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    message = 'Enter a valid URL.'

    def __call__(self, value):
        try:
            super(URIValidator, self).__call__(value)
        except ValueError as e:
            # Trivial case failed. Try for possible IDN domain
            if value:
                value = force_text(value)
                scheme, netloc, path, query, fragment = urlsplit(value)
                try:
                    netloc = netloc.encode('idna').decode('ascii')  # IDN -> ACE
                except UnicodeError:  # invalid domain part
                    raise e
                url = urlunsplit((scheme, netloc, path, query, fragment))
                super(URIValidator, self).__call__(url)
            else:
                raise
        else:
            url = value


class ConditionalURIValidator(URIValidator):
    def __init__(self, allowed_schemes=None, allow_fragments=True):
        self.allowed_schemes = allowed_schemes
        self.allow_fragments = allow_fragments

    def __call__(self, value):
        super(ConditionalURIValidator, self).__call__(value)
        value = force_text(value)
        if not self.allow_fragments and len(value.split('#')) > 1:
            raise ValueError('Redirect URIs must not contain fragments')
        scheme, netloc, path, query, fragment = urlsplit(value)
        if self.allowed_schemes and scheme.lower() not in self.allowed_schemes:
            raise ValueError('Redirect URI scheme is not allowed.')
