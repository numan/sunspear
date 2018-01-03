from __future__ import absolute_import, division, print_function, unicode_literals

import traceback
from logging import getLogger

import six

logger = getLogger(__name__)


def must_be_str(arg):
    """
    Some functions require `str` in Python 2, i.e. its binary type,
    but also `str` in Python 3, which is its text type...

    Accommodate both.
    """
    if six.PY2:
        if isinstance(arg, six.text_type):
            return arg.encode('utf-8')
        # The idea of this function is to simply remove all function calls once we're on python 3, so let's be sure we
        # always have the right type passed in in python 2, i.e. `unicode`.
        traceback.print_stack()
        logger.warn('Unexpectedly got non-unicode in `must_be_str`...', extra={'stack': True})
    return arg
