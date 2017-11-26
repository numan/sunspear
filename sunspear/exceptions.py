from __future__ import absolute_import, division, print_function, unicode_literals


class SunspearBaseException(Exception):
    pass


class SunspearRiakException(SunspearBaseException):
    pass


class SunspearInvalidConfigurationError(SunspearBaseException):
    pass


class SunspearValidationException(SunspearBaseException):
    pass


class SunspearNotFoundException(SunspearBaseException):
    pass


class SunspearDuplicateEntryException(SunspearBaseException):
    pass


class SunspearInvalidActivityException(SunspearBaseException):
    pass

class SunspearInvalidObjectException(SunspearBaseException):
    pass
