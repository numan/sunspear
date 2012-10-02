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
