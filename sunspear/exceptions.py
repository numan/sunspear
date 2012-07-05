class SunspearBaseException(Exception):
    pass


class SunspearInvalidConfigurationError(SunspearBaseException):
    pass


class SunspearValidationException(SunspearBaseException):
    pass
