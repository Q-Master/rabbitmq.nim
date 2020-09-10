type
    InvalidFieldTypeException* = object of CatchableError
    InvalidFrameException* = object of CatchableError
    InvalidFrameMethodException* = object of CatchableError
    ConnectionException* = object of Exception
    ConnectionFrameError* = object of ConnectionException
    ConnectionSyntaxError* = object of ConnectionException
    ConnectionCommandInvalid* = object of ConnectionException
    ConnectionChannelError* = object of ConnectionException
    ConnectionUnexpectedFrame* = object of ConnectionException
    ConnectionResourceError* = object of ConnectionException
    ConnectionNotAllowed* = object of ConnectionException
    ConnectionNotImplemented* = object of ConnectionException
    ConnectionInternalError* = object of ConnectionException
    ConnectionClosed* = object of ConnectionException
