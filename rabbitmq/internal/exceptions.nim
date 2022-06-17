type
    InvalidFieldTypeException* = object of CatchableError
    InvalidFrameException* = object of CatchableError
    InvalidFrameMethodException* = object of CatchableError
    InvalidShortStringSizeException* = object of CatchableError
    RMQConnectionException* = object of CatchableError
    RMQConnectionFailed* = object of RMQConnectionException
    RMQConnectionFrameError* = object of RMQConnectionException
    RMQConnectionSyntaxError* = object of RMQConnectionException
    RMQConnectionCommandInvalid* = object of RMQConnectionException
    RMQConnectionChannelError* = object of RMQConnectionException
    RMQConnectionUnexpectedFrame* = object of RMQConnectionException
    RMQConnectionResourceError* = object of RMQConnectionException
    RMQConnectionNotAllowed* = object of RMQConnectionException
    RMQConnectionNotImplemented* = object of RMQConnectionException
    RMQConnectionInternalError* = object of RMQConnectionException
    RMQConnectionClosed* = object of RMQConnectionException
    RMQChannelClosed* = object of RMQConnectionException

type
    # AMQP Errors
    AMQPError = object of CatchableError
        value*: int
    AMQPIncompatibleProtocol* = object of AMQPError
    AMQPContentTooLarge* = object of AMQPError
    AMQPNoRoute* = object of AMQPError
    AMQPNoConsumers* = object of AMQPError
    AMQPAccessRefused* = object of AMQPError
    AMQPNotFound* = object of AMQPError
    AMQPResourceLocked* = object of AMQPError
    AMQPPreconditionFailed* = object of AMQPError
    AMQPConnectionForced* = object of AMQPError
    AMQPInvalidPath* = object of AMQPError
    AMQPFrameError* = object of AMQPError
    AMQPSyntaxError* = object of AMQPError
    AMQPCommandInvalid* = object of AMQPError
    AMQPChannelError* = object of AMQPError
    AMQPUnexpectedFrame* = object of AMQPError
    AMQPUnexpectedMethod* = object of AMQPError
    AMQPResourceError* = object of AMQPError
    AMQPNotAllowed* = object of AMQPError
    AMQPNotImplemented* = object of AMQPError
    AMQPInternalError* = object of AMQPError

type
    # Frame Errors
    FrameUnmarshalingException* = object of CatchableError

type
    # Auth Errors
    AuthenticationError* = object of CatchableError

#[
# AMQP Errors
class AMQPContentTooLarge(Warning):
    """
    The client attempted to transfer content larger than the server could
    accept at the present time. The client may retry at a later time.

    """
    name = 'CONTENT-TOO-LARGE'
    value = 311


class AMQPNoRoute(Warning):
    """
    Undocumented AMQP Soft Error

    """
    name = 'NO-ROUTE'
    value = 312


class AMQPNoConsumers(Warning):
    """
    When the exchange cannot deliver to a consumer when the immediate flag is
    set. As a result of pending data on the queue or the absence of any
    consumers of the queue.

    """
    name = 'NO-CONSUMERS'
    value = 313


class AMQPAccessRefused(Warning):
    """
    The client attempted to work with a server entity to which it has no access
    due to security settings.

    """
    name = 'ACCESS-REFUSED'
    value = 403


class AMQPNotFound(Warning):
    """
    The client attempted to work with a server entity that does not exist.

    """
    name = 'NOT-FOUND'
    value = 404


class AMQPResourceLocked(Warning):
    """
    The client attempted to work with a server entity to which it has no access
    because another client is working with it.

    """
    name = 'RESOURCE-LOCKED'
    value = 405


class AMQPPreconditionFailed(Warning):
    """
    The client requested a method that was not allowed because some
    precondition failed.

    """
    name = 'PRECONDITION-FAILED'
    value = 406


class AMQPConnectionForced(Exception):
    """
    An operator intervened to close the connection for some reason. The client
    may retry at some later date.

    """
    name = 'CONNECTION-FORCED'
    value = 320


class AMQPInvalidPath(Exception):
    """
    The client tried to work with an unknown virtual host.

    """
    name = 'INVALID-PATH'
    value = 402


class AMQPFrameError(Exception):
    """
    The sender sent a malformed frame that the recipient could not decode. This
    strongly implies a programming error in the sending peer.

    """
    name = 'FRAME-ERROR'
    value = 501


class AMQPSyntaxError(Exception):
    """
    The sender sent a frame that contained illegal values for one or more
    fields. This strongly implies a programming error in the sending peer.

    """
    name = 'SYNTAX-ERROR'
    value = 502


class AMQPCommandInvalid(Exception):
    """
    The client sent an invalid sequence of frames, attempting to perform an
    operation that was considered invalid by the server. This usually implies a
    programming error in the client.

    """
    name = 'COMMAND-INVALID'
    value = 503


class AMQPChannelError(Exception):
    """
    The client attempted to work with a channel that had not been correctly
    opened. This most likely indicates a fault in the client layer.

    """
    name = 'CHANNEL-ERROR'
    value = 504


class AMQPUnexpectedFrame(Exception):
    """
    The peer sent a frame that was not expected, usually in the context of a
    content header and body.  This strongly indicates a fault in the peer's
    content processing.

    """
    name = 'UNEXPECTED-FRAME'
    value = 505


class AMQPResourceError(Exception):
    """
    The server could not complete the method because it lacked sufficient
    resources. This may be due to the client creating too many of some type of
    entity.

    """
    name = 'RESOURCE-ERROR'
    value = 506


class AMQPNotAllowed(Exception):
    """
    The client tried to work with some entity in a manner that is prohibited by
    the server, due to security settings or by some other criteria.

    """
    name = 'NOT-ALLOWED'
    value = 530


class AMQPNotImplemented(Exception):
    """
    The client tried to use functionality that is not implemented in the
    server.

    """
    name = 'NOT-IMPLEMENTED'
    value = 540


class AMQPInternalError(Exception):
    """
    The server could not complete the method because of an internal error. The
    server may require intervention by an operator in order to resume normal
    operations.

    """
    name = 'INTERNAL-ERROR'
    value = 541


# AMQP Error code to class mapping
ERRORS = {
    320: AMQPConnectionForced,
    505: AMQPUnexpectedFrame,
    502: AMQPSyntaxError,
    503: AMQPCommandInvalid,
    530: AMQPNotAllowed,
    504: AMQPChannelError,
    402: AMQPInvalidPath,
    403: AMQPAccessRefused,
    404: AMQPNotFound,
    405: AMQPResourceLocked,
    406: AMQPPreconditionFailed,
    311: AMQPContentTooLarge,
    312: AMQPNoRoute,
    313: AMQPNoConsumers,
    506: AMQPResourceError,
    540: AMQPNotImplemented,
    541: AMQPInternalError,
    501: AMQPFrameError
}
]#