type
  RMQConnectionException* = object of CatchableError
  RMQConnectionFailed* = object of RMQConnectionException
  RMQConnectionClosed* = object of RMQConnectionException
  RMQUnexpectedMethod* = object of CatchableError
  RMQChannelsExhausted* = object of CatchableError
  RMQPNoSuchChannel* = object of CatchableError


type
  # AMQP Errors
  AMQPError = object of CatchableError
    code*: int
  AMQPIncompatibleProtocol* = object of AMQPError
  AMQPContentTooLarge* = object of AMQPError ## The payload is too large for server to accept (311)
  AMQPNoRoute* = object of AMQPError ## 'mandatory' message cannot be delivered to any queue. (312)
  AMQPNoConsumers* = object of AMQPError ## The exchange cannot deliver to a consumer when the immediate flag is set (313)
  AMQPConnectionForced* = object of AMQPError ## The connection was forced to close. (320)
  AMQPInvalidPath* = object of AMQPError ## No such virtual host present (402)
  AMQPAccessRefused* = object of AMQPError ## No access granted for this instance (403)
  AMQPNotFound* = object of AMQPError ## Server entity doesn't exist (404)
  AMQPResourceLocked* = object of AMQPError ## Server entity is locked by another process (405)
  AMQPPreconditionFailed* = object of AMQPError ## Method precondition failed, method was not allowed (406)
  AMQPFrameError* = object of AMQPError ## Malformed frame was sent, recipient could not decode it (501)
  AMQPSyntaxError* = object of AMQPError ## Frame contains illegal values for one or more fields (502)
  AMQPCommandInvalid* = object of AMQPError ## Invalid sequence of frames was sent to a server (503)
  AMQPChannelError* = object of AMQPError ## Channel was not correctly opened (504)
  AMQPUnexpectedFrame* = object of AMQPError ## The unexpected frame was sent to a server (505)
  AMQPResourceError* = object of AMQPError ## Server could not complete the method because it lacked sufficient resources (506)
  AMQPNotAllowed* = object of AMQPError ## Working with entity is prohibited for some reason (530)
  AMQPNotImplemented* = object of AMQPError ## Calling not implemented feature of a server (540)
  AMQPInternalError* = object of AMQPError ## Internal server error occured while completing the method (541)


type
  # Frame Errors
  FrameUnmarshalingException* = object of CatchableError


type
  # Auth Errors
  AuthenticationError* = object of CatchableError


template newAMQPException*(exceptn: typedesc, message: string, value: int; parentException: ref Exception = nil): untyped =
  ## Creates an exception object of type `exceptn` and sets its `msg` field
  ## to `message`. Returns the new exception object.
  (ref exceptn)(msg: message, code: value, parent: parentException)


proc raiseException*(code: uint16) =
  case code
  of 311:
    raise newAMQPException(AMQPContentTooLarge, "", code.int)
  of 312:
    raise newAMQPException(AMQPNoRoute, "", code.int)
  of 313:
    raise newAMQPException(AMQPNoConsumers, "", code.int)
  of 320:
    raise newAMQPException(AMQPConnectionForced, "", code.int)
  of 402:
    raise newAMQPException(AMQPInvalidPath, "", code.int)
  of 403:
    raise newAMQPException(AMQPAccessRefused, "", code.int)
  of 404:
    raise newAMQPException(AMQPNotFound, "", code.int)
  of 405:
    raise newAMQPException(AMQPResourceLocked, "", code.int)
  of 406:
    raise newAMQPException(AMQPPreconditionFailed, "", code.int)
  of 501:
    raise newAMQPException(AMQPFrameError, "", code.int)
  of 502:
    raise newAMQPException(AMQPSyntaxError, "", code.int)
  of 503:
    raise newAMQPException(AMQPCommandInvalid, "", code.int)
  of 504:
    raise newAMQPException(AMQPChannelError, "", code.int)
  of 505:
    raise newAMQPException(AMQPUnexpectedFrame, "", code.int)
  of 506:
    raise newAMQPException(AMQPResourceError, "", code.int)
  of 530:
    raise newAMQPException(AMQPNotAllowed, "", code.int)
  of 540: 
    raise newAMQPException(AMQPNotImplemented, "", code.int)
  of 541: 
    raise newAMQPException(AMQPInternalError, "", code.int)
  else:
    discard
