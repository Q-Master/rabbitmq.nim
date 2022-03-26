import std/[asyncdispatch]
import pkg/networkutils/buffered_socket
import ../util/endians
import ../exceptions
import ./connection

export connection

const NO_SUCH_METHOD_STR = "No such method"

type
  AMQPMethods* = enum
    AMQP_CONNECTION_START_METHOD = CONNECTION_START_METHOD_ID
    AMQP_CONNECTION_START_OK_METHOD = CONNECTION_START_OK_METHOD_ID
    AMQP_CONNECTION_SECURE_METHOD = CONNECTION_SECURE_METHOD_ID
    AMQP_CONNECTION_SECURE_OK_METHOD = CONNECTION_SECURE_OK_METHOD_ID
    AMQP_CONNECTION_TUNE_METHOD = CONNECTION_TUNE_METHOD_ID
    AMQP_CONNECTION_TUNE_OK_METHOD = CONNECTION_TUNE_OK_METHOD_ID
    AMQP_CONNECTION_OPEN_METHOD = CONNECTION_OPEN_METHOD_ID
    AMQP_CONNECTION_OPEN_OK_METHOD = CONNECTION_OPEN_OK_METHOD_ID
    AMQP_CONNECTION_CLOSE_METHOD = CONNECTION_CLOSE_METHOD_ID
    AMQP_CONNECTION_CLOSE_OK_METHOD = CONNECTION_CLOSE_OK_METHOD_ID
    AMQP_CONNECTION_BLOCKED_METHOD = CONNECTION_BLOCKED_METHOD_ID
    AMQP_CONNECTION_UNBLOCKED_METHOD = CONNECTION_UNBLOCKED_METHOD_ID

  AMQPMetodKind* = enum
    NONE = 0
    CONNECTION = CONNECTION_METHODS

  AMQPMethod* = ref AMQPMethodObj
  AMQPMethodObj* = object of RootObj
    methodId*: uint32
    case kind*: AMQPMetodKind
    of CONNECTION:
      connObj*: AMQPConnection
    else:
      discard

proc newMethod*(id: uint32): AMQPMethod =
  let (idHi, _) = uint32touints16(id)
  result = AMQPMethod(kind: AMQPMetodKind(idHi), methodId: id)

proc len*(meth: AMQPMethod): int = 
  result = sizeInt32Uint32
  case meth.kind
  of CONNECTION:
    result.inc(meth.connObj.len)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc decodeMethod*(src: AsyncBufferedSocket): Future[AMQPMethod] {.async.} =
  let methodId = await src.readBEU32()
  let meth = newMethod(methodId)
  result = meth
  case meth.kind
  of CONNECTION:
    result.connObj = await src.decode(methodId)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc encodeMethod*(dst: AsyncBufferedSocket, meth: AMQPMethod) {.async.} =
  await dst.writeBE(meth.methodId)
  case meth.kind
  of CONNECTION:
    await meth.connObj.encode(dst)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)
  #await dst.flush()
