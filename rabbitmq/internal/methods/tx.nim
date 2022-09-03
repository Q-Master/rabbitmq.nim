import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../exceptions

const TX_METHODS* = 0x005A.uint16
const TX_SELECT_METHOD_ID* = 0x005A000A.uint32
const TX_SELECT_OK_METHOD_ID* = 0x005A000B.uint32
const TX_COMMIT_METHOD_ID* = 0x005A0014.uint32
const TX_COMMIT_OK_METHOD_ID* = 0x005A0015.uint32
const TX_ROLLBACK_METHOD_ID* = 0x005A001E.uint32
const TX_ROLLBACK_OK_METHOD_ID* = 0x005A001F.uint32

{. push warning[ProveField]:on .}

type
  AMQPTxKind = enum
    AMQP_TX_NONE = 0
    AMQP_TX_SELECT_SUBMETHOD = (TX_SELECT_METHOD_ID and 0x0000ffff).uint16
    AMQP_TX_SELECT_OK_SUBMETHOD = (TX_SELECT_OK_METHOD_ID and 0x0000ffff).uint16
    AMQP_TX_COMMIT_SUBMETHOD = (TX_COMMIT_METHOD_ID and 0x0000ffff).uint16
    AMQP_TX_COMMIT_OK_SUBMETHOD = (TX_COMMIT_OK_METHOD_ID and 0x0000ffff).uint16
    AMQP_TX_ROLLBACK_SUBMETHOD = (TX_ROLLBACK_METHOD_ID and 0x0000ffff).uint16
    AMQP_TX_ROLLBACK_OK_SUBMETHOD = (TX_ROLLBACK_OK_METHOD_ID and 0x0000ffff).uint16
  
  AMQPTx* = ref AMQPTxObj
  AMQPTxObj* = object of RootObj
    case kind*: AMQPTxKind
    of AMQP_TX_SELECT_SUBMETHOD,
      AMQP_TX_SELECT_OK_SUBMETHOD,
      AMQP_TX_COMMIT_SUBMETHOD,
      AMQP_TX_COMMIT_OK_SUBMETHOD,
      AMQP_TX_ROLLBACK_SUBMETHOD,
      AMQP_TX_ROLLBACK_OK_SUBMETHOD:
        discard
    else:
      discard

proc len*(meth: AMQPTx): int =
  case meth.kind:
  of AMQP_TX_SELECT_SUBMETHOD,
    AMQP_TX_SELECT_OK_SUBMETHOD,
    AMQP_TX_COMMIT_SUBMETHOD,
    AMQP_TX_COMMIT_OK_SUBMETHOD,
    AMQP_TX_ROLLBACK_SUBMETHOD,
    AMQP_TX_ROLLBACK_OK_SUBMETHOD:
      result = 0
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc decode*(_: typedesc[AMQPTx], s: AsyncBufferedSocket, t: uint32): Future[AMQPTx] {.async.} =
  case t:
  of TX_SELECT_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_SELECT_SUBMETHOD)
  of TX_SELECT_OK_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_SELECT_OK_SUBMETHOD)
  of TX_COMMIT_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_COMMIT_SUBMETHOD)
  of TX_COMMIT_OK_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_COMMIT_OK_SUBMETHOD)
  of TX_ROLLBACK_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_ROLLBACK_SUBMETHOD)
  of TX_ROLLBACK_OK_METHOD_ID:
    result = AMQPTx(kind: AMQP_TX_ROLLBACK_OK_SUBMETHOD)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPTx, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_TX_SELECT_SUBMETHOD,
    AMQP_TX_SELECT_OK_SUBMETHOD,
    AMQP_TX_COMMIT_SUBMETHOD,
    AMQP_TX_COMMIT_OK_SUBMETHOD,
    AMQP_TX_ROLLBACK_SUBMETHOD,
    AMQP_TX_ROLLBACK_OK_SUBMETHOD:
      discard
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc newTxSelect*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_SELECT_SUBMETHOD)

proc newTxSelectOk*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_SELECT_OK_SUBMETHOD)

proc newTxCommit*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_COMMIT_SUBMETHOD)

proc newTxCommitOk*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_COMMIT_OK_SUBMETHOD)

proc newTxRollback*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_ROLLBACK_SUBMETHOD)

proc newTxRollbackOk*(): AMQPTx =
  result = AMQPTx(kind: AMQP_TX_ROLLBACK_OK_SUBMETHOD)
