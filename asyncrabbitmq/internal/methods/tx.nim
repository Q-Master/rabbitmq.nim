import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket


const
  TX_METHODS* = 0x005A.uint16

  TX_SELECT_METHOD_ID* = 0x005A000A.uint32
  TX_SELECT_OK_METHOD_ID* = 0x005A000B.uint32
  TX_COMMIT_METHOD_ID* = 0x005A0014.uint32
  TX_COMMIT_OK_METHOD_ID* = 0x005A0015.uint32
  TX_ROLLBACK_METHOD_ID* = 0x005A001E.uint32
  TX_ROLLBACK_OK_METHOD_ID* = 0x005A001F.uint32


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
  
  AMQPTxMethod* = ref AMQPTxMethodObj
    ## Class Grammar:
    ##   tx  = C:SELECT S:SELECT-OK
    ##       / C:COMMIT S:COMMIT-OK
    ##       / C:ROLLBACK S:ROLLBACK-OK

  AMQPTxMethodObj* = object of RootObj
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


proc len*(meth: AMQPTxMethod): int =
  case meth.kind:
  of AMQP_TX_SELECT_SUBMETHOD,
    AMQP_TX_SELECT_OK_SUBMETHOD,
    AMQP_TX_COMMIT_SUBMETHOD,
    AMQP_TX_COMMIT_OK_SUBMETHOD,
    AMQP_TX_ROLLBACK_SUBMETHOD,
    AMQP_TX_ROLLBACK_OK_SUBMETHOD:
      result = 0
  else:
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc decode*(_: typedesc[AMQPTxMethod], s: AsyncBufferedSocket, t: uint32): Future[AMQPTxMethod] {.async.} =
  case t:
  of TX_SELECT_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_SELECT_SUBMETHOD)
  of TX_SELECT_OK_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_SELECT_OK_SUBMETHOD)
  of TX_COMMIT_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_COMMIT_SUBMETHOD)
  of TX_COMMIT_OK_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_COMMIT_OK_SUBMETHOD)
  of TX_ROLLBACK_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_ROLLBACK_SUBMETHOD)
  of TX_ROLLBACK_OK_METHOD_ID:
    result = AMQPTxMethod(kind: AMQP_TX_ROLLBACK_OK_SUBMETHOD)
  else:
    raise newException(ValueError, "Wrong MethodID: " & $t)


proc encode*(meth: AMQPTxMethod, dst: AsyncBufferedSocket) {.async.} =
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
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc newTxSelect*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_SELECT_SUBMETHOD)


proc newTxSelectOk*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_SELECT_OK_SUBMETHOD)


proc newTxCommit*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_COMMIT_SUBMETHOD)


proc newTxCommitOk*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_COMMIT_OK_SUBMETHOD)


proc newTxRollback*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_ROLLBACK_SUBMETHOD)


proc newTxRollbackOk*(): AMQPTxMethod =
  result = AMQPTxMethod(kind: AMQP_TX_ROLLBACK_OK_SUBMETHOD)
