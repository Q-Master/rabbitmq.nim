import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket


const
  CONFIRM_METHODS* = 0x0055.uint16

  CONFIRM_SELECT_METHOD_ID* = 0x0055000A.uint32
  CONFIRM_SELECT_OK_METHOD_ID* = 0x0055000B.uint32


{. push warning[ProveField]:on .}

type
  AMQPConfirmKind = enum
    AMQP_CONFIRM_NONE = 0
    AMQP_CONFIRM_SELECT_SUBMETHOD = (CONFIRM_SELECT_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONFIRM_SELECT_OK_SUBMETHOD = (CONFIRM_SELECT_OK_METHOD_ID and 0x0000FFFF).uint16
  
  AMQPConfirmSelectBits = object
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPConfirmMethod* = ref AMQPConfirmMethodObj
    ## Class Grammar:
    ##   confirm = C:SELECT S:SELECT-OK

  AMQPConfirmMethodObj* = object of RootObj
    case kind*: AMQPConfirmKind
    of AMQP_CONFIRM_SELECT_SUBMETHOD:
      confirmFlags: AMQPConfirmSelectBits
    of AMQP_CONFIRM_SELECT_OK_SUBMETHOD:
      discard
    else:
      discard


proc len*(meth: AMQPConfirmMethod): int =
  result = 0
  case meth.kind:
  of AMQP_CONFIRM_SELECT_SUBMETHOD:
    result.inc(sizeInt8Uint8)
  of AMQP_CONFIRM_SELECT_OK_SUBMETHOD:
    result.inc(0)
  else:
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc decode*(_: typedesc[AMQPConfirmMethod], s: AsyncBufferedSocket, t: uint32): Future[AMQPConfirmMethod] {.async.} =
  case t:
  of CONFIRM_SELECT_METHOD_ID:
    result = AMQPConfirmMethod(kind: AMQP_CONFIRM_SELECT_SUBMETHOD)
    result.confirmFlags = cast[AMQPConfirmSelectBits](await s.readU8())
  of CONFIRM_SELECT_OK_METHOD_ID:
    discard
  else:
    raise newException(ValueError, "Wrong MethodID: " & $t)


proc encode*(meth: AMQPConfirmMethod, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_CONFIRM_SELECT_SUBMETHOD:
    await dst.write(cast[uint8](meth.confirmFlags))
  of AMQP_CONFIRM_SELECT_OK_SUBMETHOD:
    discard
  else:
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc newConfirmSelect*(noWait: bool): AMQPConfirmMethod =
  result = AMQPConfirmMethod(
    kind: AMQP_CONFIRM_SELECT_SUBMETHOD,
    confirmFlags: AMQPConfirmSelectBits(
      noWait: noWait
    )
  )


proc newConfirmSelectOk*(): AMQPConfirmMethod =
  result = AMQPConfirmMethod(
    kind: AMQP_CONFIRM_SELECT_OK_SUBMETHOD
  )


proc noWait*(self: AMQPConfirmMethod): bool =
  case self.kind
  of AMQP_CONFIRM_SELECT_SUBMETHOD:
    result = self.confirmFlags.noWait
  else:
    raise newException(FieldDefect, "No such field")


proc `noWait=`*(self: AMQPConfirmMethod, noWait: bool) =
  case self.kind
  of AMQP_CONFIRM_SELECT_SUBMETHOD:
    self.confirmFlags.noWait = noWait
  else:
    raise newException(FieldDefect, "No such field")
