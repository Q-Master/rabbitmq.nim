import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../field


const
  ACCESS_METHODS* = 0x001E.uint16

  ACCESS_REQUEST_METHOD_ID* = 0x001E000A.uint32
  ACCESS_REQUEST_OK_METHOD_ID* = 0x001E000B.uint32


{. push warning[ProveField]:on .}

type
  AMQPAccessKind = enum
    AMQP_ACCESS_NONE = 0
    AMQP_ACCESS_REQUEST_SUBMETHOD = (ACCESS_REQUEST_METHOD_ID and 0x0000FFFF).uint16
    AMQP_ACCESS_REQUEST_OK_SUBMETHOD = (ACCESS_REQUEST_OK_METHOD_ID and 0x0000FFFF).uint16
  
  AMQPAccessRequestBits* = object
    exclusive* {.bitsize: 1.}: bool
    passive* {.bitsize: 1.}: bool
    active* {.bitsize: 1.}: bool
    write* {.bitsize: 1.}: bool
    read* {.bitsize: 1.}: bool
    unused {.bitsize: 3.}: uint8

  AMQPAccessMethod* = ref AMQPAccessMethodObj
    ## Class Grammar:
    ##   access  = C:ACCESS-REQUEST S:ACCESS-REQUEST-OK

  AMQPAccessMethodObj* = object of RootObj
    case kind*: AMQPAccessKind
    of AMQP_ACCESS_REQUEST_SUBMETHOD:
      realm*: string
      requestFlags*: AMQPAccessRequestBits
    of AMQP_ACCESS_REQUEST_OK_SUBMETHOD:
      ticket*: uint16
    else:
      discard


proc len*(meth: AMQPAccessMethod): int =
  result = 0
  case meth.kind:
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result.inc(meth.realm.len + sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
  of AMQP_ACCESS_REQUEST_OK_SUBMETHOD:
    result.inc(sizeInt16Uint16)
  else:
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc decode*(_: typedesc[AMQPAccessMethod], s: AsyncBufferedSocket, t: uint32): Future[AMQPAccessMethod] {.async.} =
  case t:
  of ACCESS_REQUEST_METHOD_ID:
    result = AMQPAccessMethod(kind: AMQP_ACCESS_REQUEST_SUBMETHOD)
    result.realm = await s.decodeShortString()
    result.requestFlags = cast[AMQPAccessRequestBits](await s.readU8())
  of ACCESS_REQUEST_OK_METHOD_ID:
    result = AMQPAccessMethod(kind: AMQP_ACCESS_REQUEST_OK_SUBMETHOD)
    result.ticket = await s.readBEU16()
  else:
    raise newException(ValueError, "Wrong MethodID: " & $t)


proc encode*(meth: AMQPAccessMethod, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    await dst.encodeShortString(meth.realm)
    await dst.write(cast[uint8](meth.requestFlags))
  of AMQP_ACCESS_REQUEST_OK_SUBMETHOD:
    await dst.writeBE(meth.ticket)
  else:
    raise newException(ValueError, "Wrong MethodID: " & $meth.kind)


proc newAccessRequest*(realm: string, exclusive, passive, active, write, read: bool): AMQPAccessMethod =
  result = AMQPAccessMethod(
    kind: AMQP_ACCESS_REQUEST_SUBMETHOD, 
    realm: realm,
    requestFlags: AMQPAccessRequestBits(
      exclusive: exclusive,
      passive: passive,
      active: active,
      write: write,
      read: read
    )
  )


proc newAccessRequestOk*(ticket: uint16): AMQPAccessMethod =
  result = AMQPAccessMethod(
    kind: AMQP_ACCESS_REQUEST_OK_SUBMETHOD, 
    ticket: ticket
  )


proc exclusive*(self: AMQPAccessMethod): bool =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result = self.requestFlags.exclusive
  else:
    raise newException(FieldDefect, "No such field")


proc `exclusive=`*(self: AMQPAccessMethod, exclusive: bool) =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    self.requestFlags.exclusive = exclusive
  else:
    raise newException(FieldDefect, "No such field")


proc passive*(self: AMQPAccessMethod): bool =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result = self.requestFlags.passive
  else:
    raise newException(FieldDefect, "No such field")


proc `passive=`*(self: AMQPAccessMethod, passive: bool) =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    self.requestFlags.passive = passive
  else:
    raise newException(FieldDefect, "No such field")


proc active*(self: AMQPAccessMethod): bool =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result = self.requestFlags.active
  else:
    raise newException(FieldDefect, "No such field")


proc `active=`*(self: AMQPAccessMethod, active: bool) =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    self.requestFlags.active = active
  else:
    raise newException(FieldDefect, "No such field")


proc write*(self: AMQPAccessMethod): bool =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result = self.requestFlags.write
  else:
    raise newException(FieldDefect, "No such field")


proc `write=`*(self: AMQPAccessMethod, write: bool) =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    self.requestFlags.write = write
  else:
    raise newException(FieldDefect, "No such field")


proc read*(self: AMQPAccessMethod): bool =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    result = self.requestFlags.read
  else:
    raise newException(FieldDefect, "No such field")


proc `read=`*(self: AMQPAccessMethod, read: bool) =
  case self.kind
  of AMQP_ACCESS_REQUEST_SUBMETHOD:
    self.requestFlags.read = read
  else:
    raise newException(FieldDefect, "No such field")
