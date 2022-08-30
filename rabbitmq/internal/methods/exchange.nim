import std/[asyncdispatch, tables, strutils]
import pkg/networkutils/buffered_socket
import ../field
import ../exceptions

const EXCHANGE_METHODS* = 0x0028.uint16
const EXCHANGE_DECLARE_METHOD_ID* = 0x0028000A.uint32
const EXCHANGE_DECLARE_OK_METHOD_ID* = 0x0028000B.uint32
const EXCHANGE_DELETE_METHOD_ID* = 0x00280014.uint32
const EXCHANGE_DELETE_OK_METHOD_ID* = 0x00280015.uint32
const EXCHANGE_BIND_METHOD_ID* = 0x0028001E.uint32
const EXCHANGE_BIND_OK_METHOD_ID* = 0x0028001F.uint32
const EXCHANGE_UNBIND_METHOD_ID* = 0x00280028.uint32
const EXCHANGE_UNBIND_OK_METHOD_ID* = 0x00280033.uint32

type
  AMQPExchangeKind = enum
    AMQP_EXCHANGE_NONE = 0
    AMQP_EXCHANGE_DECLARE_SUBMETHOD = (EXCHANGE_DECLARE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD = (EXCHANGE_DECLARE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_DELETE_SUBMETHOD = (EXCHANGE_DELETE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_DELETE_OK_SUBMETHOD = (EXCHANGE_DELETE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_BIND_SUBMETHOD = (EXCHANGE_BIND_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_BIND_OK_SUBMETHOD = (EXCHANGE_BIND_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_UNBIND_SUBMETHOD = (EXCHANGE_UNBIND_METHOD_ID and 0x0000FFFF).uint16
    AMQP_EXCHANGE_UNBIND_OK_SUBMETHOD = (EXCHANGE_UNBIND_OK_METHOD_ID and 0x0000FFFF).uint16

  AMQPExchangeType* = enum
    EXCHANGE_DIRECT = "direct"
    EXCHANGE_FANOUT = "fanout"
    EXCHANGE_TOPIC = "topic"
    EXCHANGE_HEADERS = "headers"
  
  AMQPExchangeDeclareBits = object
    passive {.bitsize: 1.}: bool
    durable {.bitsize: 1.}: bool
    autoDelete {.bitsize: 1.}: bool
    internal {.bitsize: 1.}: bool
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 3.}: uint8

  AMQPExchangeDeleteBits = object
    ifUnused {.bitsize: 1.}: bool
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 6.}: uint8

  AMQPExchangeBindUnbindBits = object
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPExchangeDeclareObj = object of RootObj
    exchange: string
    eType: AMQPExchangeType
    flags: AMQPExchangeDeclareBits
    args: FieldTable
  
  AMQPExchangeDeleteObj = object of RootObj
    exchange: string
    flags: AMQPExchangeDeleteBits

  AMQPExchangeBindUnbindObj = object of RootObj
    destination: string
    source: string
    routingKey: string
    flags: AMQPExchangeBindUnbindBits
    args: FieldTable

  AMQPExchange* = ref AMQPExchangeObj
  AMQPExchangeObj* = object of RootObj
    case kind*: AMQPExchangeKind
    of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
      declare: AMQPExchangeDeclareObj
    of AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD, 
      AMQP_EXCHANGE_DELETE_OK_SUBMETHOD, 
      AMQP_EXCHANGE_BIND_OK_SUBMETHOD, 
      AMQP_EXCHANGE_UNBIND_OK_SUBMETHOD:
      discard
    of AMQP_EXCHANGE_DELETE_SUBMETHOD:
      del: AMQPExchangeDeleteObj
    of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
      bindUnbind: AMQPExchangeBindUnbindObj
    else:
      discard

proc len*(meth: AMQPExchange): int =
  result = 0
  case meth.kind:
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.declare.exchange.shortStringLen)
    result.inc(($meth.declare.eType).shortStringLen)
    result.inc(sizeInt8Uint8)
    result.inc(meth.declare.args.len)
  of AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD, 
    AMQP_EXCHANGE_DELETE_OK_SUBMETHOD, 
    AMQP_EXCHANGE_BIND_OK_SUBMETHOD, 
    AMQP_EXCHANGE_UNBIND_OK_SUBMETHOD:
    result.inc(0)
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.del.exchange.shortStringLen)
    result.inc(sizeInt8Uint8)
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.bindUnbind.destination.shortStringLen)
    result.inc(meth.bindUnbind.source.shortStringLen)
    result.inc(meth.bindUnbind.routingKey.shortStringLen)
    result.inc(sizeInt8Uint8)
    result.inc(meth.bindUnbind.args.len)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")


proc decode*(_: typedesc[AMQPExchange], s: AsyncBufferedSocket, t: uint32): Future[AMQPExchange] {.async.} =
  case t:
  of EXCHANGE_DECLARE_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_DECLARE_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.declare.exchange = await s.decodeShortString()
    result.declare.eType = parseEnum[AMQPExchangeType]((await s.decodeShortString()).toLowerAscii())
    result.declare.flags = cast[AMQPExchangeDeclareBits](await s.readU8())
    result.declare.args = await s.decodeTable()
  of EXCHANGE_DECLARE_OK_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD)
  of EXCHANGE_DELETE_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_DELETE_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.del.exchange = await s.decodeShortString()
    result.del.flags = cast[AMQPExchangeDeleteBits](await s.readU8())
  of EXCHANGE_DELETE_OK_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_DELETE_OK_SUBMETHOD)
  of EXCHANGE_BIND_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_BIND_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.bindUnbind.destination = await s.decodeShortString()
    result.bindUnbind.source = await s.decodeShortString()
    result.bindUnbind.routingKey = await s.decodeShortString()
    result.bindUnbind.flags = cast[AMQPExchangeBindUnbindBits](await s.readU8())
    result.bindUnbind.args = await s.decodeTable()
  of EXCHANGE_BIND_OK_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_BIND_OK_SUBMETHOD)
  of EXCHANGE_UNBIND_METHOD_ID:
    result = AMQPExchange(kind: AMQP_EXCHANGE_UNBIND_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.bindUnbind.destination = await s.decodeShortString()
    result.bindUnbind.source = await s.decodeShortString()
    result.bindUnbind.routingKey = await s.decodeShortString()
    result.bindUnbind.flags = cast[AMQPExchangeBindUnbindBits](await s.readU8())
    result.bindUnbind.args = await s.decodeTable()
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPExchange, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.declare.exchange)
    await dst.encodeShortString($meth.declare.eType)
    await dst.write(cast[uint8](meth.declare.flags))
    await dst.encodeTable(meth.declare.args)
  of AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD, 
    AMQP_EXCHANGE_DELETE_OK_SUBMETHOD, 
    AMQP_EXCHANGE_BIND_OK_SUBMETHOD, 
    AMQP_EXCHANGE_UNBIND_OK_SUBMETHOD:
    discard
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.del.exchange)
    await dst.write(cast[uint8](meth.del.flags))
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.bindUnbind.destination)
    await dst.encodeShortString(meth.bindUnbind.source)
    await dst.encodeShortString(meth.bindUnbind.routingKey)
    await dst.write(cast[uint8](meth.bindUnbind.flags))
    await dst.encodeTable(meth.bindUnbind.args)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc newExchangeDeclare*(
  exchange: string, eType: AMQPExchangeType, 
  passive, durable, autoDelete, internal, noWait: bool, 
  args: FieldTable): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_DECLARE_SUBMETHOD,
    declare: AMQPExchangeDeclareObj(
      exchange: exchange,
      eType: eType,
      flags: AMQPExchangeDeclareBits(
        passive: passive,
        durable: durable,
        autoDelete: autoDelete,
        internal: internal,
        noWait: noWait
      ),
      args: args
    )
  )

proc newExchangeDeclareOk*(): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_DECLARE_OK_SUBMETHOD
  )

proc newExchangeDelete*(exchange: string, ifUnused, noWait: bool): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_DELETE_SUBMETHOD,
    del: AMQPExchangeDeleteObj(
      exchange: exchange,
      flags: AMQPExchangeDeleteBits(
        ifUnused: ifUnused,
        noWait: noWait
      )
    )
  )

proc newExchangeDeleteOk*(): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_DELETE_OK_SUBMETHOD
  )

proc newExchangeBind*(destination, source, routingKey: string, noWait=false, args: FieldTable): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_BIND_SUBMETHOD,
    bindUnbind: AMQPExchangeBindUnbindObj(
      destination: destination,
      source: source,
      routingKey: routingKey,
      flags: AMQPExchangeBindUnbindBits(
        noWait: noWait
      ),
      args: args
    )
  )

proc newExchangeBindOk*(): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_BIND_OK_SUBMETHOD
  )

proc newExchangeUnbind*(destination, source, routingKey: string, noWait=false, args: FieldTable): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_UNBIND_SUBMETHOD,
    bindUnbind: AMQPExchangeBindUnbindObj(
      destination: destination,
      source: source,
      routingKey: routingKey,
      flags: AMQPExchangeBindUnbindBits(
        noWait: noWait
      ),
      args: args
    )
  )

proc newExchangeUnbindOk*(): AMQPExchange =
  result = AMQPExchange(
    kind: AMQP_EXCHANGE_UNBIND_OK_SUBMETHOD
  )

proc exchange*(self: AMQPExchange): string =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.exchange
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    result = self.del.exchange
  else:
    raise newException(FieldDefect, "No such field")

proc eType*(self: AMQPExchange): AMQPExchangeType =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.eType
  else:
    raise newException(FieldDefect, "No such field")

proc args*(self: AMQPExchange): FieldTable =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.args
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result = self.bindUnbind.args
  else:
    raise newException(FieldDefect, "No such field")

proc destination*(self: AMQPExchange): string =
  case self.kind
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result = self.bindUnbind.destination
  else:
    raise newException(FieldDefect, "No such field")

proc source*(self: AMQPExchange): string =
  case self.kind
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result = self.bindUnbind.source
  else:
    raise newException(FieldDefect, "No such field")

proc routingKey*(self: AMQPExchange): string =
  case self.kind
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result = self.bindUnbind.routingKey
  else:
    raise newException(FieldDefect, "No such field")

proc passive*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.flags.passive
  else:
    raise newException(FieldDefect, "No such field")

proc `passive=`*(self: AMQPExchange, passive: bool) =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    self.declare.flags.passive = passive
  else:
    raise newException(FieldDefect, "No such field")

proc durable*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.flags.durable
  else:
    raise newException(FieldDefect, "No such field")

proc `durable=`*(self: AMQPExchange, durable: bool) =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    self.declare.flags.durable = durable
  else:
    raise newException(FieldDefect, "No such field")

proc autoDelete*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.flags.autoDelete
  else:
    raise newException(FieldDefect, "No such field")

proc `autoDelete=`*(self: AMQPExchange, autoDelete: bool) =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    self.declare.flags.autoDelete = autoDelete
  else:
    raise newException(FieldDefect, "No such field")

proc internal*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.flags.internal
  else:
    raise newException(FieldDefect, "No such field")

proc `internal=`*(self: AMQPExchange, internal: bool) =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    self.declare.flags.internal = internal
  else:
    raise newException(FieldDefect, "No such field")

proc ifUnused*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    result = self.del.flags.ifUnused
  else:
    raise newException(FieldDefect, "No such field")

proc `ifUnused=`*(self: AMQPExchange, ifUnused: bool) =
  case self.kind
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    self.del.flags.ifUnused = ifUnused
  else:
    raise newException(FieldDefect, "No such field")

proc noWait*(self: AMQPExchange): bool =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    result = self.declare.flags.noWait
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    result = self.del.flags.noWait
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    result = self.bindUnbind.flags.noWait
  else:
    raise newException(FieldDefect, "No such field")

proc `noWait=`*(self: AMQPExchange, noWait: bool) =
  case self.kind
  of AMQP_EXCHANGE_DECLARE_SUBMETHOD:
    self.declare.flags.noWait = noWait
  of AMQP_EXCHANGE_DELETE_SUBMETHOD:
    self.del.flags.noWait = noWait
  of AMQP_EXCHANGE_BIND_SUBMETHOD, AMQP_EXCHANGE_UNBIND_SUBMETHOD:
    self.bindUnbind.flags.noWait = noWait
  else:
    raise newException(FieldDefect, "No such field")
