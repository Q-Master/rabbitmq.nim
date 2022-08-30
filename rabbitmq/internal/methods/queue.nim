import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../field
import ../exceptions

const QUEUE_METHODS* = 0x0032.uint16
const QUEUE_DECLARE_METHOD_ID* = 0x0032000A.uint32
const QUEUE_DECLARE_OK_METHOD_ID* = 0x0032000B.uint32
const QUEUE_BIND_METHOD_ID* = 0x00320014.uint32
const QUEUE_BIND_OK_METHOD_ID* = 0x00320015.uint32
const QUEUE_PURGE_METHOD_ID* = 0x0032001E.uint32
const QUEUE_PURGE_OK_METHOD_ID* = 0x0032001F.uint32
const QUEUE_DELETE_METHOD_ID* = 0x00320028.uint32
const QUEUE_DELETE_OK_METHOD_ID* = 0x00320029.uint32
const QUEUE_UNBIND_METHOD_ID* = 0x00320032.uint32
const QUEUE_UNBIND_OK_METHOD_ID* = 0x00320033.uint32

type
  AMQPQueueKind = enum
    AMQP_QUEUE_NONE = 0
    AMQP_QUEUE_DECLARE_SUBMETHOD = (QUEUE_DECLARE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_DECLARE_OK_SUBMETHOD = (QUEUE_DECLARE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_BIND_SUBMETHOD = (QUEUE_BIND_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_BIND_OK_SUBMETHOD = (QUEUE_BIND_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_PURGE_SUBMETHOD = (QUEUE_PURGE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_PURGE_OK_SUBMETHOD = (QUEUE_PURGE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_DELETE_SUBMETHOD = (QUEUE_DELETE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_DELETE_OK_SUBMETHOD = (QUEUE_DELETE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_UNBIND_SUBMETHOD = (QUEUE_UNBIND_METHOD_ID and 0x0000FFFF).uint16
    AMQP_QUEUE_UNBIND_OK_SUBMETHOD = (QUEUE_UNBIND_OK_METHOD_ID and 0x0000FFFF).uint16
  
  AMQPQueueDeclareBits = object
    passive {.bitsize: 1.}: bool
    durable {.bitsize: 1.}: bool
    exclusive {.bitsize: 1.}: bool
    autoDelete {.bitsize: 1.}: bool
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 3.}: uint8

  AMQPQueueBindPurgeBits = object
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPQueueDeleteBits = object
    ifUnused {.bitsize: 1.}: bool
    ifEmpty {.bitsize: 1.}: bool
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 5.}: uint8

  AMQPQueueQueueObj = object of RootObj
    queue: string
  
  AMQPQueueDeclareObj = object of AMQPQueueQueueObj
    flags: AMQPQueueDeclareBits
    args: FieldTable

  AMQPQueueDeclareOkObj = object of AMQPQueueQueueObj
    messageCount: uint32
    consumerCount: uint32

  AMQPQueueUnbindObj = object of AMQPQueueQueueObj
    exchange: string
    routingKey: string
    args: FieldTable

  AMQPQueueBindObj = object of AMQPQueueUnbindObj
    flags: AMQPQueueBindPurgeBits
  
  AMQPQueuePurgeObj = object of AMQPQueueQueueObj
    flags: AMQPQueueBindPurgeBits

  AMQPQueuePurgeOkDeleteOkObj = object of RootObj
    messageCount: uint32
  
  AMQPQueueDeleteObj = object of AMQPQueueQueueObj
    flags: AMQPQueueDeleteBits

  AMQPQueue* = ref AMQPQueueObj
  AMQPQueueObj* = object of RootObj
    case kind*: AMQPQueueKind
    of AMQP_QUEUE_DECLARE_SUBMETHOD:
      declare: AMQPQueueDeclareObj
    of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
      declareOk: AMQPQueueDeclareOkObj
    of AMQP_QUEUE_BIND_SUBMETHOD:
      qBind: AMQPQueueBindObj
    of AMQP_QUEUE_PURGE_SUBMETHOD:
      purge: AMQPQueuePurgeObj
    of AMQP_QUEUE_DELETE_SUBMETHOD:
      del: AMQPQueueDeleteObj
    of AMQP_QUEUE_PURGE_OK_SUBMETHOD, AMQP_QUEUE_DELETE_OK_SUBMETHOD:
      purgeOkDeleteOk: AMQPQueuePurgeOkDeleteOkObj
    of AMQP_QUEUE_UNBIND_SUBMETHOD:
      unbind: AMQPQueueUnbindObj
    of AMQP_QUEUE_BIND_OK_SUBMETHOD, AMQP_QUEUE_UNBIND_OK_SUBMETHOD:
      discard
    else:
      discard

proc len*(meth: AMQPQueue): int =
  result = 0
  case meth.kind:
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.declare.queue.shortStringLen())
    result.inc(sizeInt8Uint8)
    result.inc(meth.declare.args.len)
  of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
    result.inc(meth.declareOk.queue.shortStringLen())
    result.inc(sizeInt32Uint32+sizeInt32Uint32)
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.qBind.queue.shortStringLen())
    result.inc(meth.qBind.exchange.shortStringLen())
    result.inc(meth.qBind.routingKey.shortStringLen())
    result.inc(sizeInt8Uint8)
  of AMQP_QUEUE_PURGE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.purge.queue.shortStringLen())
    result.inc(sizeInt8Uint8)
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.del.queue.shortStringLen())
    result.inc(sizeInt8Uint8)
  of AMQP_QUEUE_PURGE_OK_SUBMETHOD, AMQP_QUEUE_DELETE_OK_SUBMETHOD:
    result.inc(sizeInt32Uint32)
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.unbind.queue.shortStringLen())
    result.inc(meth.unbind.exchange.shortStringLen())
    result.inc(meth.unbind.routingKey.shortStringLen())
  of AMQP_QUEUE_BIND_OK_SUBMETHOD, AMQP_QUEUE_UNBIND_OK_SUBMETHOD:
    result.inc(0)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc decode*(_: typedesc[AMQPQueue], s: AsyncBufferedSocket, t: uint32): Future[AMQPQueue] {.async.} =
  case t:
  of QUEUE_DECLARE_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_DECLARE_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.declare.queue = await s.decodeShortString()
    result.declare.flags = cast[AMQPQueueDeclareBits](await s.readU8())
    result.declare.args = await s.decodeTable()
  of QUEUE_DECLARE_OK_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_DECLARE_OK_SUBMETHOD)
    result.declareOk.queue = await s.decodeShortString()
    result.declareOk.messageCount = await s.readBEU32()
    result.declareOk.consumerCount = await s.readBEU32()
  of QUEUE_BIND_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_BIND_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.qBind.queue = await s.decodeShortString()
    result.qBind.exchange = await s.decodeShortString()
    result.qBind.routingKey = await s.decodeShortString()
    result.qBind.flags = cast[AMQPQueueBindPurgeBits](await s.readU8())
    result.qBind.args = await s.decodeTable()
  of QUEUE_BIND_OK_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_BIND_OK_SUBMETHOD)
  of QUEUE_PURGE_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_PURGE_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.purge.queue = await s.decodeShortString()
    result.purge.flags = cast[AMQPQueueBindPurgeBits](await s.readU8())
  of QUEUE_PURGE_OK_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_PURGE_OK_SUBMETHOD)
    result.declareOk.messageCount = await s.readBEU32()
  of QUEUE_DELETE_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_DELETE_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.del.queue = await s.decodeShortString()
    result.del.flags = cast[AMQPQueueDeleteBits](await s.readU8())
  of QUEUE_DELETE_OK_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_DELETE_OK_SUBMETHOD)
    result.declareOk.messageCount = await s.readBEU32()
  of QUEUE_UNBIND_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_UNBIND_SUBMETHOD)
    let ticket {.used.} = await s.readBEU16()
    result.unbind.queue = await s.decodeShortString()
    result.unbind.exchange = await s.decodeShortString()
    result.unbind.routingKey = await s.decodeShortString()
    result.unbind.args = await s.decodeTable()
  of QUEUE_UNBIND_OK_METHOD_ID:
    result = AMQPQueue(kind: AMQP_QUEUE_UNBIND_OK_SUBMETHOD)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPQueue, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.declare.queue)
    await dst.write(cast[uint8](meth.declare.flags))
    await dst.encodeTable(meth.declare.args)
  of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
    await dst.encodeShortString(meth.declareOk.queue)
    await dst.writeBE(meth.declareOk.messageCount)
    await dst.writeBE(meth.declareOk.consumerCount)
  of AMQP_QUEUE_BIND_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.qBind.queue)
    await dst.encodeShortString(meth.qBind.exchange)
    await dst.encodeShortString(meth.qBind.routingKey)
    await dst.write(cast[uint8](meth.qBind.flags))
    await dst.encodeTable(meth.qBind.args)
  of AMQP_QUEUE_PURGE_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.purge.queue)
    await dst.write(cast[uint8](meth.purge.flags))
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.del.queue)
    await dst.write(cast[uint8](meth.del.flags))
  of AMQP_QUEUE_PURGE_OK_SUBMETHOD, AMQP_QUEUE_DELETE_OK_SUBMETHOD:
    await dst.writeBE(meth.purgeOkDeleteOk.messageCount)
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    await dst.writeBE(0.uint16)
    await dst.encodeShortString(meth.unbind.queue)
    await dst.encodeShortString(meth.unbind.exchange)
    await dst.encodeShortString(meth.unbind.routingKey)
    await dst.encodeTable(meth.unbind.args)
  of AMQP_QUEUE_BIND_OK_SUBMETHOD, AMQP_QUEUE_UNBIND_OK_SUBMETHOD:
    discard
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")


proc newQueueDeclare*(queue: string, 
  passive, durable, exclusive, autoDelete, noWait: bool, 
  args: FieldTable): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_DECLARE_SUBMETHOD, 
    declare: AMQPQueueDeclareObj(
      queue: queue,
      flags: AMQPQueueDeclareBits(
        passive: passive,
        durable: durable,
        exclusive: exclusive,
        autoDelete: autoDelete,
        noWait: noWait
      ),
      args: args
    )
  )

proc newQueueDeclareOk*(queue: string, messageCount, consumerCount: uint32): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_DECLARE_OK_SUBMETHOD, 
    declareOk: AMQPQueueDeclareOkObj(
      queue: queue,
      messageCount: messageCount,
      consumerCount: consumerCount
    )
  )

proc newQueueBind*(queue, exchange, routingKey: string, noWait: bool, args: FieldTable): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_BIND_SUBMETHOD, 
    qBind: AMQPQueueBindObj(
      queue: queue,
      exchange: exchange,
      routingKey: routingKey,
      flags: AMQPQueueBindPurgeBits(
        noWait: noWait
      ),
      args: args
    )
  )

proc newQueueBindOk*(): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_BIND_OK_SUBMETHOD
  )

proc newQueuePurge*(queue: string, noWait: bool): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_PURGE_SUBMETHOD, 
    purge: AMQPQueuePurgeObj(
      queue: queue,
      flags: AMQPQueueBindPurgeBits(
        noWait: noWait
      )
    )
  )

proc newQueuePurgeOk*(messageCount: uint32): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_PURGE_OK_SUBMETHOD, 
    purgeOkDeleteOk: AMQPQueuePurgeOkDeleteOkObj(
      messageCount: messageCount
    )
  )

proc newQueueDelete*(queue: string, ifUnused, ifEmpty, noWait: bool): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_DELETE_SUBMETHOD, 
    del: AMQPQueueDeleteObj(
      queue: queue,
      flags: AMQPQueueDeleteBits(
        ifUnused: ifUnused,
        ifEmpty: ifEmpty,
        noWait: noWait
      )
    )
  )

proc newQueueDeleteOk*(messageCount: uint32): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_DELETE_OK_SUBMETHOD, 
    purgeOkDeleteOk: AMQPQueuePurgeOkDeleteOkObj(
      messageCount: messageCount
    )
  )

proc newQueueUnbind*(queue, exchange, routingKey: string, args: FieldTable): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_UNBIND_SUBMETHOD, 
    unbind: AMQPQueueBindObj(
      queue: queue,
      exchange: exchange,
      routingKey: routingKey,
      args: args
    )
  )

proc newQueueUnbindOk*(): AMQPQueue =
  result = AMQPQueue(
    kind: AMQP_QUEUE_UNBIND_OK_SUBMETHOD
  )

proc queue*(self: AMQPQueue): string =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.queue
  of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
    result = self.declareOk.queue
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result = self.qBind.queue
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    result = self.unbind.queue
  of AMQP_QUEUE_PURGE_SUBMETHOD:
    result = self.purge.queue
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    result = self.del.queue
  else:
    raise newException(FieldDefect, "No such field")

proc args*(self: AMQPQueue): FieldTable =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.args
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result = self.qBind.args
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    result = self.unbind.args
  else:
    raise newException(FieldDefect, "No such field")

proc messageCount*(self: AMQPQueue): uint32 =
  case self.kind
  of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
    result = self.declareOk.messageCount
  of AMQP_QUEUE_PURGE_OK_SUBMETHOD, AMQP_QUEUE_DELETE_OK_SUBMETHOD:
    result = self.purgeOkDeleteOk.messageCount
  else:
    raise newException(FieldDefect, "No such field")

proc exchange*(self: AMQPQueue): string =
  case self.kind
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result = self.qBind.exchange
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    result = self.unbind.exchange
  else:
    raise newException(FieldDefect, "No such field")

proc routingKey*(self: AMQPQueue): string =
  case self.kind
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result = self.qBind.routingKey
  of AMQP_QUEUE_UNBIND_SUBMETHOD:
    result = self.unbind.routingKey
  else:
    raise newException(FieldDefect, "No such field")

proc consumerCount*(self: AMQPQueue): uint32 =
  case self.kind
  of AMQP_QUEUE_DECLARE_OK_SUBMETHOD:
    result = self.declareOk.consumerCount
  else:
    raise newException(FieldDefect, "No such field")

proc passive*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.flags.passive
  else:
    raise newException(FieldDefect, "No such field")

proc `passive=`*(self: AMQPQueue, passive: bool) =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    self.declare.flags.passive = passive
  else:
    raise newException(FieldDefect, "No such field")

proc durable*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.flags.durable
  else:
    raise newException(FieldDefect, "No such field")

proc `durable=`*(self: AMQPQueue, durable: bool) =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    self.declare.flags.durable = durable
  else:
    raise newException(FieldDefect, "No such field")

proc exclusive*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.flags.exclusive
  else:
    raise newException(FieldDefect, "No such field")

proc `exclusive=`*(self: AMQPQueue, exclusive: bool) =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    self.declare.flags.exclusive = exclusive
  else:
    raise newException(FieldDefect, "No such field")

proc autoDelete*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.flags.autoDelete
  else:
    raise newException(FieldDefect, "No such field")

proc `autoDelete=`*(self: AMQPQueue, autoDelete: bool) =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    self.declare.flags.autoDelete = autoDelete
  else:
    raise newException(FieldDefect, "No such field")

proc noWait*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    result = self.declare.flags.noWait
  of AMQP_QUEUE_BIND_SUBMETHOD:
    result = self.qBind.flags.noWait
  of AMQP_QUEUE_PURGE_SUBMETHOD:
    result = self.purge.flags.noWait
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    result = self.del.flags.noWait
  else:
    raise newException(FieldDefect, "No such field")

proc `noWait=`*(self: AMQPQueue, noWait: bool) =
  case self.kind
  of AMQP_QUEUE_DECLARE_SUBMETHOD:
    self.declare.flags.noWait = noWait
  of AMQP_QUEUE_BIND_SUBMETHOD:
    self.qBind.flags.noWait = noWait
  of AMQP_QUEUE_PURGE_SUBMETHOD:
    self.purge.flags.noWait = noWait
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    self.del.flags.noWait = noWait
  else:
    raise newException(FieldDefect, "No such field")

proc ifUnused*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    result = self.del.flags.ifUnused
  else:
    raise newException(FieldDefect, "No such field")

proc `ifUnused=`*(self: AMQPQueue, ifUnused: bool) =
  case self.kind
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    self.del.flags.ifUnused = ifUnused
  else:
    raise newException(FieldDefect, "No such field")

proc ifEmpty*(self: AMQPQueue): bool =
  case self.kind
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    result = self.del.flags.ifEmpty
  else:
    raise newException(FieldDefect, "No such field")

proc `ifEmpty=`*(self: AMQPQueue, ifEmpty: bool) =
  case self.kind
  of AMQP_QUEUE_DELETE_SUBMETHOD:
    self.del.flags.ifEmpty = ifEmpty
  else:
    raise newException(FieldDefect, "No such field")
