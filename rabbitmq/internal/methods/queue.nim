import asyncdispatch
import faststreams/[inputs, outputs]
import tables
import ./mthd
import ../data

type 
  QueueDeclare* = ref object of Method
    ticket: uint16
    queue: string
    passive: bool
    durable: bool
    exclusive: bool
    autoDelete: bool
    noWait: bool
    arguments: TableRef[string, DataTable]
  QueueDeclareOk* = ref object of Method
    queue: string
    messageCount: uint32
    consumerCount: uint32
  QueueBind* = ref object of Method
    ticket: uint16
    queue: string
    bQueue: string
    routingKey: string
    noWait: bool
    arguments: TableRef[string, DataTable]
  QueueBindOk* = ref object of Method
  QueuePurge* = ref object of Method
    ticket: uint16
    queue: string
    noWait: bool
  QueuePurgeOk* = ref object of Method
    messageCount: uint32
  QueueDelete* = ref object of Method
    ticket: uint16
    queue: string
    ifUnused: bool
    ifEmpty: bool
    noWait: bool
  QueueDeleteOk* = ref object of Method
    messageCount: uint32
  QueueUnbind* = ref object of Method
    ticket: uint16
    queue: string
    bQueue: string
    routingKey: string
    arguments: TableRef[string, DataTable]
  QueueUnbindOk* = ref object of Method

#--------------- Queue.Declare ---------------#

proc newQueueDeclare*(
  ticket = 0.uint16, 
  queue="", 
  passive=false, 
  durable=false, 
  exclusive=false,
  autoDelete=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable]=nil): QueueDeclare =
  result.new
  result.initMethod(true, 0x0032000A)
  result.ticket = ticket
  result.queue = queue
  result.passive = passive
  result.durable = durable
  result.exclusive = exclusive
  result.autoDelete = autoDelete
  result.noWait = noWait
  result.arguments = arguments

proc decode*(_: type[QueueDeclare], encoded: AsyncInputStream): Future[QueueDeclare] {.async.} =
  let (_, ticket) = await encoded.readBigEndianU16()
  let (_, queue) = await encoded.readShortString()
  let (_, bbuf) = await encoded.readBigEndianU8()
  let (_, arguments) = await encoded.decodeTable()
  let passive = (bbuf and 0x01) != 0
  let durable = (bbuf and 0x02) != 0
  let exclusive = (bbuf and 0x04) != 0
  let autoDelete = (bbuf and 0x08) != 0
  let noWait = (bbuf and 0x10) != 0
  result = newQueueDeclare(ticket, queue, passive, durable, exclusive, autoDelete, noWait, arguments)

proc encode*(self: QueueDeclare, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.passive: 0x01 else: 0x00) or 
    (if self.durable: 0x02 else: 0x00) or 
    (if self.exclusive: 0x04 else: 0x00) or 
    (if self.autoDelete: 0x08 else: 0x00) or 
    (if self.noWait: 0x10 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeBigEndian8(bbuf)
  discard await to.encodeTable(self.arguments)

#--------------- Queue.DeclareOk ---------------#

proc newQueueDeclareOk*(queue="", messageCount=0.uint32, consumerCount=0.uint32): QueueDeclareOk =
  result.new
  result.initMethod(false, 0x0032000B)
  result.queue = queue
  result.messageCount = messageCount
  result.consumerCount = consumerCount

proc decode*(_: type[QueueDeclareOk], encoded: AsyncInputStream): Future[QueueDeclareOk] {.async.} =
  let (_, queue) = await encoded.readShortString()
  let (_, messageCount) = await encoded.readBigEndianU32()
  let (_, consumerCount) = await encoded.readBigEndianU32()
  result = newQueueDeclareOk(queue, messageCount, consumerCount)

proc encode*(self: QueueDeclareOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.queue)
  discard await to.writeBigEndian32(self.messageCount)
  discard await to.writeBigEndian32(self.consumerCount)

#--------------- Queue.Bind ---------------#

proc newQueueBind*(
  ticket = 0.uint16, 
  queue = "", 
  bQueue = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): QueueBind =
  result.new
  result.initMethod(true, 0x00320014)
  result.queue = queue
  result.bQueue = bQueue
  result.routingKey = routingKey
  result.noWait = noWait
  result.arguments = arguments

proc decode*(_: type[QueueBind], encoded: AsyncInputStream): Future[QueueBind] {.async.} =
  let (_, ticket) = await encoded.readBigEndianU16()
  let (_, queue) = await encoded.readShortString()
  let (_, bQueue) = await encoded.readShortString()
  let (_, routingKey) = await encoded.readShortString()
  let (_, bbuf) = await encoded.readBigEndianU8()
  let (_, arguments) = await encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newQueueBind(ticket, queue, bQueue, routingKey, noWait, arguments)

proc encode*(self: QueueBind, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeShortString(self.bQueue)
  discard await to.writeShortString(self.routingKey)
  discard await to.writeBigEndian8(bbuf)
  discard await to.encodeTable(self.arguments)

#--------------- Queue.BindOk ---------------#

proc newQueueBindOk*(): QueueBindOk =
  result.new
  result.initMethod(false, 0x00320015)

proc decode*(_: type[QueueBindOk], encoded: AsyncInputStream): Future[QueueBindOk] {.async.} = newQueueBindOk()

proc encode*(self: QueueBindOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Queue.Purge ---------------#

proc newQueuePurge*(ticket = 0.uint16, queue="", noWait=false): QueuePurge =
  result.new
  result.initMethod(true, 0x0032001E)
  result.ticket = ticket
  result.queue = queue
  result.noWait = noWait

proc decode*(_: type[QueuePurge], encoded: AsyncInputStream): Future[QueuePurge] {.async.} =
  let (_, ticket) = await encoded.readBigEndianU16()
  let (_, queue) = await encoded.readShortString()
  let (_, bbuf) = await encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newQueuePurge(ticket, queue, noWait)

proc encode*(self: QueuePurge, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeBigEndian8(bbuf)

#--------------- Queue.PurgeOk ---------------#

proc newQueuePurgeOk*(messageCount = 0.uint32): QueuePurgeOk =
  result.new
  result.initMethod(false, 0x0032001F)
  result.messageCount = messageCount

proc decode*(_: type[QueuePurgeOk], encoded: AsyncInputStream): Future[QueuePurgeOk] {.async.} =
  let (_, messageCount) = await encoded.readBigEndianU32()
  result = newQueuePurgeOk(messageCount)

proc encode*(self: QueuePurgeOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian32(self.messageCount)

#--------------- Queue.Delete ---------------#

proc newQueueDelete*(ticket = 0.uint16, queue="", ifUnused=false, ifEmpty=false, noWait=false): QueueDelete =
  result.new
  result.initMethod(true, 0x00320028)
  result.ticket = ticket
  result.queue = queue
  result.ifUnused = ifUnused
  result.ifEmpty = ifEmpty
  result.noWait = noWait

proc decode*(_: type[QueueDelete], encoded: AsyncInputStream): Future[QueueDelete] {.async.} =
  let (_, ticket) = await encoded.readBigEndianU16()
  let (_, queue) = await encoded.readShortString()
  let (_, bbuf) = await encoded.readBigEndianU8()
  let ifUnused = (bbuf and 0x01) != 0
  let ifEmpty = (bbuf and 0x02) != 0
  let noWait = (bbuf and 0x04) != 0
  result = newQueueDelete(ticket, queue, ifUnused, ifEmpty, noWait)

proc encode*(self: QueueDelete, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.ifUnused: 0x01 else: 0x00) or 
    (if self.ifEmpty: 0x02 else: 0x00) or 
    (if self.noWait: 0x04 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeBigEndian8(bbuf)

#--------------- Queue.DeleteOk ---------------#

proc newQueueDeleteOk*(messageCount = 0.uint32): QueueDeleteOk =
  result.new
  result.initMethod(false, 0x00320029)
  result.messageCount = messageCount

proc decode*(_: type[QueueDeleteOk], encoded: AsyncInputStream): Future[QueueDeleteOk] {.async.} =
  let (_, messageCount) = await encoded.readBigEndianU32()
  result = newQueueDeleteOk(messageCount)

proc encode*(self: QueueDeleteOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian32(self.messageCount)

#--------------- Queue.Unbind ---------------#

proc newQueueUnbind*(
  ticket = 0.uint16, 
  queue = "", 
  bQueue = "", 
  routingKey = "", 
  arguments: TableRef[string, DataTable] = nil): QueueUnbind =
  result.new
  result.initMethod(true, 0x00320032)
  result.queue = queue
  result.bQueue = bQueue
  result.routingKey = routingKey
  result.arguments = arguments

proc decode*(_: type[QueueUnbind], encoded: AsyncInputStream): Future[QueueUnbind] {.async.} =
  let (_, ticket) = await encoded.readBigEndianU16()
  let (_, queue) = await encoded.readShortString()
  let (_, bQueue) = await encoded.readShortString()
  let (_, routingKey) = await encoded.readShortString()
  let (_, arguments) = await encoded.decodeTable()
  result = newQueueUnbind(ticket, queue, bQueue, routingKey, arguments)

proc encode*(self: QueueUnbind, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeShortString(self.bQueue)
  discard await to.writeShortString(self.routingKey)
  discard await to.encodeTable(self.arguments)

#--------------- Queue.UnbindOk ---------------#

proc newQueueUnbindOk*(): QueueUnbindOk =
  result.new
  result.initMethod(false, 0x00320033)

proc decode*(_: type[QueueUnbindOk], encoded: AsyncInputStream): Future[QueueUnbindOk] {.async.} = newQueueUnbindOk()

proc encode*(self: QueueUnbindOk, to: AsyncOutputStream) {.async.} = discard
