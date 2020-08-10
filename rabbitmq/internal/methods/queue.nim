import streams
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
  QueueuBindOk* = ref object of Method
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

proc decode*(_: type[QueueDeclare], encoded: Stream): QueueDeclare =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let passive = (bbuf and 0x01) != 0
  let durable = (bbuf and 0x02) != 0
  let exclusive = (bbuf and 0x04) != 0
  let autoDelete = (bbuf and 0x08) != 0
  let noWait = (bbuf and 0x10) != 0
  let arguments = encoded.decodeTable()
  result = newQueueDeclare(ticket, queue, passive, durable, exclusive, autoDelete, noWait, arguments)

proc encode*(self: QueueDeclare, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.passive: 0x01 else: 0x00) or 
    (if self.durable: 0x02 else: 0x00) or 
    (if self.exclusive: 0x04 else: 0x00) or 
    (if self.autoDelete: 0x08 else: 0x00) or 
    (if self.noWait: 0x10 else: 0x00)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Queue.DeclareOk ---------------#

proc newQueueDeclareOk*(queue="", messageCount=0.uint32, consumerCount=0.uint32): QueueDeclareOk =
  result.new
  result.initMethod(false, 0x0032000B)
  result.queue = queue
  result.messageCount = messageCount
  result.consumerCount = consumerCount

proc decode*(_: type[QueueDeclareOk], encoded: Stream): QueueDeclareOk =
  let queue = encoded.readShortString()
  let messageCount = encoded.readBigEndianU32()
  let consumerCount = encoded.readBigEndianU32()
  result = newQueueDeclareOk(queue, messageCount, consumerCount)

proc encode*(self: QueueDeclareOk, to: Stream) =
  to.writeShortString(self.queue)
  to.writeBigEndian32(self.messageCount)
  to.writeBigEndian32(self.consumerCount)

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

proc decode*(_: type[QueueBind], encoded: Stream): QueueBind =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bQueue = encoded.readShortString()
  let routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  let arguments = encoded.decodeTable()
  result = newQueueBind(ticket, queue, bQueue, routingKey, noWait, arguments)

proc encode*(self: QueueBind, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  to.writeShortString(self.bQueue)
  to.writeShortString(self.routingKey)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Queue.BindOk ---------------#

proc newQueueBindOk*(): QueueuBindOk =
  result.new
  result.initMethod(false, 0x00320015)

proc decode*(_: type[QueueuBindOk], encoded: Stream): QueueuBindOk = newQueueBindOk()

proc encode*(self: QueueuBindOk, to: Stream) = discard

#--------------- Queue.Purge ---------------#

proc newQueuePurge*(ticket = 0.uint16, queue="", noWait=false): QueuePurge =
  result.new
  result.initMethod(true, 0x0032001E)
  result.ticket = ticket
  result.queue = queue
  result.noWait = noWait

proc decode*(_: type[QueuePurge], encoded: Stream): QueuePurge =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newQueuePurge(ticket, queue, noWait)

proc encode*(self: QueuePurge, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Queue.PurgeOk ---------------#

proc newQueuePurgeOk*(messageCount = 0.uint32): QueuePurgeOk =
  result.new
  result.initMethod(false, 0x0032001F)
  result.messageCount = messageCount

proc decode*(_: type[QueuePurgeOk], encoded: Stream): QueuePurgeOk =
  let messageCount = encoded.readBigEndianU32()
  result = newQueuePurgeOk(messageCount)

proc encode*(self: QueuePurgeOk, to: Stream) =
  to.writeBigEndian32(self.messageCount)

#--------------- Queue.Delete ---------------#

proc newQueueDelete*(ticket = 0.uint16, queue="", ifUnused=false, ifEmpty=false, noWait=false): QueueDelete =
  result.new
  result.initMethod(true, 0x00320028)
  result.ticket = ticket
  result.queue = queue
  result.ifUnused = ifUnused
  result.ifEmpty = ifEmpty
  result.noWait = noWait

proc decode*(_: type[QueueDelete], encoded: Stream): QueueDelete =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let ifUnused = (bbuf and 0x01) != 0
  let ifEmpty = (bbuf and 0x02) != 0
  let noWait = (bbuf and 0x04) != 0
  result = newQueueDelete(ticket, queue, ifUnused, ifEmpty, noWait)

proc encode*(self: QueueDelete, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.ifUnused: 0x01 else: 0x00) or 
    (if self.ifEmpty: 0x02 else: 0x00) or 
    (if self.noWait: 0x04 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Queue.DeleteOk ---------------#

proc newQueueDeleteOk*(messageCount = 0.uint32): QueueDeleteOk =
  result.new
  result.initMethod(false, 0x00320029)
  result.messageCount = messageCount

proc decode*(_: type[QueueDeleteOk], encoded: Stream): QueueDeleteOk =
  let messageCount = encoded.readBigEndianU32()
  result = newQueueDeleteOk(messageCount)

proc encode*(self: QueueDeleteOk, to: Stream) =
  to.writeBigEndian32(self.messageCount)

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

proc decode*(_: type[QueueUnbind], encoded: Stream): QueueUnbind =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bQueue = encoded.readShortString()
  let routingKey = encoded.readShortString()
  let arguments = encoded.decodeTable()
  result = newQueueUnbind(ticket, queue, bQueue, routingKey, arguments)

proc encode*(self: QueueUnbind, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  to.writeShortString(self.bQueue)
  to.writeShortString(self.routingKey)
  to.encodeTable(self.arguments)

#--------------- Queue.UnbindOk ---------------#

proc newQueueUnbindOk*(): QueueUnbindOk =
  result.new
  result.initMethod(false, 0x00320033)

proc decode*(_: type[QueueUnbindOk], encoded: Stream): QueueUnbindOk = newQueueUnbindOk()

proc encode*(self: QueueUnbindOk, to: Stream) = discard
