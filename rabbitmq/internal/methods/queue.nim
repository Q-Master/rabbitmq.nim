import streams
import tables
import ./mthd
import ../data

type QueueDeclare* = ref object of Method
  ticket: uint16
  queue: string
  passive: bool
  durable: bool
  exclusive: bool
  autoDelete: bool
  noWait: bool
  arguments: TableRef[string, DataTable]

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

method decode*(self: QueueDeclare, encoded: Stream): QueueDeclare =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.passive = (bbuf and 0x01) != 0
  self.durable = (bbuf and 0x02) != 0
  self.exclusive = (bbuf and 0x04) != 0
  self.autoDelete = (bbuf and 0x08) != 0
  self.noWait = (bbuf and 0x10) != 0
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: QueueDeclare): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.passive: 0x01 else: 0x00) or 
  (if self.durable: 0x02 else: 0x00) or 
  (if self.exclusive: 0x04 else: 0x00) or 
  (if self.autoDelete: 0x08 else: 0x00) or 
  (if self.noWait: 0x10 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type QueueDeclareOk* = ref object of Method
  queue: string
  messageCount: uint32
  consumerCount: uint32

proc newQueueDeclareOk*(queue="", messageCount=0.uint32, consumerCount=0.uint32): QueueDeclareOk =
  result.new
  result.initMethod(false, 0x0032000B)
  result.queue = queue
  result.messageCount = messageCount
  result.consumerCount = consumerCount

method decode*(self: QueueDeclareOk, encoded: Stream): QueueDeclareOk =
  self.queue = encoded.readShortString()
  self.messageCount = encoded.readBigEndianU32()
  self.consumerCount = encoded.readBigEndianU32()
  return self

method encode*(self: QueueDeclareOk): string =
  var s = newStringStream("")
  s.writeShortString(self.queue)
  s.writeBigEndian32(self.messageCount)
  s.writeBigEndian32(self.consumerCount)
  result = s.readAll()
  s.close()

type QueueBind* = ref object of Method
  ticket: uint16
  queue: string
  bQueue: string
  routingKey: string
  noWait: bool
  arguments: TableRef[string, DataTable]

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

method decode*(self: QueueBind, encoded: Stream): QueueBind =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  self.bQueue = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noWait = (bbuf and 0x01) != 0
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: QueueBind): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  s.writeShortString(self.bQueue)
  s.writeShortString(self.routingKey)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type QueueuBindOk* = ref object of Method

proc newQueueBindOk*(): QueueuBindOk =
  result.new
  result.initMethod(false, 0x00320015)

type QueuePurge* = ref object of Method
  ticket: uint16
  queue: string
  noWait: bool

proc newQueuePurge*(ticket = 0.uint16, queue="", noWait=false): QueuePurge =
  result.new
  result.initMethod(true, 0x0032001E)
  result.ticket = ticket
  result.queue = queue
  result.noWait = noWait

method decode*(self: QueuePurge, encoded: Stream): QueuePurge =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noWait = (bbuf and 0x01) != 0
  return self

method encode*(self: QueuePurge): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type QueuePurgeOk* = ref object of Method
  messageCount: uint32

proc newQueuePurgeOk*(messageCount = 0.uint32): QueuePurgeOk =
  result.new
  result.initMethod(false, 0x0032001F)
  result.messageCount = messageCount

method decode*(self: QueuePurgeOk, encoded: Stream): QueuePurgeOk =
  self.messageCount = encoded.readBigEndianU32()
  return self

method encode*(self: QueuePurgeOk): string =
  var s = newStringStream("")
  s.writeBigEndian32(self.messageCount)
  result = s.readAll()
  s.close()

type QueueDelete* = ref object of Method
  ticket: uint16
  queue: string
  ifUnused: bool
  ifEmpty: bool
  noWait: bool

proc newQueueDelete*(ticket = 0.uint16, queue="", ifUnused=false, ifEmpty=false, noWait=false): QueueDelete =
  result.new
  result.initMethod(true, 0x00320028)
  result.ticket = ticket
  result.queue = queue
  result.ifUnused = ifUnused
  result.ifEmpty = ifEmpty
  result.noWait = noWait

method decode*(self: QueueDelete, encoded: Stream): QueueDelete =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.ifUnused = (bbuf and 0x01) != 0
  self.ifEmpty = (bbuf and 0x02) != 0
  self.noWait = (bbuf and 0x04) != 0
  return self

method encode*(self: QueueDelete): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.ifUnused: 0x01 else: 0x00) or 
    (if self.ifEmpty: 0x02 else: 0x00) or 
    (if self.noWait: 0x04 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type QueueDeleteOk* = ref object of Method
  messageCount: uint32

proc newQueueDeleteOk*(messageCount = 0.uint32): QueueDeleteOk =
  result.new
  result.initMethod(false, 0x00320029)
  result.messageCount = messageCount

method decode*(self: QueueDeleteOk, encoded: Stream): QueueDeleteOk =
  self.messageCount = encoded.readBigEndianU32()
  return self

method encode*(self: QueueDeleteOk): string =
  var s = newStringStream("")
  s.writeBigEndian32(self.messageCount)
  result = s.readAll()
  s.close()

type QueueUnbind* = ref object of Method
  ticket: uint16
  queue: string
  bQueue: string
  routingKey: string
  arguments: TableRef[string, DataTable]

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

method decode*(self: QueueUnbind, encoded: Stream): QueueUnbind =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  self.bQueue = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: QueueUnbind): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  s.writeShortString(self.bQueue)
  s.writeShortString(self.routingKey)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type QueueUnbindOk* = ref object of Method

proc newQueueUnbindOk*(): QueueUnbindOk =
  result.new
  result.initMethod(false, 0x00320033)
