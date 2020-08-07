import streams
import tables
import ./mthd
import ../data

type BasicQos* = ref object of Method
  prefetchSize: uint32
  prefetchCount: uint16
  globalQos: bool

proc newBasicQos*(prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false): BasicQos =
  result.new
  result.initMethod(true, 0x003C000A)
  result.prefetchSize = prefetchSize
  result.prefetchCount = prefetchCount

method decode*(self: BasicQos, encoded: Stream): BasicQos =
  self.prefetchSize = encoded.readBigEndianU32()
  self.prefetchCount = encoded.readBigEndianU16()
  let bbuf = encoded.readBigEndianU8()
  self.globalQos = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicQos): string =
  var s = newStringStream("")
  s.writeBigEndian32(self.prefetchSize)
  s.writeBigEndian16(self.prefetchCount)
  let bbuf: uint8 = (if self.globalQos: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicQosOk* = ref object of Method

proc newBasicQosOk*(): BasicQosOk =
  result.new
  result.initMethod(false, 0x003C000B)

type BasicConsume* = ref object of Method
  ticket: uint16
  queue: string
  consumerTag: string
  noLocal: bool
  noAck: bool
  exclusive: bool
  noWait: bool
  arguments: TableRef[string, DataTable]

proc newBasicConsume*(
  ticket=0.uint16, 
  queue="", 
  consumerTag="", 
  noLocal=false, 
  noAck=false, 
  exclusive=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): BasicConsume =
  result.new
  result.initMethod(true, 0x003C0014)
  result.ticket = ticket
  result.queue = queue
  result.consumerTag = consumerTag
  result.noLocal = noLocal
  result.noAck = noAck
  result.exclusive = exclusive
  result.noWait = noWait
  result.arguments = arguments

method decode*(self: BasicConsume, encoded: Stream): BasicConsume =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  self.consumerTag = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noLocal = (bbuf and 0x01) != 0
  self.noAck = (bbuf and 0x02) != 0
  self.exclusive = (bbuf and 0x04) != 0
  self.noWait =  (bbuf and 0x08) != 0
  self.arguments = encoded.decodeTable()

method encode*(self: BasicConsume): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  s.writeShortString(self.consumerTag)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.noLocal: 0x01 else: 0x00) or 
  (if self.noAck: 0x02 else: 0x00) or 
  (if self.exclusive: 0x04 else: 0x00) or 
  (if self.noWait: 0x08 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type BasicConsumeOk* = ref object of Method
  consumerTag: string

proc newBasicConsumeOk*(consumerTag=""): BasicConsumeOk =
  result.new
  result.initMethod(false, 0x003C0015)
  result.consumerTag = consumerTag

method decode*(self: BasicConsumeOk, encoded: Stream): BasicConsumeOk =
  self.consumerTag = encoded.readShortString()
  return self

method encode*(self: BasicConsumeOk): string =
  var s = newStringStream("")
  s.writeShortString(self.consumerTag)
  result = s.readAll()
  s.close()

type BasicCancel* = ref object of Method
  consumerTag: string
  noWait: bool

proc newBasicCancel*(consumerTag="", noWait=false): BasicCancel =
  result.new
  result.initMethod(true, 0x003C001E)
  result.consumerTag = consumerTag
  result.noWait = noWait

method decode*(self: BasicCancel, encoded: Stream): BasicCancel =
  self.consumerTag = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noWait = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicCancel): string =
  var s = newStringStream("")
  s.writeShortString(self.consumerTag)
  let bbuf = (if self.noWait: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicCancelOk* = ref object of Method
  consumerTag: string

proc newBasicCancelOk*(consumerTag=""): BasicCancelOk =
  result.new
  result.initMethod(false, 0x003C001F)
  result.consumerTag = consumerTag

method decode*(self: BasicCancelOk, encoded: Stream): BasicCancelOk =
  self.consumerTag = encoded.readShortString()
  return self

method encode*(self: BasicCancelOk): string =
  var s = newStringStream("")
  s.writeShortString(self.consumerTag)
  result = s.readAll()
  s.close()

type BasicPublish* = ref object of Method
  ticket: uint16
  exchange: string
  routingKey: string
  mandatory: bool
  immediate: bool

proc newBasicPublish*(ticket=0.uint16, exchange="", routingKey="", mandatory=false, immediate=false): BasicPublish =
  result.new
  result.initMethod(false, 0x003C0028)
  result.ticket = ticket
  result.exchange = exchange
  result.routingKey = routingKey
  result.mandatory = mandatory
  result.immediate = immediate

method decode*(self: BasicPublish, encoded: Stream): BasicPublish =
  self.ticket = encoded.readBigEndianU16()
  self.exchange = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.mandatory = (bbuf and 0x01) != 0
  self.immediate = (bbuf and 0x02) != 0
  return self

method encode*(self: BasicPublish): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.exchange)
  s.writeShortString(self.routingKey)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.mandatory: 0x01 else: 0x00) or 
  (if self.immediate: 0x02 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicReturn* = ref object of Method
  replyCode: uint16
  replyText: string
  exchange: string
  routingKey: string

proc newBasicReturn*(replyCode=0.uint16, replyText="", exchange="", routingKey=""): BasicReturn =
  result.new
  result.initMethod(false, 0x003C0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.exchange = exchange
  result.routingKey = routingKey

method encode*(self: BasicReturn, encoded: Stream): BasicReturn =
  self.replyCode = encoded.readBigEndianU16()
  self.replyText = encoded.readShortString()
  self.exchange = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  return self

method decode*(self: BasicReturn): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.replyCode)
  s.writeShortString(self.replyText)
  s.writeShortString(self.exchange)
  s.writeShortString(self.routingKey)
  result = s.readAll()
  s.close()

type BasicDeliver* = ref object of Method
  consumerTag: string
  deliveryTag: uint64
  redelivered: bool
  exchange: string
  routingKey: string

proc newBasicDeliver*(consumerTag="", deliveryTag=0.uint64, redelivered=false, exchange="", routingKey=""): BasicDeliver =
  result.new
  result.initMethod(false, 0x003C003C)
  result.consumerTag = consumerTag
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey

method decode*(self: BasicDeliver, encoded: Stream): BasicDeliver =
  self.consumerTag = encoded.readShortString()
  self.deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  self.redelivered = (bbuf and 0x01) != 0
  self.exchange = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  return self

method encode*(self: BasicDeliver): string =
  var s = newStringStream("")
  s.writeShortString(self.consumerTag)
  s.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  s.writeShortString(self.exchange)
  s.writeShortString(self.routingKey)
  result = s.readAll()
  s.close()

type BasicGet* = ref object of Method
  ticket: uint16
  queue: string
  noAck: bool

proc newBasicGet*(ticket=0.uint16, queue="", noAck=false): BasicGet =
  result.new
  result.initMethod(true, 0x003C0046)
  result.ticket = ticket
  result.queue = queue
  result.noAck = noAck

method decode*(self: BasicGet, encoded: Stream): BasicGet =
  self.ticket = encoded.readBigEndianU16()
  self.queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noAck = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicGet): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.queue)
  let bbuf = (if self.noAck: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicGetOk* = ref object of Method
  deliveryTag: uint64
  redelivered: bool
  exchange: string
  routingKey: string
  messageCount: uint32

proc newBasicGetOk*(deliveryTag=0.uint64, redelivered=false, exchange="", routingKey="", messageCount=0.uint32): BasicGetOk =
  result.new
  result.initMethod(false, 0x003C0047)
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey
  result.messageCount = messageCount

method decode*(self: BasicGetOk, encoded: Stream): BasicGetOk =
  self.deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  self.redelivered = (bbuf and 0x01) != 0
  self.exchange = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  self.messageCount = encoded.readBigEndianU32()
  return self

method encode*(self: BasicGetOk): string =
  var s = newStringStream("")
  s.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  s.writeShortString(self.exchange)
  s.writeShortString(self.routingKey)
  s.writeBigEndian32(self.messageCount)
  result = s.readAll()
  s.close()

type BasicGetEmpty* = ref object of Method
  clusterId: string

proc newBasicGetEmpty*(clusterId=""): BasicGetEmpty =
  result.new
  result.initMethod(false, 0x003C0048)
  result.clusterId = clusterId

method decode*(self: BasicGetEmpty, encoded: Stream): BasicGetEmpty =
  self.clusterId = encoded.readShortString()
  return self

method encode*(self: BasicGetEmpty): string =
  var s = newStringStream("")
  s.writeShortString(self.clusterId)
  result = s.readAll()
  s.close()

type BasicAck* = ref object of Method
  deliveryTag: uint64
  multiple: bool

proc newBasicAck*(deliveryTag=0.uint64, multiple=false): BasicAck =
  result.new
  result.initMethod(false, 0x003C0050)
  result.deliveryTag = deliveryTag
  result.multiple = multiple

method decode*(self: BasicAck, encoded: Stream): BasicAck =
  self.deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  self.multiple = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicAck): string =
  var s = newStringStream("")
  s.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.multiple: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicReject* = ref object of Method
  deliveryTag: uint64
  requeue: bool

proc newBasicReject*(deliveryTag=0.uint64, requeue=false): BasicReject =
  result.new
  result.initMethod(false, 0x003C005A)
  result.deliveryTag = deliveryTag
  result.requeue = requeue

method decode*(self: BasicReject, encoded: Stream): BasicReject =
  self.deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  self.requeue = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicReject): string =
  var s = newStringStream("")
  s.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicRecoverAsync* = ref object of Method
  requeue: bool

proc newBasicRecoverAsync*(requeue=false): BasicRecoverAsync =
  result.new
  result.initMethod(false, 0x003C0064)
  result.requeue = requeue

method decode*(self: BasicRecoverAsync, encoded: Stream): BasicRecoverAsync =
  let bbuf = encoded.readBigEndianU8()
  self.requeue = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicRecoverAsync): string =
  var s = newStringStream("")
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicRecover* = ref object of Method
  requeue: bool

proc newBasicRecover*(requeue=false): BasicRecover =
  result.new
  result.initMethod(true, 0x003C006E)
  result.requeue = requeue

method decode*(self: BasicRecover, encoded: Stream): BasicRecover =
  let bbuf = encoded.readBigEndianU8()
  self.requeue = (bbuf and 0x01) != 0
  return self

method encode*(self: BasicRecover): string =
  var s = newStringStream("")
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type BasicRecoverOk* = ref object of Method

proc newBasicRecoverOk*(): BasicRecoverOk =
  result.new
  result.initMethod(false, 0x003C006F)

type BasicNack* = ref object of Method
  deliveryTag: uint64
  multiple: bool
  requeue: bool

proc newBasicNack*(deliveryTag=0.uint64, multiple=false, requeue=false): BasicNack =
  result.new
  result.initMethod(false, 0x003C0078)
  result.deliveryTag = deliveryTag
  result.multiple = multiple
  result.requeue = requeue

method decode*(self: BasicNack, encoded: Stream): BasicNack =
  self.deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  self.multiple = (bbuf and 0x01) != 0
  self.requeue = (bbuf and 0x02) != 0
  return self

method encode*(self: BasicNack): string =
  var s = newStringStream("")
  s.writeBigEndian64(self.deliveryTag)
  let bbuf = 0x00.uint8 or
    (if self.multiple: 0x01.uint8 else: 0x00.uint8) or
    (if self.requeue: 0x02.uint8 else: 0x00.uint8)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()
