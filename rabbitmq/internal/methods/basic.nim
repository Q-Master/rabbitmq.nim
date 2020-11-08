import asyncdispatch
import faststreams/[inputs, outputs]
import tables
import ./mthd
import ../data

type 
  BasicQos* = ref object of Method
    prefetchSize: uint32
    prefetchCount: uint16
    globalQos: bool
  BasicQosOk* = ref object of Method
  BasicConsume* = ref object of Method
    ticket: uint16
    queue: string
    consumerTag: string
    noLocal: bool
    noAck: bool
    exclusive: bool
    noWait: bool
    arguments: TableRef[string, DataTable]
  BasicConsumeOk* = ref object of Method
    consumerTag: string
  BasicCancel* = ref object of Method
    consumerTag: string
    noWait: bool
  BasicCancelOk* = ref object of Method
    consumerTag: string
  BasicPublish* = ref object of Method
    ticket: uint16
    exchange: string
    routingKey: string
    mandatory: bool
    immediate: bool
  BasicReturn* = ref object of Method
    replyCode: uint16
    replyText: string
    exchange: string
    routingKey: string
  BasicDeliver* = ref object of Method
    consumerTag: string
    deliveryTag: uint64
    redelivered: bool
    exchange: string
    routingKey: string
  BasicGet* = ref object of Method
    ticket: uint16
    queue: string
    noAck: bool
  BasicGetOk* = ref object of Method
    deliveryTag: uint64
    redelivered: bool
    exchange: string
    routingKey: string
    messageCount: uint32
  BasicGetEmpty* = ref object of Method
    clusterId: string
  BasicAck* = ref object of Method
    deliveryTag: uint64
    multiple: bool
  BasicReject* = ref object of Method
    deliveryTag: uint64
    requeue: bool
  BasicRecoverAsync* = ref object of Method
    requeue: bool
  BasicRecover* = ref object of Method
    requeue: bool
  BasicRecoverOk* = ref object of Method
  BasicNack* = ref object of Method
    deliveryTag: uint64
    multiple: bool
    requeue: bool

#--------------- Basic.Qos ---------------#

proc newBasicQos*(prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false): BasicQos =
  result.new
  result.initMethod(true, 0x003C000A)
  result.prefetchSize = prefetchSize
  result.prefetchCount = prefetchCount

proc decode*(_: type[BasicQos], encoded: InputStream): BasicQos =
  let (_, prefetchSize) = encoded.readBigEndianU32()
  let (_, prefetchCount) = encoded.readBigEndianU16()
  let (_, bbuf) = encoded.readBigEndianU8()
  let globalQos = (bbuf and 0x01) != 0
  result = newBasicQos(prefetchSize, prefetchCount, globalQos)

proc encode*(self: BasicQos, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = (if self.globalQos: 0x01 else: 0x00)
  discard await to.writeBigEndian32(self.prefetchSize)
  discard await to.writeBigEndian16(self.prefetchCount)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.QosOk ---------------#

proc newBasicQosOk*(): BasicQosOk =
  result.new
  result.initMethod(false, 0x003C000B)

proc decode*(_: type[BasicQosOk], encoded: InputStream): BasicQosOk =
  result = newBasicQosOk()

proc encode*(self: BasicQosOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Basic.Consume ---------------#

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

proc decode*(_: type[BasicConsume], encoded: InputStream): BasicConsume =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, consumerTag) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noLocal = (bbuf and 0x01) != 0
  let noAck = (bbuf and 0x02) != 0
  let exclusive = (bbuf and 0x04) != 0
  let noWait =  (bbuf and 0x08) != 0
  result = newBasicConsume(ticket, queue, consumerTag, noLocal, noAck, exclusive, noWait, arguments)

proc encode*(self: BasicConsume, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.noLocal: 0x01 else: 0x00) or 
  (if self.noAck: 0x02 else: 0x00) or 
  (if self.exclusive: 0x04 else: 0x00) or 
  (if self.noWait: 0x08 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeShortString(self.consumerTag)
  discard await to.writeBigEndian8(bbuf)
  discard await to.encodeTable(self.arguments)

#--------------- Basic.ConsumeOk ---------------#

proc newBasicConsumeOk*(consumerTag=""): BasicConsumeOk =
  result.new
  result.initMethod(false, 0x003C0015)
  result.consumerTag = consumerTag

proc decode*(_: type[BasicConsumeOk], encoded: InputStream): BasicConsumeOk =
  let (_, consumerTag) = encoded.readShortString()
  result = newBasicConsumeOk(consumerTag)

proc encode*(self: BasicConsumeOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.consumerTag)

#--------------- Basic.Cancel ---------------#

proc newBasicCancel*(consumerTag="", noWait=false): BasicCancel =
  result.new
  result.initMethod(true, 0x003C001E)
  result.consumerTag = consumerTag
  result.noWait = noWait

proc decode*(_: type[BasicCancel], encoded: InputStream): BasicCancel =
  let (_, consumerTag) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newBasicCancel(consumerTag, noWait)

proc encode*(self: BasicCancel, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.noWait: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeShortString(self.consumerTag)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.CancelOk ---------------#

proc newBasicCancelOk*(consumerTag=""): BasicCancelOk =
  result.new
  result.initMethod(false, 0x003C001F)
  result.consumerTag = consumerTag

proc decode*(_: type[BasicCancelOk], encoded: InputStream): BasicCancelOk =
  let (_, consumerTag) = encoded.readShortString()
  result = newBasicCancelOk(consumerTag)

proc encode*(self: BasicCancelOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.consumerTag)

#--------------- Basic.Publish ---------------#

proc newBasicPublish*(ticket=0.uint16, exchange="", routingKey="", mandatory=false, immediate=false): BasicPublish =
  result.new
  result.initMethod(false, 0x003C0028)
  result.ticket = ticket
  result.exchange = exchange
  result.routingKey = routingKey
  result.mandatory = mandatory
  result.immediate = immediate

proc decode*(_: type[BasicPublish], encoded: InputStream): BasicPublish =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let mandatory = (bbuf and 0x01) != 0
  let immediate = (bbuf and 0x02) != 0
  result = newBasicPublish(ticket, exchange, routingKey, mandatory, immediate)

proc encode*(self: BasicPublish, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.mandatory: 0x01 else: 0x00) or 
  (if self.immediate: 0x02 else: 0x00)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.exchange)
  discard await to.writeShortString(self.routingKey)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.Return ---------------#

proc newBasicReturn*(replyCode=0.uint16, replyText="", exchange="", routingKey=""): BasicReturn =
  result.new
  result.initMethod(false, 0x003C0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.exchange = exchange
  result.routingKey = routingKey

proc decode*(_: type[BasicReturn], encoded: InputStream): BasicReturn =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  result = newBasicReturn(replyCode, replyText, exchange, routingKey)

proc encode*(self: BasicReturn, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian16(self.replyCode)
  discard await to.writeShortString(self.replyText)
  discard await to.writeShortString(self.exchange)
  discard await to.writeShortString(self.routingKey)

#--------------- Basic.Deliver ---------------#

proc newBasicDeliver*(consumerTag="", deliveryTag=0.uint64, redelivered=false, exchange="", routingKey=""): BasicDeliver =
  result.new
  result.initMethod(false, 0x003C003C)
  result.consumerTag = consumerTag
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey

proc decode*(_: type[BasicDeliver], encoded: InputStream): BasicDeliver =
  let (_, consumerTag) = encoded.readShortString()
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let redelivered = (bbuf and 0x01) != 0
  result = newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc encode*(self: BasicDeliver, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeShortString(self.consumerTag)
  discard await to.writeBigEndian64(self.deliveryTag)
  discard await to.writeBigEndian8(bbuf)
  discard await to.writeShortString(self.exchange)
  discard await to.writeShortString(self.routingKey)

#--------------- Basic.Get ---------------#

proc newBasicGet*(ticket=0.uint16, queue="", noAck=false): BasicGet =
  result.new
  result.initMethod(true, 0x003C0046)
  result.ticket = ticket
  result.queue = queue
  result.noAck = noAck

proc decode*(_: type[BasicGet], encoded: InputStream): BasicGet =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let noAck = (bbuf and 0x01) != 0
  result = newBasicGet(ticket, queue, noAck)

proc encode*(self: BasicGet, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.noAck: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian16(self.ticket)
  discard await to.writeShortString(self.queue)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.GetOk ---------------#

proc newBasicGetOk*(deliveryTag=0.uint64, redelivered=false, exchange="", routingKey="", messageCount=0.uint32): BasicGetOk =
  result.new
  result.initMethod(false, 0x003C0047)
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey
  result.messageCount = messageCount

proc decode*(_: type[BasicGetOk], encoded: InputStream): BasicGetOk =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, messageCount) = encoded.readBigEndianU32()
  let redelivered = (bbuf and 0x01) != 0
  result = newBasicGetOk(deliveryTag, redelivered, exchange, routingKey, messageCount)

proc encode*(self: BasicGetOk, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian64(self.deliveryTag)
  discard await to.writeBigEndian8(bbuf)
  discard await to.writeShortString(self.exchange)
  discard await to.writeShortString(self.routingKey)
  discard await to.writeBigEndian32(self.messageCount)

#--------------- Basic.GetEmpty ---------------#

proc newBasicGetEmpty*(clusterId=""): BasicGetEmpty =
  result.new
  result.initMethod(false, 0x003C0048)
  result.clusterId = clusterId

proc decode*(_: type[BasicGetEmpty], encoded: InputStream): BasicGetEmpty =
  let (_, clusterId) = encoded.readShortString()
  result = newBasicGetEmpty(clusterId)

proc encode*(self: BasicGetEmpty, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.clusterId)

#--------------- Basic.Ack ---------------#

proc newBasicAck*(deliveryTag=0.uint64, multiple=false): BasicAck =
  result.new
  result.initMethod(false, 0x003C0050)
  result.deliveryTag = deliveryTag
  result.multiple = multiple

proc decode*(_: type[BasicAck], encoded: InputStream): BasicAck =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  result = newBasicAck(deliveryTag, multiple)

proc encode*(self: BasicAck, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.multiple: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian64(self.deliveryTag)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.Reject ---------------#

proc newBasicReject*(deliveryTag=0.uint64, requeue=false): BasicReject =
  result.new
  result.initMethod(false, 0x003C005A)
  result.deliveryTag = deliveryTag
  result.requeue = requeue

proc decode*(_: type[BasicReject], encoded: InputStream): BasicReject =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicReject(deliveryTag, requeue)

proc encode*(self: BasicReject, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian64(self.deliveryTag)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverAsync ---------------#

proc newBasicRecoverAsync*(requeue=false): BasicRecoverAsync =
  result.new
  result.initMethod(false, 0x003C0064)
  result.requeue = requeue

proc decode*(_: type[BasicRecoverAsync], encoded: InputStream): BasicRecoverAsync =
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecoverAsync(requeue)

proc encode*(self: BasicRecoverAsync, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.Recover ---------------#

proc newBasicRecover*(requeue=false): BasicRecover =
  result.new
  result.initMethod(true, 0x003C006E)
  result.requeue = requeue

proc decode*(_: type[BasicRecover], encoded: InputStream): BasicRecover =
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecover(requeue)

proc encode*(self: BasicRecover, to: AsyncOutputStream) {.async.} =
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverOk ---------------#

proc newBasicRecoverOk*(): BasicRecoverOk =
  result.new
  result.initMethod(false, 0x003C006F)

proc decode*(_: type[BasicRecoverOk], encoded: InputStream): BasicRecoverOk = newBasicRecoverOk()

proc encode*(self: BasicRecoverOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Basic.Nack ---------------#

proc newBasicNack*(deliveryTag=0.uint64, multiple=false, requeue=false): BasicNack =
  result.new
  result.initMethod(false, 0x003C0078)
  result.deliveryTag = deliveryTag
  result.multiple = multiple
  result.requeue = requeue

proc decode*(_: type[BasicNack], encoded: InputStream): BasicNack =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  let requeue = (bbuf and 0x02) != 0
  result = newBasicNack(deliveryTag, multiple, requeue)

proc encode*(self: BasicNack, to: AsyncOutputStream) {.async.} =
  let bbuf = 0x00.uint8 or
    (if self.multiple: 0x01.uint8 else: 0x00.uint8) or
    (if self.requeue: 0x02.uint8 else: 0x00.uint8)
  discard await to.writeBigEndian64(self.deliveryTag)
  discard await to.writeBigEndian8(bbuf)
