import streams
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

proc decode*(_: type[BasicQos], encoded: Stream): BasicQos =
  let prefetchSize = encoded.readBigEndianU32()
  let prefetchCount = encoded.readBigEndianU16()
  let bbuf = encoded.readBigEndianU8()
  let globalQos = (bbuf and 0x01) != 0
  result = newBasicQos(prefetchSize, prefetchCount, globalQos)

proc encode*(self: BasicQos, to: Stream) =
  to.writeBigEndian32(self.prefetchSize)
  to.writeBigEndian16(self.prefetchCount)
  let bbuf: uint8 = (if self.globalQos: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Basic.QosOk ---------------#

proc newBasicQosOk*(): BasicQosOk =
  result.new
  result.initMethod(false, 0x003C000B)

proc decode*(_: type[BasicQosOk], encoded: Stream): BasicQosOk =
  result = newBasicQosOk()

proc encode*(self: BasicQosOk, to: Stream) = discard

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

proc decode*(_: type[BasicConsume], encoded: Stream): BasicConsume =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let consumerTag = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let noLocal = (bbuf and 0x01) != 0
  let noAck = (bbuf and 0x02) != 0
  let exclusive = (bbuf and 0x04) != 0
  let noWait =  (bbuf and 0x08) != 0
  let arguments = encoded.decodeTable()
  result = newBasicConsume(ticket, queue, consumerTag, noLocal, noAck, exclusive, noWait, arguments)

proc encode*(self: BasicConsume, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  to.writeShortString(self.consumerTag)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.noLocal: 0x01 else: 0x00) or 
  (if self.noAck: 0x02 else: 0x00) or 
  (if self.exclusive: 0x04 else: 0x00) or 
  (if self.noWait: 0x08 else: 0x00)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Basic.ConsumeOk ---------------#

proc newBasicConsumeOk*(consumerTag=""): BasicConsumeOk =
  result.new
  result.initMethod(false, 0x003C0015)
  result.consumerTag = consumerTag

proc decode*(_: type[BasicConsumeOk], encoded: Stream): BasicConsumeOk =
  let consumerTag = encoded.readShortString()
  result = newBasicConsumeOk(consumerTag)

proc encode*(self: BasicConsumeOk, to: Stream) =
  to.writeShortString(self.consumerTag)

#--------------- Basic.Cancel ---------------#

proc newBasicCancel*(consumerTag="", noWait=false): BasicCancel =
  result.new
  result.initMethod(true, 0x003C001E)
  result.consumerTag = consumerTag
  result.noWait = noWait

proc decode*(_: type[BasicCancel], encoded: Stream): BasicCancel =
  let consumerTag = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newBasicCancel(consumerTag, noWait)

proc encode*(self: BasicCancel, to: Stream) =
  to.writeShortString(self.consumerTag)
  let bbuf = (if self.noWait: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.CancelOk ---------------#

proc newBasicCancelOk*(consumerTag=""): BasicCancelOk =
  result.new
  result.initMethod(false, 0x003C001F)
  result.consumerTag = consumerTag

proc decode*(_: type[BasicCancelOk], encoded: Stream): BasicCancelOk =
  let consumerTag = encoded.readShortString()
  result = newBasicCancelOk(consumerTag)

proc encode*(self: BasicCancelOk, to: Stream) =
  to.writeShortString(self.consumerTag)

#--------------- Basic.Publish ---------------#

proc newBasicPublish*(ticket=0.uint16, exchange="", routingKey="", mandatory=false, immediate=false): BasicPublish =
  result.new
  result.initMethod(false, 0x003C0028)
  result.ticket = ticket
  result.exchange = exchange
  result.routingKey = routingKey
  result.mandatory = mandatory
  result.immediate = immediate

proc decode*(_: BasicPublish, encoded: Stream): BasicPublish =
  let ticket = encoded.readBigEndianU16()
  let exchange = encoded.readShortString()
  let routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let mandatory = (bbuf and 0x01) != 0
  let immediate = (bbuf and 0x02) != 0
  result = newBasicPublish(ticket, exchange, routingKey, mandatory, immediate)

proc encode*(self: BasicPublish, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.exchange)
  to.writeShortString(self.routingKey)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.mandatory: 0x01 else: 0x00) or 
  (if self.immediate: 0x02 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Return ---------------#

proc newBasicReturn*(replyCode=0.uint16, replyText="", exchange="", routingKey=""): BasicReturn =
  result.new
  result.initMethod(false, 0x003C0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.exchange = exchange
  result.routingKey = routingKey

proc encode*(_: type[BasicReturn], encoded: Stream): BasicReturn =
  let replyCode = encoded.readBigEndianU16()
  let replyText = encoded.readShortString()
  let exchange = encoded.readShortString()
  let routingKey = encoded.readShortString()
  result = newBasicReturn(replyCode, replyText, exchange, routingKey)

proc decode*(self: BasicReturn, to: Stream) =
  to.writeBigEndian16(self.replyCode)
  to.writeShortString(self.replyText)
  to.writeShortString(self.exchange)
  to.writeShortString(self.routingKey)

#--------------- Basic.Deliver ---------------#

proc newBasicDeliver*(consumerTag="", deliveryTag=0.uint64, redelivered=false, exchange="", routingKey=""): BasicDeliver =
  result.new
  result.initMethod(false, 0x003C003C)
  result.consumerTag = consumerTag
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey

proc decode*(_: type[BasicDeliver], encoded: Stream): BasicDeliver =
  let consumerTag = encoded.readShortString()
  let deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  let redelivered = (bbuf and 0x01) != 0
  let exchange = encoded.readShortString()
  let routingKey = encoded.readShortString()
  result = newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc encode*(self: BasicDeliver, to: Stream) =
  to.writeShortString(self.consumerTag)
  to.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)
  to.writeShortString(self.exchange)
  to.writeShortString(self.routingKey)

#--------------- Basic.Get ---------------#

proc newBasicGet*(ticket=0.uint16, queue="", noAck=false): BasicGet =
  result.new
  result.initMethod(true, 0x003C0046)
  result.ticket = ticket
  result.queue = queue
  result.noAck = noAck

proc decode*(_: type[BasicGet], encoded: Stream): BasicGet =
  let ticket = encoded.readBigEndianU16()
  let queue = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let noAck = (bbuf and 0x01) != 0
  result = newBasicGet(ticket, queue, noAck)

proc encode*(self: BasicGet, to: Stream) =
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.queue)
  let bbuf = (if self.noAck: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.GetOk ---------------#

proc newBasicGetOk*(deliveryTag=0.uint64, redelivered=false, exchange="", routingKey="", messageCount=0.uint32): BasicGetOk =
  result.new
  result.initMethod(false, 0x003C0047)
  result.deliveryTag = deliveryTag
  result.redelivered = redelivered
  result.exchange = exchange
  result.routingKey = routingKey
  result.messageCount = messageCount

proc decode*(_: type[BasicGetOk], encoded: Stream): BasicGetOk =
  let deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  let redelivered = (bbuf and 0x01) != 0
  let exchange = encoded.readShortString()
  let routingKey = encoded.readShortString()
  let messageCount = encoded.readBigEndianU32()
  result = newBasicGetOk(deliveryTag, redelivered, exchange, routingKey, messageCount)

proc encode*(self: BasicGetOk, to: Stream) =
  to.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.redelivered: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)
  to.writeShortString(self.exchange)
  to.writeShortString(self.routingKey)
  to.writeBigEndian32(self.messageCount)

#--------------- Basic.GetEmpty ---------------#

proc newBasicGetEmpty*(clusterId=""): BasicGetEmpty =
  result.new
  result.initMethod(false, 0x003C0048)
  result.clusterId = clusterId

proc decode*(_: type[BasicGetEmpty], encoded: Stream): BasicGetEmpty =
  let clusterId = encoded.readShortString()
  result = newBasicGetEmpty(clusterId)

proc encode*(self: BasicGetEmpty, to: Stream) =
  to.writeShortString(self.clusterId)

#--------------- Basic.Ack ---------------#

proc newBasicAck*(deliveryTag=0.uint64, multiple=false): BasicAck =
  result.new
  result.initMethod(false, 0x003C0050)
  result.deliveryTag = deliveryTag
  result.multiple = multiple

proc decode*(_: type[BasicAck], encoded: Stream): BasicAck =
  let deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  result = newBasicAck(deliveryTag, multiple)

proc encode*(self: BasicAck, to: Stream) =
  to.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.multiple: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Reject ---------------#

proc newBasicReject*(deliveryTag=0.uint64, requeue=false): BasicReject =
  result.new
  result.initMethod(false, 0x003C005A)
  result.deliveryTag = deliveryTag
  result.requeue = requeue

proc decode*(_: type[BasicReject], encoded: Stream): BasicReject =
  let deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicReject(deliveryTag, requeue)

proc encode*(self: BasicReject, to: Stream) =
  to.writeBigEndian64(self.deliveryTag)
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverAsync ---------------#

proc newBasicRecoverAsync*(requeue=false): BasicRecoverAsync =
  result.new
  result.initMethod(false, 0x003C0064)
  result.requeue = requeue

proc decode*(_: type[BasicRecoverAsync], encoded: Stream): BasicRecoverAsync =
  let bbuf = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecoverAsync(requeue)

proc encode*(self: BasicRecoverAsync, to: Stream) =
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Recover ---------------#

proc newBasicRecover*(requeue=false): BasicRecover =
  result.new
  result.initMethod(true, 0x003C006E)
  result.requeue = requeue

proc decode*(_: type[BasicRecover], encoded: Stream): BasicRecover =
  let bbuf = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecover(requeue)

proc encode*(self: BasicRecover, to: Stream) =
  let bbuf = (if self.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverOk ---------------#

proc newBasicRecoverOk*(): BasicRecoverOk =
  result.new
  result.initMethod(false, 0x003C006F)

proc decode*(_: type[BasicRecoverOk], encoded: Stream): BasicRecoverOk = newBasicRecoverOk()

proc encode*(self: BasicRecoverOk, to: Stream) = discard

#--------------- Basic.Nack ---------------#

proc newBasicNack*(deliveryTag=0.uint64, multiple=false, requeue=false): BasicNack =
  result.new
  result.initMethod(false, 0x003C0078)
  result.deliveryTag = deliveryTag
  result.multiple = multiple
  result.requeue = requeue

proc decode*(_: type[BasicNack], encoded: Stream): BasicNack =
  let deliveryTag = encoded.readBigEndianU64()
  let bbuf = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  let requeue = (bbuf and 0x02) != 0
  result = newBasicNack(deliveryTag, multiple, requeue)

proc encode*(self: BasicNack, to: Stream) =
  to.writeBigEndian64(self.deliveryTag)
  let bbuf = 0x00.uint8 or
    (if self.multiple: 0x01.uint8 else: 0x00.uint8) or
    (if self.requeue: 0x02.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)
