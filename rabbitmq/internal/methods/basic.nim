import tables
import ../data
import ../streams
import ./submethods

const BASIC_METHODS* = 0x003C.uint16
const BASIC_QOS_METHOD_ID = 0x003C000A.uint32
const BASIC_QOS_OK_METHOD_ID = 0x003C000B.uint32
const BASIC_CONSUME_METHOD_ID = 0x003C0014.uint32
const BASIC_CONSUME_OK_METHOD_ID = 0x003C0015.uint32
const BASIC_CANCEL_METHOD_ID = 0x003C001E.uint32
const BASIC_CANCEL_OK_METHOD_ID = 0x003C001F.uint32
const BASIC_PUBLISH_METHOD_ID = 0x003C0028.uint32
const BASIC_RETURN_METHOD_ID = 0x003C0032.uint32
const BASIC_DELIVER_METHOD_ID = 0x003C003C.uint32
const BASIC_GET_METHOD_ID = 0x003C0046.uint32
const BASIC_GET_OK_METHOD_ID = 0x003C0047.uint32
const BASIC_GET_EMPTY_METHOD_ID = 0x003C0048.uint32
const BASIC_ACK_METHOD_ID = 0x003C0050.uint32
const BASIC_REJECT_METHOD_ID = 0x003C005A.uint32
const BASIC_RECOVER_ASYNC_METHOD_ID = 0x003C0064.uint32
const BASIC_RECOVER_METHOD_ID = 0x003C006E.uint32
const BASIC_RECOVER_OK_METHOD_ID = 0x003C006F.uint32
const BASIC_NACK_METHOD_ID = 0x003C0078.uint32

type
  BasicVariants* = enum
    NONE = 0
    BASIC_QOS_METHOD = (BASIC_QOS_METHOD_ID and 0x0000FFFF).uint16
    BASIC_QOS_OK_METHOD = (BASIC_QOS_OK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_CONSUME_METHOD = (BASIC_CONSUME_METHOD_ID and 0x0000FFFF).uint16
    BASIC_CONSUME_OK_METHOD = (BASIC_CONSUME_OK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_CANCEL_METHOD = (BASIC_CANCEL_METHOD_ID and 0x0000FFFF).uint16
    BASIC_CANCEL_OK_METHOD = (BASIC_CANCEL_OK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_PUBLISH_METHOD = (BASIC_PUBLISH_METHOD_ID and 0x0000FFFF).uint16
    BASIC_RETURN_METHOD = (BASIC_RETURN_METHOD_ID and 0x0000FFFF).uint16
    BASIC_DELIVER_METHOD = (BASIC_DELIVER_METHOD_ID and 0x0000FFFF).uint16
    BASIC_GET_METHOD = (BASIC_GET_METHOD_ID and 0x0000FFFF).uint16
    BASIC_GET_OK_METHOD = (BASIC_GET_OK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_GET_EMPTY_METHOD = (BASIC_GET_EMPTY_METHOD_ID and 0x0000FFFF).uint16
    BASIC_ACK_METHOD = (BASIC_ACK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_REJECT_METHOD = (BASIC_REJECT_METHOD_ID and 0x0000FFFF).uint16
    BASIC_RECOVER_ASYNC_METHOD = (BASIC_RECOVER_ASYNC_METHOD_ID and 0x0000FFFF).uint16
    BASIC_RECOVER_METHOD = (BASIC_RECOVER_METHOD_ID and 0x0000FFFF).uint16
    BASIC_RECOVER_OK_METHOD = (BASIC_RECOVER_OK_METHOD_ID and 0x0000FFFF).uint16
    BASIC_NACK_METHOD = (BASIC_NACK_METHOD_ID and 0x0000FFFF).uint16


type 
  BasicMethod* = ref object of SubMethod
    ticket*: uint16
    exchange*: string
    consumerTag*: string
    routingKey*: string
    noWait*: bool
    queue*: string
    noAck*: bool
    deliveryTag*: uint64
    redelivered*: bool
    requeue*: bool
    multiple*: bool
    case indexLo*: BasicVariants
    of NONE:
      discard
    of BASIC_QOS_METHOD:
      prefetchSize*: uint32
      prefetchCount*: uint16
      globalQos*: bool
    of BASIC_CONSUME_METHOD:
      noLocal*: bool
      exclusive*: bool
      arguments*: TableRef[string, Field]
    of BASIC_PUBLISH_METHOD:
      mandatory*: bool
      immediate*: bool
    of BASIC_RETURN_METHOD:
      replyCode*: uint16
      replyText*: string
    of BASIC_GET_OK_METHOD:
      messageCount*: uint32
    of BASIC_GET_EMPTY_METHOD:
      clusterId*: string
    of BASIC_QOS_OK_METHOD:
      discard
    of BASIC_CONSUME_OK_METHOD:
      discard
    of BASIC_CANCEL_METHOD:
      discard
    of BASIC_CANCEL_OK_METHOD:
      discard
    of BASIC_DELIVER_METHOD:
      discard
    of BASIC_GET_METHOD:
      discard
    of BASIC_ACK_METHOD:
      discard
    of BASIC_REJECT_METHOD:
      discard
    of BASIC_RECOVER_ASYNC_METHOD:
      discard
    of BASIC_RECOVER_METHOD:
      discard
    of BASIC_RECOVER_OK_METHOD:
      discard
    of BASIC_NACK_METHOD:
      discard

proc decodeBasicQos(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicQos(to: OutputStream, data: BasicMethod)
proc decodeBasicQosOk(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicQosOk(to: OutputStream, data: BasicMethod)
proc decodeBasicConsume(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicConsume(to: OutputStream, data: BasicMethod)
proc decodeBasicConsumeOk(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicConsumeOk(to: OutputStream, data: BasicMethod)
proc decodeBasicCancel(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicCancel(to: OutputStream, data: BasicMethod)
proc decodeBasicCancelOk(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicCancelOk(to: OutputStream, data: BasicMethod)
proc decodeBasicPublish(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicPublish(to: OutputStream, data: BasicMethod)
proc decodeBasicReturn(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicReturn(to: OutputStream, data: BasicMethod)
proc decodeBasicDeliver(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicDeliver(to: OutputStream, data: BasicMethod)
proc decodeBasicGet(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicGet(to: OutputStream, data: BasicMethod)
proc decodeBasicGetOk(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicGetOk(to: OutputStream, data: BasicMethod)
proc decodeBasicGetEmpty(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicGetEmpty(to: OutputStream, data: BasicMethod)
proc decodeBasicAck(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicAck(to: OutputStream, data: BasicMethod)
proc decodeBasicReject(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicReject(to: OutputStream, data: BasicMethod)
proc decodeBasicRecoverAsync(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicRecoverAsync(to: OutputStream, data: BasicMethod)
proc decodeBasicRecover(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicRecover(to: OutputStream, data: BasicMethod)
proc decodeBasicRecoverOk(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicRecoverOk(to: OutputStream, data: BasicMethod)
proc decodeBasicNack(encoded: InputStream): (bool, seq[uint16], BasicMethod)
proc encodeBasicNack(to: OutputStream, data: BasicMethod)

proc decode*(_: type[BasicMethod], submethodId: BasicVariants, encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  case submethodId
  of BASIC_QOS_METHOD:
    result = decodeBasicQos(encoded)
  of BASIC_QOS_OK_METHOD:
    result = decodeBasicQosOk(encoded)
  of BASIC_CONSUME_METHOD:
    result = decodeBasicConsume(encoded)
  of BASIC_CONSUME_OK_METHOD:
    result = decodeBasicConsumeOk(encoded)
  of BASIC_CANCEL_METHOD:
    result = decodeBasicCancel(encoded)
  of BASIC_CANCEL_OK_METHOD:
    result = decodeBasicCancelOk(encoded)
  of BASIC_PUBLISH_METHOD:
    result = decodeBasicPublish(encoded)
  of BASIC_RETURN_METHOD:
    result = decodeBasicReturn(encoded)
  of BASIC_DELIVER_METHOD:
    result = decodeBasicDeliver(encoded)
  of BASIC_GET_METHOD:
    result = decodeBasicGet(encoded)
  of BASIC_GET_OK_METHOD:
    result = decodeBasicGetOk(encoded)
  of BASIC_GET_EMPTY_METHOD:
    result = decodeBasicGetEmpty(encoded)
  of BASIC_ACK_METHOD:
    result = decodeBasicAck(encoded)
  of BASIC_REJECT_METHOD:
    result = decodeBasicReject(encoded)
  of BASIC_RECOVER_ASYNC_METHOD:
    result = decodeBasicRecoverAsync(encoded)
  of BASIC_RECOVER_METHOD:
    result = decodeBasicRecover(encoded)
  of BASIC_RECOVER_OK_METHOD:
    result = decodeBasicRecoverOk(encoded)
  of BASIC_NACK_METHOD:
    result = decodeBasicNack(encoded)
  else:
    discard

proc encode*(to: OutputStream, data: BasicMethod) =
  case data.indexLo
  of BASIC_QOS_METHOD:
    to.encodeBasicQos(data)
  of BASIC_QOS_OK_METHOD:
    to.encodeBasicQosOk(data)
  of BASIC_CONSUME_METHOD:
    to.encodeBasicConsume(data)
  of BASIC_CONSUME_OK_METHOD:
    to.encodeBasicConsumeOk(data)
  of BASIC_CANCEL_METHOD:
    to.encodeBasicCancel(data)
  of BASIC_CANCEL_OK_METHOD:
    to.encodeBasicCancelOk(data)
  of BASIC_PUBLISH_METHOD:
    to.encodeBasicPublish(data)
  of BASIC_RETURN_METHOD:
    to.encodeBasicReturn(data)
  of BASIC_DELIVER_METHOD:
    to.encodeBasicDeliver(data)
  of BASIC_GET_METHOD:
    to.encodeBasicGet(data)
  of BASIC_GET_OK_METHOD:
    to.encodeBasicGetOk(data)
  of BASIC_GET_EMPTY_METHOD:
    to.encodeBasicGetEmpty(data)
  of BASIC_ACK_METHOD:
    to.encodeBasicAck(data)
  of BASIC_REJECT_METHOD:
    to.encodeBasicReject(data)
  of BASIC_RECOVER_ASYNC_METHOD:
    to.encodeBasicRecoverAsync(data)
  of BASIC_RECOVER_METHOD:
    to.encodeBasicRecover(data)
  of BASIC_RECOVER_OK_METHOD:
    to.encodeBasicRecoverOk(data)
  of BASIC_NACK_METHOD:
    to.encodeBasicNack(data)
  else:
    discard

#--------------- Basic.Qos ---------------#

proc newBasicQos*(prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_QOS_METHOD)
  res.prefetchSize = prefetchSize
  res.prefetchCount = prefetchCount
  result = (true, @[ord(BASIC_QOS_OK_METHOD).uint16], res)

proc decodeBasicQos(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, prefetchSize) = encoded.readBigEndianU32()
  let (_, prefetchCount) = encoded.readBigEndianU16()
  let (_, bbuf) = encoded.readBigEndianU8()
  let globalQos = (bbuf and 0x01) != 0
  result = newBasicQos(prefetchSize, prefetchCount, globalQos)

proc encodeBasicQos(to: OutputStream, data: BasicMethod) =
  let bbuf: uint8 = (if data.globalQos: 0x01 else: 0x00)
  to.writeBigEndian32(data.prefetchSize)
  to.writeBigEndian16(data.prefetchCount)
  to.writeBigEndian8(bbuf)

#--------------- Basic.QosOk ---------------#

proc newBasicQosOk*(): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_QOS_OK_METHOD)
  result = (false, @[], res)

proc decodeBasicQosOk(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  result = newBasicQosOk()

proc encodeBasicQosOk(to: OutputStream, data: BasicMethod) = discard

#--------------- Basic.Consume ---------------#

proc newBasicConsume*(
  ticket=0.uint16, 
  queue="", 
  consumerTag="", 
  noLocal=false, 
  noAck=false, 
  exclusive=false, 
  noWait=false, 
  arguments: TableRef[string, Field] = nil): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_CONSUME_METHOD)
  res.ticket = ticket
  res.queue = queue
  res.consumerTag = consumerTag
  res.noLocal = noLocal
  res.noAck = noAck
  res.exclusive = exclusive
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(BASIC_CONSUME_OK_METHOD).uint16], res)

proc decodeBasicConsume(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
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

proc encodeBasicConsume(to: OutputStream, data: BasicMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
  (if data.noLocal: 0x01 else: 0x00) or 
  (if data.noAck: 0x02 else: 0x00) or 
  (if data.exclusive: 0x04 else: 0x00) or 
  (if data.noWait: 0x08 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeShortString(data.consumerTag)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Basic.ConsumeOk ---------------#

proc newBasicConsumeOk*(consumerTag=""): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_CONSUME_OK_METHOD)
  res.consumerTag = consumerTag
  result = (false, @[], res)

proc decodeBasicConsumeOk(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, consumerTag) = encoded.readShortString()
  result = newBasicConsumeOk(consumerTag)

proc encodeBasicConsumeOk(to: OutputStream, data: BasicMethod) =
  to.writeShortString(data.consumerTag)

#--------------- Basic.Cancel ---------------#

proc newBasicCancel*(consumerTag="", noWait=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_CANCEL_METHOD)
  res.consumerTag = consumerTag
  res.noWait = noWait
  result = (true, @[ord(BASIC_CANCEL_OK_METHOD).uint16], res)

proc decodeBasicCancel(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, consumerTag) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newBasicCancel(consumerTag, noWait)

proc encodeBasicCancel(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.noWait: 0x01.uint8 else: 0x00.uint8)
  to.writeShortString(data.consumerTag)
  to.writeBigEndian8(bbuf)

#--------------- Basic.CancelOk ---------------#

proc newBasicCancelOk*(consumerTag=""): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_CANCEL_OK_METHOD)
  res.consumerTag = consumerTag
  result = (false, @[], res)

proc decodeBasicCancelOk(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, consumerTag) = encoded.readShortString()
  result = newBasicCancelOk(consumerTag)

proc encodeBasicCancelOk(to: OutputStream, data: BasicMethod) =
  to.writeShortString(data.consumerTag)

#--------------- Basic.Publish ---------------#

proc newBasicPublish*(ticket=0.uint16, exchange="", routingKey="", mandatory=false, immediate=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_PUBLISH_METHOD)
  res.ticket = ticket
  res.exchange = exchange
  res.routingKey = routingKey
  res.mandatory = mandatory
  res.immediate = immediate
  result = (false, @[], res)

proc decodeBasicPublish(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let mandatory = (bbuf and 0x01) != 0
  let immediate = (bbuf and 0x02) != 0
  result = newBasicPublish(ticket, exchange, routingKey, mandatory, immediate)

proc encodeBasicPublish(to: OutputStream, data: BasicMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
  (if data.mandatory: 0x01 else: 0x00) or 
  (if data.immediate: 0x02 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.exchange)
  to.writeShortString(data.routingKey)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Return ---------------#

proc newBasicReturn*(replyCode=0.uint16, replyText="", exchange="", routingKey=""): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_RETURN_METHOD)
  res.replyCode = replyCode
  res.replyText = replyText
  res.exchange = exchange
  res.routingKey = routingKey
  result = (false, @[], res)

proc decodeBasicReturn(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  result = newBasicReturn(replyCode, replyText, exchange, routingKey)

proc encodeBasicReturn(to: OutputStream, data: BasicMethod) =
  to.writeBigEndian16(data.replyCode)
  to.writeShortString(data.replyText)
  to.writeShortString(data.exchange)
  to.writeShortString(data.routingKey)

#--------------- Basic.Deliver ---------------#

proc newBasicDeliver*(consumerTag="", deliveryTag=0.uint64, redelivered=false, exchange="", routingKey=""): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_DELIVER_METHOD)
  res.consumerTag = consumerTag
  res.deliveryTag = deliveryTag
  res.redelivered = redelivered
  res.exchange = exchange
  res.routingKey = routingKey
  result = (false, @[], res)

proc decodeBasicDeliver(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, consumerTag) = encoded.readShortString()
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let redelivered = (bbuf and 0x01) != 0
  result = newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc encodeBasicDeliver(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.redelivered: 0x01.uint8 else: 0x00.uint8)
  to.writeShortString(data.consumerTag)
  to.writeBigEndian64(data.deliveryTag)
  to.writeBigEndian8(bbuf)
  to.writeShortString(data.exchange)
  to.writeShortString(data.routingKey)

#--------------- Basic.Get ---------------#

proc newBasicGet*(ticket=0.uint16, queue="", noAck=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_GET_METHOD)
  res.ticket = ticket
  res.queue = queue
  res.noAck = noAck
  result = (true, @[ord(BASIC_GET_OK_METHOD).uint16, ord(BASIC_GET_EMPTY_METHOD).uint16], res)

proc decodeBasicGet(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let noAck = (bbuf and 0x01) != 0
  result = newBasicGet(ticket, queue, noAck)

proc encodeBasicGet(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.noAck: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeBigEndian8(bbuf)

#--------------- Basic.GetOk ---------------#

proc newBasicGetOk*(deliveryTag=0.uint64, redelivered=false, exchange="", routingKey="", messageCount=0.uint32): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_GET_OK_METHOD)
  res.deliveryTag = deliveryTag
  res.redelivered = redelivered
  res.exchange = exchange
  res.routingKey = routingKey
  res.messageCount = messageCount
  result = (false, @[], res)

proc decodeBasicGetOk(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, exchange) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, messageCount) = encoded.readBigEndianU32()
  let redelivered = (bbuf and 0x01) != 0
  result = newBasicGetOk(deliveryTag, redelivered, exchange, routingKey, messageCount)

proc encodeBasicGetOk(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.redelivered: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian64(data.deliveryTag)
  to.writeBigEndian8(bbuf)
  to.writeShortString(data.exchange)
  to.writeShortString(data.routingKey)
  to.writeBigEndian32(data.messageCount)

#--------------- Basic.GetEmpty ---------------#

proc newBasicGetEmpty*(clusterId=""): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_GET_EMPTY_METHOD)
  res.clusterId = clusterId
  result = (false, @[], res)

proc decodeBasicGetEmpty(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, clusterId) = encoded.readShortString()
  result = newBasicGetEmpty(clusterId)

proc encodeBasicGetEmpty(to: OutputStream, data: BasicMethod) =
  to.writeShortString(data.clusterId)

#--------------- Basic.Ack ---------------#

proc newBasicAck*(deliveryTag=0.uint64, multiple=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_ACK_METHOD)
  res.deliveryTag = deliveryTag
  res.multiple = multiple
  result = (false, @[], res)

proc decodeBasicAck(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  result = newBasicAck(deliveryTag, multiple)

proc encodeBasicAck(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.multiple: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian64(data.deliveryTag)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Reject ---------------#

proc newBasicReject*(deliveryTag=0.uint64, requeue=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_REJECT_METHOD)
  res.deliveryTag = deliveryTag
  res.requeue = requeue
  result = (false, @[], res)

proc decodeBasicReject(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicReject(deliveryTag, requeue)

proc encodeBasicReject(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian64(data.deliveryTag)
  to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverAsync ---------------#

proc newBasicRecoverAsync*(requeue=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_RECOVER_ASYNC_METHOD)
  res.requeue = requeue
  result = (false, @[], res)

proc decodeBasicRecoverAsync(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecoverAsync(requeue)

proc encodeBasicRecoverAsync(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.Recover ---------------#

proc newBasicRecover*(requeue=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_RECOVER_METHOD)
  res.requeue = requeue
  result = (true, @[ord(BASIC_RECOVER_OK_METHOD).uint16], res)

proc decodeBasicRecover(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, bbuf) = encoded.readBigEndianU8()
  let requeue = (bbuf and 0x01) != 0
  result = newBasicRecover(requeue)

proc encodeBasicRecover(to: OutputStream, data: BasicMethod) =
  let bbuf = (if data.requeue: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Basic.RecoverOk ---------------#

proc newBasicRecoverOk*(): (bool, seq[uint16], BasicMethod) =
  result = (false, @[], BasicMethod(indexLo: BASIC_RECOVER_OK_METHOD))

proc decodeBasicRecoverOk(encoded: InputStream): (bool, seq[uint16], BasicMethod) = newBasicRecoverOk()

proc encodeBasicRecoverOk(to: OutputStream, data: BasicMethod) = discard

#--------------- Basic.Nack ---------------#

proc newBasicNack*(deliveryTag=0.uint64, multiple=false, requeue=false): (bool, seq[uint16], BasicMethod) =
  var res = BasicMethod(indexLo: BASIC_NACK_METHOD)
  res.deliveryTag = deliveryTag
  res.multiple = multiple
  res.requeue = requeue
  result = (false, @[], res)

proc decodeBasicNack(encoded: InputStream): (bool, seq[uint16], BasicMethod) =
  let (_, deliveryTag) = encoded.readBigEndianU64()
  let (_, bbuf) = encoded.readBigEndianU8()
  let multiple = (bbuf and 0x01) != 0
  let requeue = (bbuf and 0x02) != 0
  result = newBasicNack(deliveryTag, multiple, requeue)

proc encodeBasicNack(to: OutputStream, data: BasicMethod) =
  let bbuf = 0x00.uint8 or
    (if data.multiple: 0x01.uint8 else: 0x00.uint8) or
    (if data.requeue: 0x02.uint8 else: 0x00.uint8)
  to.writeBigEndian64(data.deliveryTag)
  to.writeBigEndian8(bbuf)
