import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../field
import ../exceptions

const BASIC_METHODS* = 0x003C.uint16
const BASIC_QOS_METHOD_ID* = 0x003C000A.uint32
const BASIC_QOS_OK_METHOD_ID* = 0x003C000B.uint32
const BASIC_CONSUME_METHOD_ID* = 0x003C0014.uint32
const BASIC_CONSUME_OK_METHOD_ID* = 0x003C0015.uint32
const BASIC_CANCEL_METHOD_ID* = 0x003C001E.uint32
const BASIC_CANCEL_OK_METHOD_ID* = 0x003C001F.uint32
const BASIC_PUBLISH_METHOD_ID* = 0x003C0028.uint32
const BASIC_RETURN_METHOD_ID* = 0x003C0032.uint32
const BASIC_DELIVER_METHOD_ID* = 0x003C003C.uint32
const BASIC_GET_METHOD_ID* = 0x003C0046.uint32
const BASIC_GET_OK_METHOD_ID* = 0x003C0047.uint32
const BASIC_GET_EMPTY_METHOD_ID* = 0x003C0048.uint32
const BASIC_ACK_METHOD_ID* = 0x003C0050.uint32
const BASIC_REJECT_METHOD_ID* = 0x003C005A.uint32
const BASIC_RECOVER_ASYNC_METHOD_ID* = 0x003C0064.uint32
const BASIC_RECOVER_METHOD_ID* = 0x003C006E.uint32
const BASIC_RECOVER_OK_METHOD_ID* = 0x003C006F.uint32
const BASIC_NACK_METHOD_ID* = 0x003C0078.uint32

type
  AMQPBasicKind = enum
    AMQP_BASIC_NONE = 0
    AMQP_BASIC_QOS_SUBMETHOD = (BASIC_QOS_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_QOS_OK_SUBMETHOD = (BASIC_QOS_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_CONSUME_SUBMETHOD = (BASIC_CONSUME_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_CONSUME_OK_SUBMETHOD = (BASIC_CONSUME_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_CANCEL_SUBMETHOD = (BASIC_CANCEL_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_CANCEL_OK_SUBMETHOD = (BASIC_CANCEL_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_PUBLISH_SUBMETHOD = (BASIC_PUBLISH_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_RETURN_SUBMETHOD = (BASIC_RETURN_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_DELIVER_SUBMETHOD = (BASIC_DELIVER_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_GET_SUBMETHOD = (BASIC_GET_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_GET_OK_SUBMETHOD = (BASIC_GET_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_GET_EMPTY_SUBMETHOD = (BASIC_GET_EMPTY_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_ACK_SUBMETHOD = (BASIC_ACK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_REJECT_SUBMETHOD = (BASIC_REJECT_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD = (BASIC_RECOVER_ASYNC_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_RECOVER_SUBMETHOD = (BASIC_RECOVER_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_RECOVER_OK_SUBMETHOD = (BASIC_RECOVER_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_BASIC_NACK_SUBMETHOD = (BASIC_NACK_METHOD_ID and 0x0000FFFF).uint16

  AMQPBasicQOSBits = object
    global {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicConsumeBits = object
    noLocal {.bitsize: 1.}: bool
    noAck {.bitsize: 1.}: bool
    exclusive {.bitsize: 1.}: bool
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 4.}: uint8

  AMQPBasicCancelBits = object
    noWait {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicPublishBits = object
    mandatory {.bitsize: 1.}: bool
    immediate {.bitsize: 1.}: bool
    unused {.bitsize: 5.}: uint8

  AMQPBasicRedeliveredBits = object
    redelivered {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicGetBits = object
    noAck {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicAckBits = object
    multiple {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicRequeueBits = object
    requeue {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPBasicNackBits = object
    multiple {.bitsize: 1.}: bool
    requeue {.bitsize: 1.}: bool
    unused {.bitsize: 5.}: uint8

  AMQPBasicQOSObj = object of RootObj
    prefetchSize: uint32
    prefetchCount: uint16
    flags: AMQPBasicQOSBits

  AMQPBasicConsumerTagObj = object of RootObj
    consumerTag: string
  
  AMQPBasicConsumeObj = object of AMQPBasicConsumerTagObj
    ticket: uint16
    queue: string
    flags: AMQPBasicConsumeBits
    args: FieldTable
  
  AMQPBasicCancelObj = object of AMQPBasicConsumerTagObj
    flags: AMQPBasicCancelBits
  
  AMQPBasicExchangeRoutingKeyObj = object of RootObj
    exchange: string
    routingKey: string

  AMQPBasicPublishObj = object of AMQPBasicExchangeRoutingKeyObj
    ticket: uint16
    flags: AMQPBasicPublishBits

  AMQPBasicReturnObj = object of AMQPBasicExchangeRoutingKeyObj
    replyCode: uint16
    replyText: string
  
  AMQPBasicDeliveryTagObj = object of RootObj
    deliveryTag: uint64

  AMQPBasicDeliveryTagExchangeRoutingKeyObj = object of AMQPBasicDeliveryTagObj
    exchange: string
    routingKey: string

  AMQPBasicDeliverObj = object of AMQPBasicDeliveryTagExchangeRoutingKeyObj
    consumerTag: string
    flags: AMQPBasicRedeliveredBits
  
  AMQPBasicGetObj = object of RootObj
    ticket: uint16
    queue: string
    flags: AMQPBasicGetBits

  AMQPBasicGetOkObj = object of AMQPBasicDeliveryTagExchangeRoutingKeyObj
    messageCount: uint32
    flags: AMQPBasicRedeliveredBits
  
  AMQPBasicGetEmptyObj = object of RootObj
    clusterId: string

  AMQPBasicAckObj = object of AMQPBasicDeliveryTagObj
    flags: AMQPBasicAckBits

  AMQPBasicRejectObj = object of AMQPBasicDeliveryTagObj
    flags: AMQPBasicRequeueBits

  AMQPBasicRecoverObj = object of RootObj
    flags: AMQPBasicRequeueBits
  
  AMQPBasicNackObj = object of AMQPBasicDeliveryTagObj
    flags: AMQPBasicNackBits
  
  AMQPBasic* = ref AMQPBasicObj
  AMQPBasicObj* = object of RootObj
    case kind*: AMQPBasicKind
    of AMQP_BASIC_QOS_SUBMETHOD:
      qos: AMQPBasicQOSObj
    of AMQP_BASIC_QOS_OK_SUBMETHOD, AMQP_BASIC_RECOVER_OK_SUBMETHOD:
      discard
    of AMQP_BASIC_CONSUME_SUBMETHOD:
      consume: AMQPBasicConsumeObj
    of AMQP_BASIC_CONSUME_OK_SUBMETHOD, AMQP_BASIC_CANCEL_OK_SUBMETHOD:
      consumeCancelOk: AMQPBasicConsumerTagObj
    of AMQP_BASIC_CANCEL_SUBMETHOD:
      cancel: AMQPBasicCancelObj
    of AMQP_BASIC_PUBLISH_SUBMETHOD:
      publish: AMQPBasicPublishObj
    of AMQP_BASIC_RETURN_SUBMETHOD:
      ret: AMQPBasicReturnObj
    of AMQP_BASIC_DELIVER_SUBMETHOD:
      deliver: AMQPBasicDeliverObj
    of AMQP_BASIC_GET_SUBMETHOD:
      get: AMQPBasicGetObj
    of AMQP_BASIC_GET_OK_SUBMETHOD:
      getOk: AMQPBasicGetOkObj
    of AMQP_BASIC_GET_EMPTY_SUBMETHOD:
      getEmpty: AMQPBasicGetEmptyObj
    of AMQP_BASIC_ACK_SUBMETHOD:
      ack: AMQPBasicAckObj
    of AMQP_BASIC_REJECT_SUBMETHOD:
      reject: AMQPBasicRejectObj
    of AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, AMQP_BASIC_RECOVER_SUBMETHOD:
      recover: AMQPBasicRecoverObj
    of AMQP_BASIC_NACK_SUBMETHOD:
      nack: AMQPBasicNackObj
    else:
      discard

proc len*(meth: AMQPBasic): int =
  result = 0
  case meth.kind:
  of AMQP_BASIC_QOS_SUBMETHOD:
    result.inc(sizeInt32Uint32+sizeInt16Uint16+sizeInt8Uint8)
  of AMQP_BASIC_QOS_OK_SUBMETHOD, AMQP_BASIC_RECOVER_OK_SUBMETHOD:
    result.inc(0)
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.consume.queue.len+sizeInt8Uint8)
    result.inc(meth.consume.consumerTag.len+sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
    result.inc(meth.consume.args.len)
  of AMQP_BASIC_CONSUME_OK_SUBMETHOD:
    result.inc(meth.consume.consumerTag.len+sizeInt8Uint8)
  of AMQP_BASIC_CANCEL_SUBMETHOD:
    result.inc(meth.cancel.consumerTag.len+sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
  of AMQP_BASIC_CANCEL_OK_SUBMETHOD:
    result.inc(meth.cancel.consumerTag.len+sizeInt8Uint8)
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.publish.exchange.len+sizeInt8Uint8)
    result.inc(meth.publish.routingKey.len+sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
  of AMQP_BASIC_RETURN_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.ret.replyText.len+sizeInt8Uint8)
    result.inc(meth.ret.exchange.len+sizeInt8Uint8)
    result.inc(meth.ret.routingKey.len+sizeInt8Uint8)
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result.inc(meth.deliver.consumerTag.len+sizeInt8Uint8)
    result.inc(sizeInt64Uint64)
    result.inc(sizeInt8Uint8)
    result.inc(meth.deliver.exchange.len+sizeInt8Uint8)
    result.inc(meth.deliver.routingKey.len+sizeInt8Uint8)
  of AMQP_BASIC_GET_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.get.queue.len+sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result.inc(sizeInt64Uint64+sizeInt8Uint8)
    result.inc(meth.getOk.exchange.len+sizeInt8Uint8)
    result.inc(meth.getOk.routingKey.len+sizeInt8Uint8)
    result.inc(sizeInt32Uint32)
  of AMQP_BASIC_GET_EMPTY_SUBMETHOD:
    result.inc(meth.getEmpty.clusterId.len+sizeInt8Uint8)
  of AMQP_BASIC_ACK_SUBMETHOD:
    result.inc(sizeInt64Uint64+sizeInt8Uint8)
  of AMQP_BASIC_REJECT_SUBMETHOD:
    result.inc(sizeInt64Uint64+sizeInt8Uint8)
  of AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, AMQP_BASIC_RECOVER_SUBMETHOD:
    result.inc(sizeInt8Uint8)
  of AMQP_BASIC_NACK_SUBMETHOD:
    result.inc(sizeInt64Uint64+sizeInt8Uint8)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc decode*(_: typedesc[AMQPBasic], s: AsyncBufferedSocket, t: uint32): Future[AMQPBasic] {.async.} =
  case t:
  of BASIC_QOS_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_QOS_SUBMETHOD, qos: AMQPBasicQOSObj())
    result.qos.prefetchSize = await s.readBEU32()
    result.qos.prefetchCount = await s.readBEU16()
    result.qos.flags = cast[AMQPBasicQOSBits](await s.readU8())
  of BASIC_QOS_OK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_QOS_OK_SUBMETHOD)
  of BASIC_CONSUME_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_CONSUME_SUBMETHOD, consume: AMQPBasicConsumeObj())
    result.consume.ticket = await s.readBEU16()
    result.consume.queue = await s.decodeShortString()
    result.consume.consumerTag = await s.decodeShortString()
    result.consume.flags = cast[AMQPBasicConsumeBits](await s.readU8())
    result.consume.args = await s.decodeTable()
  of BASIC_CONSUME_OK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_CONSUME_OK_SUBMETHOD, consumeCancelOk: AMQPBasicConsumerTagObj())
    result.consumeCancelOk.consumerTag = await s.decodeShortString()
  of BASIC_CANCEL_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_CANCEL_SUBMETHOD, cancel: AMQPBasicCancelObj())
    result.cancel.consumerTag = await s.decodeShortString()
    result.cancel.flags = cast[AMQPBasicCancelBits](await s.readU8())
  of BASIC_CANCEL_OK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_CANCEL_OK_SUBMETHOD, consumeCancelOk: AMQPBasicConsumerTagObj())
    result.consumeCancelOk.consumerTag = await s.decodeShortString()
  of BASIC_PUBLISH_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_PUBLISH_SUBMETHOD, publish: AMQPBasicPublishObj())
    result.publish.ticket = await s.readBEU16()
    result.publish.exchange = await s.decodeShortString()
    result.publish.routingKey = await s.decodeShortString()
    result.publish.flags = cast[AMQPBasicPublishBits](await s.readU8())
  of BASIC_RETURN_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_RETURN_SUBMETHOD, ret: AMQPBasicReturnObj())
    result.ret.replyCode = await s.readBEU16()
    result.ret.replyText = await s.decodeShortString()
    result.ret.exchange = await s.decodeShortString()
    result.ret.routingKey = await s.decodeShortString()
  of BASIC_DELIVER_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_DELIVER_SUBMETHOD, deliver: AMQPBasicDeliverObj())
    result.deliver.consumerTag = await s.decodeShortString()
    result.deliver.deliveryTag = await s.readBEU64()
    result.deliver.flags = cast[AMQPBasicRedeliveredBits](await s.readU8())
    result.deliver.exchange = await s.decodeShortString()
    result.deliver.routingKey = await s.decodeShortString()
  of BASIC_GET_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_GET_SUBMETHOD, get: AMQPBasicGetObj())
    result.get.ticket = await s.readBEU16()
    result.get.queue = await s.decodeShortString()
    result.get.flags = cast[AMQPBasicGetBits](await s.readU8())
  of BASIC_GET_OK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_GET_OK_SUBMETHOD, getOk: AMQPBasicGetOkObj())
    result.getOk.deliveryTag = await s.readBEU64()
    result.getOk.flags = cast[AMQPBasicRedeliveredBits](await s.readU8())
    result.getOk.exchange = await s.decodeShortString()
    result.getOk.routingKey = await s.decodeShortString()
    result.getOk.messageCount = await s.readBEU32()
  of BASIC_GET_EMPTY_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_GET_EMPTY_SUBMETHOD, getEmpty: AMQPBasicGetEmptyObj())
    result.getEmpty.clusterId = await s.decodeShortString()
  of BASIC_ACK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_ACK_SUBMETHOD, ack: AMQPBasicAckObj())
    result.ack.deliveryTag = await s.readBEU64()
    result.ack.flags = cast[AMQPBasicAckBits](await s.readU8())
  of BASIC_REJECT_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_REJECT_SUBMETHOD, reject: AMQPBasicRejectObj())
    result.reject.deliveryTag = await s.readBEU64()
    result.reject.flags = cast[AMQPBasicRequeueBits](await s.readU8())
  of BASIC_RECOVER_ASYNC_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, recover: AMQPBasicRecoverObj())
    result.recover.flags = cast[AMQPBasicRequeueBits](await s.readU8())
  of BASIC_RECOVER_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_RECOVER_SUBMETHOD, recover: AMQPBasicRecoverObj())
    result.recover.flags = cast[AMQPBasicRequeueBits](await s.readU8())
  of BASIC_RECOVER_OK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_RECOVER_OK_SUBMETHOD)
  of BASIC_NACK_METHOD_ID:
    result = AMQPBasic(kind: AMQP_BASIC_NACK_SUBMETHOD, nack: AMQPBasicNackObj())
    result.nack.deliveryTag = await s.readBEU64()
    result.nack.flags = cast[AMQPBasicNackBits](await s.readU8())
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")
  
proc encode*(meth: AMQPBasic, dst: AsyncBufferedSocket) {.async.} =
  echo $meth.kind
  case meth.kind:
  of AMQP_BASIC_QOS_SUBMETHOD:
    await dst.writeBE(meth.qos.prefetchSize)
    await dst.writeBE(meth.qos.prefetchCount)
    await dst.write(cast[uint8](meth.qos.flags))
  of AMQP_BASIC_QOS_OK_SUBMETHOD, AMQP_BASIC_RECOVER_OK_SUBMETHOD:
    discard
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    await dst.writeBE(meth.consume.ticket)
    await dst.encodeShortString(meth.consume.queue)
    await dst.encodeShortString(meth.consume.consumerTag)
    await dst.write(cast[uint8](meth.consume.flags))
    await dst.encodeTable(meth.consume.args)
  of AMQP_BASIC_CONSUME_OK_SUBMETHOD, AMQP_BASIC_CANCEL_OK_SUBMETHOD:
    await dst.encodeShortString(meth.consumeCancelOk.consumerTag)
  of AMQP_BASIC_CANCEL_SUBMETHOD:
    await dst.encodeShortString(meth.cancel.consumerTag)
    await dst.write(cast[uint8](meth.cancel.flags))
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    await dst.writeBE(meth.publish.ticket)
    await dst.encodeShortString(meth.publish.exchange)
    await dst.encodeShortString(meth.publish.routingKey)
    await dst.write(cast[uint8](meth.publish.flags))
  of AMQP_BASIC_RETURN_SUBMETHOD:
    await dst.writeBE(meth.ret.replyCode)
    await dst.encodeShortString(meth.ret.replyText)
    await dst.encodeShortString(meth.ret.exchange)
    await dst.encodeShortString(meth.ret.routingKey)
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    await dst.encodeShortString(meth.deliver.consumerTag)
    await dst.writeBE(meth.deliver.deliveryTag)
    await dst.write(cast[uint8](meth.deliver.flags))
    await dst.encodeShortString(meth.deliver.exchange)
    await dst.encodeShortString(meth.deliver.routingKey)
  of AMQP_BASIC_GET_SUBMETHOD:
    await dst.writeBE(meth.get.ticket)
    await dst.encodeShortString(meth.get.queue)
    await dst.write(cast[uint8](meth.get.flags))
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    await dst.writeBE(meth.getOk.deliveryTag)
    await dst.write(cast[uint8](meth.getOk.flags))
    await dst.encodeShortString(meth.getOk.exchange)
    await dst.encodeShortString(meth.getOk.routingKey)
    await dst.writeBE(meth.getOk.messageCount)
  of AMQP_BASIC_GET_EMPTY_SUBMETHOD:
    await dst.encodeShortString(meth.getEmpty.clusterId)
  of AMQP_BASIC_ACK_SUBMETHOD:
    await dst.writeBE(meth.ack.deliveryTag)
    await dst.write(cast[uint8](meth.ack.flags))
  of AMQP_BASIC_REJECT_SUBMETHOD:
    await dst.writeBE(meth.reject.deliveryTag)
    await dst.write(cast[uint8](meth.reject.flags))
  of AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, AMQP_BASIC_RECOVER_SUBMETHOD:
    await dst.write(cast[uint8](meth.recover.flags))
  of AMQP_BASIC_NACK_SUBMETHOD:
    await dst.writeBE(meth.nack.deliveryTag)
    await dst.write(cast[uint8](meth.nack.flags))
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")


proc newBasicQos*(prefetchSize: uint32, prefetchCount: uint16, globalQos: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_QOS_SUBMETHOD,
    qos: AMQPBasicQOSObj(
      prefetchSize: prefetchSize,
      prefetchCount: prefetchCount,
      flags: AMQPBasicQOSBits(
        global: globalQos
      )
    )
  )

proc newBasicQosOk*(): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_QOS_OK_SUBMETHOD
  )

proc newBasicConsume*(ticket: uint16, queue, consumerTag: string, noLocal, noAck, exclusive, noWait: bool, args: FieldTable): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_CONSUME_SUBMETHOD,
    consume: AMQPBasicConsumeObj(
      ticket: ticket,
      queue: queue,
      consumerTag: consumerTag,
      args: args,
      flags: AMQPBasicConsumeBits(
        noLocal: noLocal,
        noAck: noAck,
        exclusive: exclusive,
        noWait: noWait
      )
    )
  )

proc newBasicConsumeOk*(consumerTag: string): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_CONSUME_OK_SUBMETHOD,
    consumeCancelOk: AMQPBasicConsumerTagObj(
      consumerTag: consumerTag
    )
  )

proc newBasicCancel*(consumerTag: string, noWait: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_CANCEL_SUBMETHOD,
    cancel: AMQPBasicCancelObj(
      consumerTag: consumerTag,
      flags: AMQPBasicCancelBits(
        noWait: noWait
      )
    )
  )

proc newBasicCancelOk*(consumerTag: string): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_CANCEL_OK_SUBMETHOD,
    consumeCancelOk: AMQPBasicConsumerTagObj(
      consumerTag: consumerTag
    )
  )

proc newBasicPublish*(ticket: uint16, exchange, routingKey: string, mandatory, immediate: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_PUBLISH_SUBMETHOD,
    publish: AMQPBasicPublishObj(
      ticket: ticket,
      exchange: exchange,
      routingKey: routingKey,
      flags: AMQPBasicPublishBits(
        mandatory: mandatory,
        immediate: immediate
      )
    )
  )

proc newBasicReturn*(replyCode: uint16, replyText, exchange, routingKey: string): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_RETURN_SUBMETHOD,
    ret: AMQPBasicReturnObj(
      replyCode: replyCode,
      replyText: replyText,
      exchange: exchange,
      routingKey: routingKey
    )
  )

proc newBasicDeliver*(consumerTag: string, deliveryTag: uint64, redelivered: bool, exchange, routingKey: string): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_DELIVER_SUBMETHOD,
    deliver: AMQPBasicDeliverObj(
      consumerTag: consumerTag,
      deliveryTag: deliveryTag,
      flags: AMQPBasicRedeliveredBits(
        redelivered: redelivered
      ),
      exchange: exchange,
      routingKey: routingKey
    )
  )

proc newBasicGet*(ticket: uint16, queue: string, noAck: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_GET_SUBMETHOD,
    get: AMQPBasicGetObj(
      ticket: ticket,
      queue: queue,
      flags: AMQPBasicGetBits(
        noAck: noAck
      )
    )
  )

proc newBasicGetOk*(deliveryTag: uint64, redelivered: bool, exchange, routingKey: string, messageCount: uint32): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_GET_OK_SUBMETHOD,
    getOk: AMQPBasicGetOkObj(
      deliveryTag: deliveryTag,
      flags: AMQPBasicRedeliveredBits(
        redelivered: redelivered
      ),
      exchange: exchange,
      routingKey: routingKey,
      messageCount: messageCount
    )
  )

proc newBasicGetEmpty*(clusterId: string): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_GET_EMPTY_SUBMETHOD,
    getEmpty: AMQPBasicGetEmptyObj(
      clusterId: clusterId
    )
  )

proc newBasicAck*(deliveryTag: uint64, multiple: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_ACK_SUBMETHOD,
    ack: AMQPBasicAckObj(
      deliveryTag: deliveryTag,
      flags: AMQPBasicAckBits(
        multiple: multiple
      )
    )
  )

proc newBasicReject*(deliveryTag: uint64, requeue: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_REJECT_SUBMETHOD,
    reject: AMQPBasicRejectObj(
      deliveryTag: deliveryTag,
      flags: AMQPBasicRequeueBits(
        requeue: requeue
      )
    )
  )

proc newBasicRecoverAsync*(requeue: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD,
    recover: AMQPBasicRecoverObj(
      flags: AMQPBasicRequeueBits(
        requeue: requeue
      )
    )
  )

proc newBasicRecover*(requeue: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_RECOVER_SUBMETHOD,
    recover: AMQPBasicRecoverObj(
      flags: AMQPBasicRequeueBits(
        requeue: requeue
      )
    )
  )

proc newBasicRecoverOk*(): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_RECOVER_OK_SUBMETHOD
  )

proc newBasicNack*(deliveryTag: uint64, multiple, requeue: bool): AMQPBasic =
  result = AMQPBasic(
    kind: AMQP_BASIC_NACK_SUBMETHOD,
    nack: AMQPBasicNackObj(
      deliveryTag: deliveryTag,
      flags: AMQPBasicNackBits(
        multiple: multiple,
        requeue: requeue
      )
    )
  )

proc consumerTag*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.consumerTag
  of AMQP_BASIC_CONSUME_OK_SUBMETHOD, AMQP_BASIC_CANCEL_OK_SUBMETHOD:
    result = self.consumeCancelOk.consumerTag
  of AMQP_BASIC_CANCEL_SUBMETHOD:
    result = self.cancel.consumerTag
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result = self.deliver.consumerTag
  else:
    raise newException(FieldDefect, "No such field")

proc ticket*(self: AMQPBasic): uint16 =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.ticket
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result = self.publish.ticket
  of AMQP_BASIC_GET_SUBMETHOD:
    result = self.get.ticket
  else:
    raise newException(FieldDefect, "No such field")

proc queue*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.queue
  of AMQP_BASIC_GET_SUBMETHOD:
    result = self.get.queue
  else:
    raise newException(FieldDefect, "No such field")

proc args*(self: AMQPBasic): FieldTable =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.args
  else:
    raise newException(FieldDefect, "No such field")

proc exchange*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result = self.publish.exchange
  of AMQP_BASIC_RETURN_SUBMETHOD:
    result = self.ret.exchange
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result = self.deliver.exchange
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result = self.getOk.exchange
  else:
    raise newException(FieldDefect, "No such field")

proc routingKey*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result = self.publish.routingKey
  of AMQP_BASIC_RETURN_SUBMETHOD:
    result = self.ret.routingKey
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result = self.deliver.routingKey
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result = self.getOk.routingKey
  else:
    raise newException(FieldDefect, "No such field")

proc replyCode*(self: AMQPBasic): uint16 =
  case self.kind
  of AMQP_BASIC_RETURN_SUBMETHOD:
    result = self.ret.replyCode
  else:
    raise newException(FieldDefect, "No such field")

proc replyText*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_RETURN_SUBMETHOD:
    result = self.ret.replyText
  else:
    raise newException(FieldDefect, "No such field")

proc deliveryTag*(self: AMQPBasic): uint64 =
  case self.kind
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result = self.deliver.deliveryTag
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result = self.getOk.deliveryTag
  of AMQP_BASIC_ACK_SUBMETHOD:
    result = self.ack.deliveryTag
  of AMQP_BASIC_REJECT_SUBMETHOD:
    result = self.reject.deliveryTag
  of AMQP_BASIC_NACK_SUBMETHOD:
    result = self.nack.deliveryTag
  else:
    raise newException(FieldDefect, "No such field")

proc messageCount*(self: AMQPBasic): uint32 =
  case self.kind
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result = self.getOk.messageCount
  else:
    raise newException(FieldDefect, "No such field")

proc clusterId*(self: AMQPBasic): string =
  case self.kind
  of AMQP_BASIC_GET_EMPTY_SUBMETHOD:
    result = self.getEmpty.clusterId
  else:
    raise newException(FieldDefect, "No such field")

proc global*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_QOS_SUBMETHOD:
    result = self.qos.flags.global
  else:
    raise newException(FieldDefect, "No such field")

proc `global=`*(self: AMQPBasic, global: bool) =
  case self.kind
  of AMQP_BASIC_QOS_SUBMETHOD:
    self.qos.flags.global = global
  else:
    raise newException(FieldDefect, "No such field")

proc noLocal*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.flags.noLocal
  else:
    raise newException(FieldDefect, "No such field")

proc `noLocal=`*(self: AMQPBasic, noLocal: bool) =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    self.consume.flags.noLocal = noLocal
  else:
    raise newException(FieldDefect, "No such field")

proc noAck*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.flags.noAck
  of AMQP_BASIC_GET_SUBMETHOD:
    result = self.get.flags.noAck
  else:
    raise newException(FieldDefect, "No such field")

proc `noAck=`*(self: AMQPBasic, noAck: bool) =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    self.consume.flags.noAck = noAck
  of AMQP_BASIC_GET_SUBMETHOD:
    self.get.flags.noAck = noAck
  else:
    raise newException(FieldDefect, "No such field")

proc exclusive*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.flags.exclusive
  else:
    raise newException(FieldDefect, "No such field")

proc `exclusive=`*(self: AMQPBasic, exclusive: bool) =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    self.consume.flags.exclusive = exclusive
  else:
    raise newException(FieldDefect, "No such field")

proc noWait*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    result = self.consume.flags.noWait
  of AMQP_BASIC_CANCEL_SUBMETHOD:
    result = self.cancel.flags.noWait
  else:
    raise newException(FieldDefect, "No such field")

proc `noWait=`*(self: AMQPBasic, noWait: bool) =
  case self.kind
  of AMQP_BASIC_CONSUME_SUBMETHOD:
    self.consume.flags.noWait = noWait
  of AMQP_BASIC_CANCEL_SUBMETHOD:
    self.cancel.flags.noWait = noWait
  else:
    raise newException(FieldDefect, "No such field")

proc mandatory*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result = self.publish.flags.mandatory
  else:
    raise newException(FieldDefect, "No such field")

proc `mandatory=`*(self: AMQPBasic, mandatory: bool) =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    self.publish.flags.mandatory = mandatory
  else:
    raise newException(FieldDefect, "No such field")

proc immediate*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    result = self.publish.flags.immediate
  else:
    raise newException(FieldDefect, "No such field")

proc `immediate=`*(self: AMQPBasic, immediate: bool) =
  case self.kind
  of AMQP_BASIC_PUBLISH_SUBMETHOD:
    self.publish.flags.immediate = immediate
  else:
    raise newException(FieldDefect, "No such field")

proc redelivered*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    result = self.deliver.flags.redelivered
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    result = self.getOk.flags.redelivered
  else:
    raise newException(FieldDefect, "No such field")

proc `redelivered=`*(self: AMQPBasic, redelivered: bool) =
  case self.kind
  of AMQP_BASIC_DELIVER_SUBMETHOD:
    self.deliver.flags.redelivered = redelivered
  of AMQP_BASIC_GET_OK_SUBMETHOD:
    self.getOk.flags.redelivered = redelivered
  else:
    raise newException(FieldDefect, "No such field")

proc multiple*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_ACK_SUBMETHOD:
    result = self.ack.flags.multiple
  of AMQP_BASIC_NACK_SUBMETHOD:
    result = self.nack.flags.multiple
  else:
    raise newException(FieldDefect, "No such field")

proc `multiple=`*(self: AMQPBasic, multiple: bool) =
  case self.kind
  of AMQP_BASIC_ACK_SUBMETHOD:
    self.ack.flags.multiple = multiple
  of AMQP_BASIC_NACK_SUBMETHOD:
    self.nack.flags.multiple = multiple
  else:
    raise newException(FieldDefect, "No such field")

proc requeue*(self: AMQPBasic): bool =
  case self.kind
  of AMQP_BASIC_REJECT_SUBMETHOD:
    result = self.reject.flags.requeue
  of AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, AMQP_BASIC_RECOVER_SUBMETHOD:
    result = self.recover.flags.requeue
  of AMQP_BASIC_NACK_SUBMETHOD:
    result = self.nack.flags.requeue
  else:
    raise newException(FieldDefect, "No such field")

proc `requeue=`*(self: AMQPBasic, requeue: bool) =
  case self.kind
  of AMQP_BASIC_REJECT_SUBMETHOD:
    self.reject.flags.requeue = requeue
  of AMQP_BASIC_RECOVER_ASYNC_SUBMETHOD, AMQP_BASIC_RECOVER_SUBMETHOD:
    self.recover.flags.requeue = requeue
  of AMQP_BASIC_NACK_SUBMETHOD:
    self.nack.flags.requeue = requeue
  else:
    raise newException(FieldDefect, "No such field")
