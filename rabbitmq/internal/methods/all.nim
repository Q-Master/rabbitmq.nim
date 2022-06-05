import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../util/endians
import ../field
import ../exceptions
import ./connection
import ./channel
import ./basic

export connection, channel, basic

const NO_SUCH_METHOD_STR = "No such method"

type
  AMQPMethods* = enum
    AMQP_CONNECTION_START_METHOD = CONNECTION_START_METHOD_ID
    AMQP_CONNECTION_START_OK_METHOD = CONNECTION_START_OK_METHOD_ID
    AMQP_CONNECTION_SECURE_METHOD = CONNECTION_SECURE_METHOD_ID
    AMQP_CONNECTION_SECURE_OK_METHOD = CONNECTION_SECURE_OK_METHOD_ID
    AMQP_CONNECTION_TUNE_METHOD = CONNECTION_TUNE_METHOD_ID
    AMQP_CONNECTION_TUNE_OK_METHOD = CONNECTION_TUNE_OK_METHOD_ID
    AMQP_CONNECTION_OPEN_METHOD = CONNECTION_OPEN_METHOD_ID
    AMQP_CONNECTION_OPEN_OK_METHOD = CONNECTION_OPEN_OK_METHOD_ID
    AMQP_CONNECTION_CLOSE_METHOD = CONNECTION_CLOSE_METHOD_ID
    AMQP_CONNECTION_CLOSE_OK_METHOD = CONNECTION_CLOSE_OK_METHOD_ID
    AMQP_CONNECTION_BLOCKED_METHOD = CONNECTION_BLOCKED_METHOD_ID
    AMQP_CONNECTION_UNBLOCKED_METHOD = CONNECTION_UNBLOCKED_METHOD_ID

    AMQP_CHANNEL_OPEN_METHOD = CHANNEL_OPEN_METHOD_ID
    AMQP_CHANNEL_OPEN_OK_METHOD = CHANNEL_OPEN_OK_METHOD_ID
    AMQP_CHANNEL_FLOW_METHOD = CHANNEL_FLOW_METHOD_ID
    AMQP_CHANNEL_FLOW_OK_METHOD = CHANNEL_FLOW_OK_METHOD_ID
    AMQP_CHANNEL_CLOSE_METHOD = CHANNEL_CLOSE_METHOD_ID
    AMQP_CHANNEL_CLOSE_OK_METHOD = CHANNEL_CLOSE_OK_METHOD_ID

  AMQPMetodKind* = enum
    NONE = 0
    CONNECTION = CONNECTION_METHODS
    CHANNEL = CHANNEL_METHODS
    BASIC = BASIC_METHODS

  AMQPMethod* = ref AMQPMethodObj
  AMQPMethodObj* = object of RootObj
    methodId*: uint32
    case kind*: AMQPMetodKind
    of CONNECTION:
      connObj*: AMQPConnection
    of CHANNEL:
      channelObj*: AMQPChannel
    of BASIC:
      basicObj*: AMQPBasic
    else:
      discard

proc newMethod*(id: uint32): AMQPMethod =
  let (idHi, _) = uint32touints16(id)
  result = AMQPMethod(kind: AMQPMetodKind(idHi), methodId: id)

proc len*(meth: AMQPMethod): int = 
  result = sizeInt32Uint32
  case meth.kind
  of CONNECTION:
    result.inc(meth.connObj.len)
  of CHANNEL:
    result.inc(meth.channelObj.len)
  of BASIC:
    result.inc(meth.basicObj.len)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc decodeMethod*(src: AsyncBufferedSocket): Future[AMQPMethod] {.async.} =
  let methodId = await src.readBEU32()
  let meth = newMethod(methodId)
  result = meth
  case meth.kind
  of CONNECTION:
    result.connObj = await AMQPConnection.decode(src, methodId)
  of CHANNEL:
    result.channelObj = await AMQPChannel.decode(src, methodId)
  of BASIC:
    result.basicObj = await AMQPBasic.decode(src, methodId)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc encodeMethod*(dst: AsyncBufferedSocket, meth: AMQPMethod) {.async.} =
  await dst.writeBE(meth.methodId)
  case meth.kind
  of CONNECTION:
    await meth.connObj.encode(dst)
  of CHANNEL:
    await meth.channelObj.encode(dst)
  of BASIC:
    await meth.basicObj.encode(dst)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)
  #await dst.flush()

#--

#-- Connection

proc newConnectionStartMethod*(major, minor: uint8, serverprops: FieldTable, mechanisms, locales: string): AMQPMethod =
  result = newMethod(CONNECTION_START_METHOD_ID)
  result.connObj = newConnectionStart(major, minor, serverprops, mechanisms, locales)

proc newConnectionStartOkMethod*(clientProps: FieldTable, mechanism="PLAIN", response="", locale="en_US"): AMQPMethod =
  result = newMethod(CONNECTION_START_OK_METHOD_ID)
  result.connObj = newConnectionStartOk(clientProps, mechanism, response, locale)

proc newConnectionTuneMethod*(channelMax: uint16, frameMax: uint32, heartbeat: uint16): AMQPMethod =
  result = newMethod(CONNECTION_TUNE_METHOD_ID)
  result.connObj = newConnectionTune(channelMax, frameMax, heartbeat)

proc newConnectionTuneOkMethod*(channelMax: uint16, frameMax: uint32, heartbeat: uint16): AMQPMethod =
  result = newMethod(CONNECTION_TUNE_OK_METHOD_ID)
  result.connObj = newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc newConnectionOpenMethod*(virtualHost: string, caps: string, insist: bool): AMQPMethod =
  result = newMethod(CONNECTION_OPEN_METHOD_ID)
  result.connObj = newConnectionOpen(virtualHost, caps, insist)

proc newConnectionOpenOkMethod*(knownHosts: string): AMQPMethod =
  result = newMethod(CONNECTION_OPEN_OK_METHOD_ID)
  result.connObj = newConnectionOpenOk(knownHosts)

proc newConnectionCloseMethod*(replyCode: uint16, replyText: string, classId: uint16 = 0.uint16, methodId: uint16 = 0.uint16): AMQPMethod =
  result = newMethod(CONNECTION_CLOSE_METHOD_ID)
  result.connObj = newConnectionClose(replyCode, replyText, classId, methodId)

proc newConnectionCloseOkMethod*(): AMQPMethod =
  result = newMethod(CONNECTION_CLOSE_OK_METHOD_ID)
  result.connObj = newConnectionCloseOk()

proc newConnectionBlockedMethod*(reason: string): AMQPMethod =
  result = newMethod(CONNECTION_BLOCKED_METHOD_ID)
  result.connObj = newConnectionBlocked(reason)

proc newConnectionUnblockedMethod*(): AMQPMethod =
  result = newMethod(CONNECTION_UNBLOCKED_METHOD_ID)
  result.connObj = newConnectionUnblocked()

#-- Channel

proc newChannelOpenMethod*(outOfBand: string): AMQPMethod =
  result = newMethod(CHANNEL_OPEN_METHOD_ID)
  result.channelObj = newChannelOpen(outOfBand)

proc newChannelOpenOkMethod*(channelId: string): AMQPMethod =
  result = newMethod(CHANNEL_OPEN_OK_METHOD_ID)
  result.channelObj = newChannelOpenOk(channelId)

proc newChannelFlowMethod*(active: bool): AMQPMethod =
  result = newMethod(CHANNEL_FLOW_METHOD_ID)
  result.channelObj = newChannelFlow(active)

proc newChannelFlowOkMethod*(active: bool): AMQPMethod =
  result = newMethod(CHANNEL_FLOW_OK_METHOD_ID)
  result.channelObj = newChannelFlowOk(active)

proc newChannelCloseMethod*(replyCode: uint16, replyText: string, classId: uint16, methodId: uint16): AMQPMethod =
  result = newMethod(CHANNEL_CLOSE_METHOD_ID)
  result.channelObj = newChannelClose(replyCode, replyText, classId, methodId)

proc newChannelCloseMethod*(): AMQPMethod =
  result = newMethod(CHANNEL_CLOSE_OK_METHOD_ID)
  result.channelObj = newChannelCloseOk()

#-- Basic

proc newBasicQosMethod*(prefetchSize: uint32, prefetchCount: uint16, globalQos: bool): AMQPMethod =
  result = newMethod(BASIC_QOS_METHOD_ID)
  result.basicObj = newBasicQos(prefetchSize, prefetchCount, globalQos)

proc newBasicQosOkMethod*(): AMQPMethod =
  result = newMethod(BASIC_QOS_OK_METHOD_ID)
  result.basicObj = newBasicQosOk()

proc newBasicConsumeMethod*(ticket: uint16, queue, consumerTag: string, noLocal, noAck, exclusive, noWait: bool, args: FieldTable): AMQPMethod =
  result = newMethod(BASIC_CONSUME_METHOD_ID)
  result.basicObj = newBasicConsume(ticket, queue, consumerTag, noLocal, noAck, exclusive, noWait, args)

proc newBasicConsumeOkMethod*(consumerTag: string): AMQPMethod =
  result = newMethod(BASIC_CONSUME_OK_METHOD_ID)
  result.basicObj = newBasicConsumeOk(consumerTag)

proc newBasicCancelMethod*(consumerTag: string, noWait: bool): AMQPMethod =
  result = newMethod(BASIC_CANCEL_METHOD_ID)
  result.basicObj = newBasicCancel(consumerTag, noWait)

proc newBasicCancelOkMethod*(consumerTag=""): AMQPMethod =
  result = newMethod(BASIC_CANCEL_OK_METHOD_ID)
  result.basicObj = newBasicCancelOk(consumerTag)

proc newBasicPublishMethod*(ticket: uint16, exchange, routingKey: string, mandatory, immediate: bool): AMQPMethod =
  result = newMethod(BASIC_PUBLISH_METHOD_ID)
  result.basicObj = newBasicPublish(ticket, exchange, routingKey, mandatory, immediate)

proc newBasicReturnMethod*(replyCode: uint16, replyText, exchange, routingKey: string): AMQPMethod =
  result = newMethod(BASIC_RETURN_METHOD_ID)
  result.basicObj = newBasicReturn(replyCode, replyText, exchange, routingKey)

proc newBasicDeliverMethod*(consumerTag: string, deliveryTag: uint64, redelivered: bool, exchange, routingKey: string): AMQPMethod =
  result = newMethod(BASIC_DELIVER_METHOD_ID)
  result.basicObj = newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc newBasicGetMethod*(ticket: uint16, queue: string, noAck: bool): AMQPMethod =
  result = newMethod(BASIC_GET_METHOD_ID)
  result.basicObj = newBasicGet(ticket, queue, noAck)

proc newBasicGetOkMethod*(deliveryTag: uint64, redelivered: bool, exchange, routingKey: string, messageCount: uint32): AMQPMethod =
  result = newMethod(BASIC_GET_OK_METHOD_ID)
  result.basicObj = newBasicGetOk(deliveryTag, redelivered, exchange, routingKey, messageCount)

proc newBasicGetEmptyMethod*(clusterId: string): AMQPMethod =
  result = newMethod(BASIC_GET_EMPTY_METHOD_ID)
  result.basicObj = newBasicGetEmpty(clusterId)

proc newBasicAckMethod*(deliveryTag: uint64, multiple: bool): AMQPMethod =
  result = newMethod(BASIC_ACK_METHOD_ID)
  result.basicObj = newBasicAck(deliveryTag, multiple)

proc newBasicRejectMethod*(deliveryTag: uint64, requeue: bool): AMQPMethod =
  result = newMethod(BASIC_REJECT_METHOD_ID)
  result.basicObj = newBasicReject(deliveryTag, requeue)

proc newBasicRecoverAsyncMethod*(requeue: bool): AMQPMethod =
  result = newMethod(BASIC_RECOVER_ASYNC_METHOD_ID)
  result.basicObj = newBasicRecoverAsync(requeue)

proc newBasicRecoverMethod*(requeue: bool): AMQPMethod =
  result = newMethod(BASIC_RECOVER_METHOD_ID)
  result.basicObj = newBasicRecover(requeue)

proc newBasicRecoverOkMethod*(): AMQPMethod =
  result = newMethod(BASIC_RECOVER_OK_METHOD_ID)
  result.basicObj = newBasicRecoverOk()

proc newBasicNackMethod*(deliveryTag: uint64, multiple, requeue: bool): AMQPMethod =
  result = newMethod(BASIC_NACK_METHOD_ID)
  result.basicObj = newBasicNack(deliveryTag, multiple, requeue)
