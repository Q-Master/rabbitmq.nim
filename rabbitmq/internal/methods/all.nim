import std/[asyncdispatch]
import pkg/networkutils/buffered_socket
import ../util/endians
import ../field
import ../exceptions
import ./connection
import ./channel
import ./basic
import ./access
import ./confirm
import ./exchange
import ./queue
import ./tx

export connection, channel, basic, access, confirm, exchange, queue, tx

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

    AMQP_ACCESS_REQUEST_METHOD = ACCESS_REQUEST_METHOD_ID
    AMQP_ACCESS_REQUEST_OK_METHOD = ACCESS_REQUEST_OK_METHOD_ID

    AMQP_EXCHANGE_DECLARE_METHOD = EXCHANGE_DECLARE_METHOD_ID
    AMQP_EXCHANGE_DECLARE_OK_METHOD = EXCHANGE_DECLARE_OK_METHOD_ID
    AMQP_EXCHANGE_DELETE_METHOD = EXCHANGE_DELETE_METHOD_ID
    AMQP_EXCHANGE_DELETE_OK_METHOD = EXCHANGE_DELETE_OK_METHOD_ID
    AMQP_EXCHANGE_BIND_METHOD = EXCHANGE_BIND_METHOD_ID
    AMQP_EXCHANGE_BIND_OK_METHOD = EXCHANGE_BIND_OK_METHOD_ID
    AMQP_EXCHANGE_UNBIND_METHOD = EXCHANGE_UNBIND_METHOD_ID
    AMQP_EXCHANGE_UNBIND_OK_METHOD = EXCHANGE_UNBIND_OK_METHOD_ID

    AMQP_QUEUE_DECLARE_METHOD = QUEUE_DECLARE_METHOD_ID
    AMQP_QUEUE_DECLARE_OK_METHOD = QUEUE_DECLARE_OK_METHOD_ID
    AMQP_QUEUE_BIND_METHOD = QUEUE_BIND_METHOD_ID
    AMQP_QUEUE_BIND_OK_METHOD = QUEUE_BIND_OK_METHOD_ID
    AMQP_QUEUE_PURGE_METHOD = QUEUE_PURGE_METHOD_ID
    AMQP_QUEUE_PURGE_OK_METHOD = QUEUE_PURGE_OK_METHOD_ID
    AMQP_QUEUE_DELETE_METHOD = QUEUE_DELETE_METHOD_ID
    AMQP_QUEUE_DELETE_OK_METHOD = QUEUE_DELETE_OK_METHOD_ID
    AMQP_QUEUE_UNBIND_METHOD = QUEUE_UNBIND_METHOD_ID
    AMQP_QUEUE_UNBIND_OK_METHOD = QUEUE_UNBIND_OK_METHOD_ID

    AMQP_BASIC_QOS_METHOD = BASIC_QOS_METHOD_ID
    AMQP_BASIC_QOS_OK_METHOD = BASIC_QOS_OK_METHOD_ID
    AMQP_BASIC_CONSUME_METHOD = BASIC_CONSUME_METHOD_ID
    AMQP_BASIC_CONSUME_OK_METHOD = BASIC_CONSUME_OK_METHOD_ID
    AMQP_BASIC_CANCEL_METHOD = BASIC_CANCEL_METHOD_ID
    AMQP_BASIC_CANCEL_OK_METHOD = BASIC_CANCEL_OK_METHOD_ID
    AMQP_BASIC_PUBLISH_METHOD = BASIC_PUBLISH_METHOD_ID
    AMQP_BASIC_RETURN_METHOD = BASIC_RETURN_METHOD_ID
    AMQP_BASIC_DELIVER_METHOD = BASIC_DELIVER_METHOD_ID
    AMQP_BASIC_GET_METHOD = BASIC_GET_METHOD_ID
    AMQP_BASIC_GET_OK_METHOD = BASIC_GET_OK_METHOD_ID
    AMQP_BASIC_GET_EMPTY_METHOD = BASIC_GET_EMPTY_METHOD_ID
    AMQP_BASIC_ACK_METHOD = BASIC_ACK_METHOD_ID
    AMQP_BASIC_REJECT_METHOD = BASIC_REJECT_METHOD_ID
    AMQP_BASIC_RECOVER_ASYNC_METHOD = BASIC_RECOVER_ASYNC_METHOD_ID
    AMQP_BASIC_RECOVER_METHOD = BASIC_RECOVER_METHOD_ID
    AMQP_BASIC_RECOVER_OK_METHOD = BASIC_RECOVER_OK_METHOD_ID
    AMQP_BASIC_NACK_METHOD = BASIC_NACK_METHOD_ID

    AMQP_CONFIRM_SELECT_METHOD = CONFIRM_SELECT_METHOD_ID
    AMQP_CONFIRM_SELECT_OK_METHOD = CONFIRM_SELECT_OK_METHOD_ID

    AMQP_TX_SELECT_METHOD = TX_SELECT_METHOD_ID
    AMQP_TX_SELECT_OK_METHOD = TX_SELECT_OK_METHOD_ID
    AMQP_TX_COMMIT_METHOD = TX_COMMIT_METHOD_ID
    AMQP_TX_COMMIT_OK_METHOD = TX_COMMIT_OK_METHOD_ID
    AMQP_TX_ROLLBACK_METHOD = TX_ROLLBACK_METHOD_ID
    AMQP_TX_ROLLBACK_OK_METHOD = TX_ROLLBACK_OK_METHOD_ID

  AMQPMetodKind* = enum
    NONE = 0
    CONNECTION = CONNECTION_METHODS
    CHANNEL = CHANNEL_METHODS
    ACCESS = ACCESS_METHODS
    EXCHANGE = EXCHANGE_METHODS
    QUEUE = QUEUE_METHODS
    BASIC = BASIC_METHODS
    CONFIRM = CONFIRM_METHODS
    TX = TX_METHODS

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
    of ACCESS:
      accessObj*: AMQPAccess
    of CONFIRM:
      confirmObj*: AMQPConfirm
    of EXCHANGE:
      exchangeObj*: AMQPExchange
    of QUEUE:
      queueObj*: AMQPQueue
    of TX:
      txObj*: AMQPTx
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
  of ACCESS:
    result.inc(meth.accessObj.len)
  of CONFIRM:
    result.inc(meth.confirmObj.len)
  of EXCHANGE:
    result.inc(meth.exchangeObj.len)
  of QUEUE:
    result.inc(meth.queueObj.len)
  of TX:
    result.inc(meth.txObj.len)
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
  of ACCESS:
    result.accessObj = await AMQPAccess.decode(src, methodId)
  of CONFIRM:
    result.confirmObj = await AMQPConfirm.decode(src, methodId)
  of EXCHANGE:
    result.exchangeObj = await AMQPExchange.decode(src, methodId)
  of QUEUE:
    result.queueObj = await AMQPQueue.decode(src, methodId)
  of TX:
    result.txObj = await AMQPTx.decode(src, methodId)
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
  of ACCESS:
    await meth.accessObj.encode(dst)
  of CONFIRM:
    await meth.confirmObj.encode(dst)
  of EXCHANGE:
    await meth.exchangeObj.encode(dst)
  of QUEUE:
    await meth.queueObj.encode(dst)
  of TX:
    await meth.txObj.encode(dst)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)
  #await dst.flush()

proc idToAMQPMethod*(meth: AMQPMethod): AMQPMethods = AMQPMethods(meth.methodId)

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

proc newBasicConsumeMethod*(queue, consumerTag: string, noLocal, noAck, exclusive, noWait: bool, args: FieldTable): AMQPMethod =
  result = newMethod(BASIC_CONSUME_METHOD_ID)
  result.basicObj = newBasicConsume(queue, consumerTag, noLocal, noAck, exclusive, noWait, args)

proc newBasicConsumeOkMethod*(consumerTag: string): AMQPMethod =
  result = newMethod(BASIC_CONSUME_OK_METHOD_ID)
  result.basicObj = newBasicConsumeOk(consumerTag)

proc newBasicCancelMethod*(consumerTag: string, noWait: bool): AMQPMethod =
  result = newMethod(BASIC_CANCEL_METHOD_ID)
  result.basicObj = newBasicCancel(consumerTag, noWait)

proc newBasicCancelOkMethod*(consumerTag=""): AMQPMethod =
  result = newMethod(BASIC_CANCEL_OK_METHOD_ID)
  result.basicObj = newBasicCancelOk(consumerTag)

proc newBasicPublishMethod*(exchange, routingKey: string, mandatory, immediate: bool): AMQPMethod =
  result = newMethod(BASIC_PUBLISH_METHOD_ID)
  result.basicObj = newBasicPublish(exchange, routingKey, mandatory, immediate)

proc newBasicReturnMethod*(replyCode: uint16, replyText, exchange, routingKey: string): AMQPMethod =
  result = newMethod(BASIC_RETURN_METHOD_ID)
  result.basicObj = newBasicReturn(replyCode, replyText, exchange, routingKey)

proc newBasicDeliverMethod*(consumerTag: string, deliveryTag: uint64, redelivered: bool, exchange, routingKey: string): AMQPMethod =
  result = newMethod(BASIC_DELIVER_METHOD_ID)
  result.basicObj = newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc newBasicGetMethod*(queue: string, noAck: bool): AMQPMethod =
  result = newMethod(BASIC_GET_METHOD_ID)
  result.basicObj = newBasicGet(queue, noAck)

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

#-- Basic

proc newAccessRequestMethod*(realm: string, exclusive, passive, active, write, read: bool): AMQPMethod =
  result = newMethod(ACCESS_REQUEST_METHOD_ID)
  result.accessObj = newAccessRequest(realm, exclusive, passive, active, write, read)

proc newAccessRequestOkMethod*(ticket: uint16): AMQPMethod =
  result = newMethod(ACCESS_REQUEST_OK_METHOD_ID)
  result.accessObj = newAccessRequestOk(ticket)

#-- Confirm

proc newConfirmSelectMethod*(noWait: bool): AMQPMethod =
  result = newMethod(CONFIRM_SELECT_METHOD_ID)
  result.confirmObj = newConfirmSelect(noWait)

proc neConfirmSelectOkMethod*(): AMQPMethod =
  result = newMethod(CONFIRM_SELECT_OK_METHOD_ID)
  result.confirmObj = newConfirmSelectOk()

#-- Exchange

proc newExchangeDeclareMethod*(
  exchange: string, eType: AMQPExchangeType, 
  passive, durable, autoDelete, internal, noWait: bool, 
  args: FieldTable): AMQPMethod =
  result = newMethod(EXCHANGE_DECLARE_METHOD_ID)
  result.exchangeObj = newExchangeDeclare(exchange, eType, passive, durable, autoDelete, internal, noWait, args)

proc newExchangeDeclareOkMethod*(): AMQPMethod =
  result = newMethod(EXCHANGE_DECLARE_OK_METHOD_ID)
  result.exchangeObj = newExchangeDeclareOk()

proc newExchangeDeleteMethod*(exchange: string, ifUnused, noWait: bool): AMQPMethod =
  result = newMethod(EXCHANGE_DELETE_METHOD_ID)
  result.exchangeObj = newExchangeDelete(exchange, ifUnused, noWait)

proc newExchangeDeleteOkMethod*(): AMQPMethod =
  result = newMethod(EXCHANGE_DELETE_OK_METHOD_ID)
  result.exchangeObj = newExchangeDeleteOk()

proc newExchangeBindMethod*(destination, source, routingKey: string, noWait: bool, args: FieldTable): AMQPMethod =
  result = newMethod(EXCHANGE_BIND_METHOD_ID)
  result.exchangeObj = newExchangeBind(destination, source, routingKey, noWait, args)

proc newExchangeBindOkMethod*(): AMQPMethod =
  result = newMethod(EXCHANGE_BIND_OK_METHOD_ID)
  result.exchangeObj = newExchangeBindOk()

proc newExchangeUnbindMethod*(destination, source, routingKey: string, noWait: bool, args: FieldTable): AMQPMethod =
  result = newMethod(EXCHANGE_UNBIND_METHOD_ID)
  result.exchangeObj = newExchangeUnbind(destination, source, routingKey, noWait, args)

proc newExchangeUnbindOkMethod*(): AMQPMethod =
  result = newMethod(EXCHANGE_UNBIND_OK_METHOD_ID)
  result.exchangeObj = newExchangeUnbindOk()

#-- Queue

proc newQueueDeclareMethod*(queue: string, 
  passive, durable, exclusive, autoDelete, noWait: bool, 
  args: FieldTable): AMQPMethod =
  result = newMethod(QUEUE_DECLARE_METHOD_ID)
  result.queueObj = newQueueDeclare(queue, passive, durable, exclusive, autoDelete, noWait, args)

proc newQueueDeclareOkMethod*(queue: string, messageCount, consumerCount: uint32): AMQPMethod =
  result = newMethod(QUEUE_DECLARE_METHOD_ID)
  result.queueObj = newQueueDeclareOk(queue, messageCount, consumerCount)

proc newQueueBindMethod*(queue, exchange, routingKey: string, noWait: bool, args: FieldTable): AMQPMethod =
  result = newMethod(QUEUE_BIND_METHOD_ID)
  result.queueObj = newQueueBind(queue, exchange, routingKey, noWait, args)

proc newQueueBindOkMethod*(): AMQPMethod =
  result = newMethod(QUEUE_BIND_OK_METHOD_ID)
  result.queueObj = newQueueBindOk()

proc newQueuePurgeMethod*(queue: string, noWait: bool): AMQPMethod =
  result = newMethod(QUEUE_PURGE_METHOD_ID)
  result.queueObj = newQueuePurge(queue, noWait)

proc newQueuePurgeOkMethod*(messageCount: uint32): AMQPMethod =
  result = newMethod(QUEUE_PURGE_OK_METHOD_ID)
  result.queueObj = newQueuePurgeOk(messageCount)

proc newQueueDeleteMethod*(queue: string, ifUnused, ifEmpty, noWait: bool): AMQPMethod =
  result = newMethod(QUEUE_DELETE_METHOD_ID)
  result.queueObj = newQueueDelete(queue, ifUnused, ifEmpty, noWait)

proc newQueueDeleteOkMethod*(messageCount: uint32): AMQPMethod =
  result = newMethod(QUEUE_DELETE_OK_METHOD_ID)
  result.queueObj = newQueueDeleteOk(messageCount)

proc newQueueUnbindMethod*(queue, exchange, routingKey: string, args: FieldTable): AMQPMethod =
  result = newMethod(QUEUE_UNBIND_METHOD_ID)
  result.queueObj = newQueueUnbind(queue, exchange, routingKey, args)

proc newQueueUnbindOkMethod*(): AMQPMethod =
  result = newMethod(QUEUE_UNBIND_OK_METHOD_ID)
  result.queueObj = newQueueUnbindOk()

#-- TX

proc newTxSelectMethod*(): AMQPMethod =
  result = newMethod(TX_SELECT_METHOD_ID)
  result.txObj = newTxSelect()

proc newTxSelectOkMethod*(): AMQPMethod =
  result = newMethod(TX_SELECT_OK_METHOD_ID)
  result.txObj = newTxSelectOk()

proc newTxCommitMethod*(): AMQPMethod =
  result = newMethod(TX_COMMIT_METHOD_ID)
  result.txObj = newTxCommit()

proc newTxCommitOkMethod*(): AMQPMethod =
  result = newMethod(TX_COMMIT_OK_METHOD_ID)
  result.txObj = newTxCommitOk()

proc newTxRollbackMethod*(): AMQPMethod =
  result = newMethod(TX_ROLLBACK_METHOD_ID)
  result.txObj = newTxRollback()

proc newTxRollbackOkMethod*(): AMQPMethod =
  result = newMethod(TX_ROLLBACK_OK_METHOD_ID)
  result.txObj = newTxRollbackOk()
