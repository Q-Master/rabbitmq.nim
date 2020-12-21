import tables
import ./methods/access
import ./methods/basic
import ./methods/channel
import ./methods/confirm
import ./methods/connection
import ./methods/exchange
import ./methods/queue
import ./methods/tx
import ./exceptions
import ./data
import ./streams
import ./spec

export access.AccessMethod, basic.BasicMethod, channel.ChannelMethod, confirm.ConfirmMethod, connection.ConnectionMethod, exchange.ExchangeMethod, queue.QueueMethod

const NO_SUCH_METHOD_STR = "No such method"
const WRONG_METHOD_STR = "Wrong method"

type
  MethodVariants = enum
    NONE = 0
    CONNECTION_METHODS = connection.CONNECTION_METHODS
    CHANNEL_METHODS = channel.CHANNEL_METHODS
    ACCESS_METHODS = access.ACCESS_METHODS
    EXCHANGE_METHODS = exchange.EXCHANGE_METHODS
    QUEUE_METHODS = queue.QUEUE_METHODS
    BASIC_METHODS = basic.BASIC_METHODS
    CONFIRM_METHODS = confirm.CONFIRM_METHODS
    TX_METHODS = tx.TX_METHODS
  Method* = ref MethodObj
  MethodObj* = object
    name*: string
    validResponses*: seq[uint16]
    syncronous*: bool
    case indexHi*: MethodVariants
    of ACCESS_METHODS:
      access*: AccessMethod
    of BASIC_METHODS:
      basic*: BasicMethod
    of CHANNEL_METHODS:
      channel*: ChannelMethod
    of CONFIRM_METHODS:
      confirm*: ConfirmMethod
    of CONNECTION_METHODS:
      connection*: ConnectionMethod
    of EXCHANGE_METHODS:
      exchange*: ExchangeMethod
    of QUEUE_METHODS:
      queue*: QueueMethod
    of TX_METHODS:
      tx*: TxMethod
    else:
      discard

proc newMethod(index: uint32): (uint16, Method) =
  let (indexHi, indexLo) = uint32touints16(index)
  case MethodVariants(indexHi)
  of ACCESS_METHODS:
    result = (indexLo, Method(indexHi: ACCESS_METHODS))
  of BASIC_METHODS:
    result = (indexLo, Method(indexHi: BASIC_METHODS))
  of CHANNEL_METHODS:
    result = (indexLo, Method(indexHi: CHANNEL_METHODS))
  of CONFIRM_METHODS:
    result = (indexLo, Method(indexHi: CONFIRM_METHODS))
  of CONNECTION_METHODS:
    result = (indexLo, Method(indexHi: CONNECTION_METHODS))
  of EXCHANGE_METHODS:
    result = (indexLo, Method(indexHi: EXCHANGE_METHODS))
  of QUEUE_METHODS:
    result = (indexLo, Method(indexHi: QUEUE_METHODS))
  of TX_METHODS:
    result = (indexLo, Method(indexHi: TX_METHODS))
  else:
      raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc decodeMethod*(encoded: InputStream): Method =
  let (_, methodId) = encoded.readBigEndianU32()
  let (submethodId, meth) = newMethod(methodId)
  result = meth
  case meth.indexHi
  of ACCESS_METHODS:
    (meth.syncronous, meth.validResponses, meth.access) = AccessMethod.decode(AccessVariants(submethodId), encoded)
  of BASIC_METHODS:
    (meth.syncronous, meth.validResponses, meth.basic) = BasicMethod.decode(BasicVariants(submethodId), encoded)
  of CHANNEL_METHODS:
    (meth.syncronous, meth.validResponses, meth.channel) = ChannelMethod.decode(ChannelVariants(submethodId), encoded)
  of CONFIRM_METHODS:
    (meth.syncronous, meth.validResponses, meth.confirm) = ConfirmMethod.decode(ConfirmVariants(submethodId), encoded)
  of CONNECTION_METHODS:
    (meth.syncronous, meth.validResponses, meth.connection) = ConnectionMethod.decode(ConnectionVariants(submethodId), encoded)
  of EXCHANGE_METHODS:
    (meth.syncronous, meth.validResponses, meth.exchange) = ExchangeMethod.decode(ExchangeVariants(submethodId), encoded)
  of QUEUE_METHODS:
    (meth.syncronous, meth.validResponses, meth.queue) = QueueMethod.decode(QueueVariants(submethodId), encoded)
  of TX_METHODS:
    (meth.syncronous, meth.validResponses, meth.tx) = TxMethod.decode(TxVariants(submethodId), encoded)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

template encodeIndex(to: OutputStream, idxHi: uint16, idxLo: uint16) =
  let idx = uints16touint32(idxHi, idxLo)
  to.writeBigEndian32(idx)

proc encodeMethod*(to: OutputStream, m: Method) =
  case m.indexHi
  of ACCESS_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.access.indexLo).uint16)
    to.encode(m.access)
  of BASIC_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.basic.indexLo).uint16)
    to.encode(m.basic)
  of CHANNEL_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.channel.indexLo).uint16)
    to.encode(m.channel)
  of CONFIRM_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.confirm.indexLo).uint16)
    to.encode(m.confirm)
  of CONNECTION_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.connection.indexLo).uint16)
    to.encode(m.connection)
  of EXCHANGE_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.exchange.indexLo).uint16)
    to.encode(m.exchange)
  of QUEUE_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.queue.indexLo).uint16)
    to.encode(m.queue)
  of TX_METHODS:
    to.encodeIndex(ord(m.indexHi).uint16, ord(m.tx.indexLo).uint16)
    to.encode(m.tx)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

#----------------------- Access -----------------------#

proc newAccessRequest*(realm="/data", exclusive=false, passive=true, active=true, write=true, read=true): Method =
  result = Method(indexHi: ACCESS_METHODS)
  (result.syncronous, result.validResponses, result.access) = access.newAccessRequest(realm, exclusive, passive, active, write, read)

proc newAccessRequestOk*(ticket = 1.uint16): Method =
  result = Method(indexHi: ACCESS_METHODS)
  (result.syncronous, result.validResponses, result.access) = access.newAccessRequestOk(ticket)

#----------------------- Basic -----------------------#

proc newBasicQos*(prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicQos(prefetchSize, prefetchCount, globalQos)

proc newBasicQosOk*(): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicQosOk()

proc newBasicConsume*(
  ticket=0.uint16, 
  queue="", 
  consumerTag="", 
  noLocal=false, 
  noAck=false, 
  exclusive=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicConsume(ticket, queue, consumerTag, noLocal, noAck, exclusive, noWait, arguments)

proc newBasicConsumeOk*(consumerTag=""): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicConsumeOk(consumerTag)

proc newBasicCancel*(consumerTag="", noWait=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicCancel(consumerTag, noWait)

proc newBasicCancelOk*(consumerTag=""): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicCancelOk(consumerTag)

proc newBasicPublish*(ticket=0.uint16, exchange="", routingKey="", mandatory=false, immediate=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicPublish(ticket, exchange, routingKey, mandatory, immediate)

proc newBasicReturn*(replyCode=0.uint16, replyText="", exchange="", routingKey=""): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicReturn(replyCode, replyText, exchange, routingKey)

proc newBasicDeliver*(consumerTag="", deliveryTag=0.uint64, redelivered=false, exchange="", routingKey=""): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey)

proc newBasicGet*(ticket=0.uint16, queue="", noAck=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicGet(ticket, queue, noAck)

proc newBasicGetOk*(deliveryTag=0.uint64, redelivered=false, exchange="", routingKey="", messageCount=0.uint32): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicGetOk(deliveryTag, redelivered, exchange, routingKey, messageCount)

proc newBasicGetEmpty*(clusterId=""): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicGetEmpty(clusterId)

proc newBasicAck*(deliveryTag=0.uint64, multiple=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicAck(deliveryTag, multiple)

proc newBasicReject*(deliveryTag=0.uint64, requeue=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicReject(deliveryTag, requeue)

proc newBasicRecoverAsync*(requeue=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicRecoverAsync(requeue)

proc newBasicRecover*(requeue=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicRecover(requeue)

proc newBasicRecoverOk*(): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicRecoverOk()

proc newBasicNack*(deliveryTag=0.uint64, multiple=false, requeue=false): Method =
  result = Method(indexHi: BASIC_METHODS)
  (result.syncronous, result.validResponses, result.basic) = basic.newBasicNack(deliveryTag, multiple, requeue)

#----------------------- Channel -----------------------#

proc newChannelOpen*(outOfBand = ""): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelOpen(outOfBand)

proc newChannelOpenOk*(channelId = ""): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelOpenOk(channelId)

proc newChannelFlow*(active = false): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelFlow(active)

proc newChannelFlowOk*(active = false): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelFlowOk(active)

proc newChannelClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelClose(replyCode, replyText, classId, methodId)

proc newChannelCloseOk*(): Method =
  result = Method(indexHi: CHANNEL_METHODS)
  (result.syncronous, result.validResponses, result.channel) = channel.newChannelCloseOk()

#----------------------- Confirm -----------------------#

proc newConfirmSelect*(noWait=false): Method =
  result = Method(indexHi: CONFIRM_METHODS)
  (result.syncronous, result.validResponses, result.confirm) = confirm.newConfirmSelect(noWait)

proc newConfirmSelectOk*(): Method =
  result = Method(indexHi: CONFIRM_METHODS)
  (result.syncronous, result.validResponses, result.confirm) = confirm.newConfirmSelectOk()

#----------------------- Connection -----------------------#

proc newConnectionStart*(major = PROTOCOL_VERSION[0], minor = PROTOCOL_VERSION[1], properties: TableRef[string, DataTable]=nil, mechanisms="PLAIN", locales="en_US"): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionStart(major, minor, properties, mechanisms, locales)

proc newConnectionStartOk*(clientProps: TableRef[string, DataTable] = nil, mechanisms="PLAIN", response="", locales="en_US"): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionStartOk(clientProps, mechanisms, response, locales)

proc newConnectionSecure*(challenge: string = ""): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionSecure(challenge)

proc newConnectionSecureOk*(response: string = ""): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionSecureOk(response)

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionTune(channelMax, frameMax, heartbeat)

proc newConnectionTuneOk*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc newConnectionOpen*(virtualHost = "/", capabilities = "", insist = false): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionOpen(virtualHost, capabilities, insist)

proc newConnectionOpenOk*(knownHosts = ""): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionOpenOk(knownHosts)

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionClose(replyCode, replyText, classId, methodId)

proc newConnectionCloseOk*(): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionCloseOk()

proc newConnectionBlocked*(reason = ""): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionBlocked(reason)

proc newConnectionUnblocked*(): Method =
  result = Method(indexHi: CONNECTION_METHODS)
  (result.syncronous, result.validResponses, result.connection) = connection.newConnectionUnblocked()

#----------------------- Exchange -----------------------#

proc newExchangeDeclare*(
  ticket = 0.uint16, 
  exchangeName="", 
  etype="direct", 
  passive=false, 
  durable=false, 
  autoDelete=false, 
  internal=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable]=nil): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeDeclare(ticket, exchangeName, etype, passive, durable, autoDelete, internal, noWait, arguments)
proc newExchangeDeclareOk*(): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeDeclareOk()

proc newExchangeDelete*(ticket = 0.uint16, exchangeName = "", ifUnused = false, noWait = false): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeDelete(ticket, exchangeName, ifUnused, noWait)

proc newExchangeDeleteOk*(): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeDeleteOk()

proc newExchangeBind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeBind(ticket, destination, source, routingKey, noWait, arguments)

proc newExchangeBindOk*(): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeBindOk()

proc newExchangeUnbind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeUnbind(ticket, destination, source, routingKey, noWait, arguments)

proc newExchangeUnbindOk*(): Method =
  result = Method(indexHi: EXCHANGE_METHODS)
  (result.syncronous, result.validResponses, result.exchange) = exchange.newExchangeUnbindOk()

#----------------------- Queue -----------------------#

proc newQueueDeclare*(
  ticket = 0.uint16, 
  queueName="", 
  passive=false, 
  durable=false, 
  exclusive=false,
  autoDelete=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable]=nil): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueDeclare(ticket, queueName, passive, durable, exclusive, autoDelete, noWait, arguments)

proc newQueueDeclareOk*(queueName="", messageCount=0.uint32, consumerCount=0.uint32): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueDeclareOk(queueName, messageCount, consumerCount)

proc newQueueBind*(
  ticket = 0.uint16, 
  queueName = "", 
  bQueue = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueBind(ticket, queueName, bQueue, routingKey, noWait, arguments)

proc newQueueBindOk*(): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueBindOk()

proc newQueuePurge*(ticket = 0.uint16, queueName="", noWait=false): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueuePurge(ticket, queueName, noWait)

proc newQueuePurgeOk*(messageCount = 0.uint32): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueuePurgeOk(messageCount)

proc newQueueDelete*(ticket = 0.uint16, queueName="", ifUnused=false, ifEmpty=false, noWait=false): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueDelete(ticket, queueName, ifUnused, ifEmpty, noWait)

proc newQueueDeleteOk*(messageCount = 0.uint32): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueDeleteOk()

proc newQueueUnbind*(
  ticket = 0.uint16, 
  queueName = "", 
  bQueue = "", 
  routingKey = "", 
  arguments: TableRef[string, DataTable] = nil): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueUnbind(ticket, queueName, bQueue, routingKey, arguments)

proc newQueueUnbindOk*(): Method =
  result = Method(indexHi: QUEUE_METHODS)
  (result.syncronous, result.validResponses, result.queue) = queue.newQueueUnbindOk()

#----------------------- TX -----------------------#

proc newTxSelect*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxSelect()

proc newTxSelectOk*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxSelectOk()

proc newTxCommit*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxCommit()

proc newTxCommitOk*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxCommitOk()

proc newTxRollback*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxRollback()

proc newTxRollbackOk*(): Method =
  result = Method(indexHi: TX_METHODS)
  (result.syncronous, result.validResponses, result.tx) = tx.newTxRollbackOk()
