import std/[asyncdispatch]
import ./internal/methods/all
import ./internal/[exceptions, field]
import ./connection
import ./queue
import ./exchange
import ./message
import ./consumertag

proc qos*(channel: Channel, prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicQosMethod(
      prefetchSize, prefetchCount, globalQos
    ), 
    @[AMQP_BASIC_QOS_OK_METHOD]
  )

proc consume*(
  queue: Queue, consumerTag: ConsumerTag = nil, 
  noLocal=false, noAck=false, exclusive=false, noWait=false, 
  args: FieldTable=nil
  ): Future[ConsumerTag] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  let res = await queue.channel.rpc(
    newBasicConsumeMethod(
      queue.id, consumerTag.id,
      noLocal, noAck, exclusive, noWait,
      args
    ), 
    @[AMQP_BASIC_CONSUME_OK_METHOD],
    noWait = noWait
  )
  result = newConsumerTag(res.meth.basicObj.consumerTag)
  queue.channel.addConsumer(result)

proc cancel*(ch: Channel, consumerTag: ConsumerTag, noWait=false): Future[ConsumerTag] {.async.} =
  let res = await ch.rpc(
    newBasicCancelMethod(
      consumerTag.id, noWait
    ), 
    @[AMQP_BASIC_CANCEL_OK_METHOD],
    noWait = noWait
  )
  if not res.isNil:
    if res.meth.basicObj.consumerTag != consumerTag.id:
      raise newException(AMQPCommandInvalid, "Consumer tag differs")
  result = consumerTag
  ch.removeConsumer(consumerTag)

proc publish*(
  exchange: Exchange, routingKey: string, msg: Message, 
  mandatory=false, immediate=false
  ) {.async.} =
  let res {.used.} = await exchange.channel.rpc(
    newBasicPublishMethod(exchange.id, routingKey, mandatory, immediate), 
    @[],
    payload = msg
  )

proc get*(queue: Queue, noAck=false): Future[Envelope] {.async.} =
  let res = await queue.channel.rpc(
    newBasicGetMethod(
      queue.id, noAck
    ), 
    @[AMQP_BASIC_GET_OK_METHOD, AMQP_BASIC_GET_EMPTY_METHOD]
  )
  if res.meth.idToAMQPMethod == AMQP_BASIC_GET_OK_METHOD:
    result = Envelope(
      msg: res.msg,
      consumerTag: res.meth.basicObj.consumerTag,
      deliveryTag: res.meth.basicObj.deliveryTag,
      exchange: res.meth.basicObj.exchange,
      routingKey: res.meth.basicObj.routingKey,
      redelivered: res.meth.basicObj.redelivered
    )
  else:
    result = nil

proc ack*(channel: Channel, deliveryTag: uint64, multiple: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicAckMethod(deliveryTag, multiple),
    @[]
  )

proc reject*(channel: Channel, deliveryTag: uint64, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicRejectMethod(deliveryTag, requeue),
    @[]
  )

proc nack*(channel: Channel, deliveryTag: uint64, multiple: bool = false, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicNackMethod(deliveryTag, multiple, requeue),
    @[]
  )

proc recover*(channel: Channel, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicRecoverMethod(requeue),
    @[AMQP_BASIC_RECOVER_OK_METHOD]
  )

proc queue*(consumerTag: ConsumerTag): Queue = consumerTag.queue
