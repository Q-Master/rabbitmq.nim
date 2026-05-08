import std/[asyncdispatch]
import ./internal/methods/all
import ./internal/[exceptions, field]
import ./[connection, queue, exchange, message, consumertag]


proc qos*(channel: AMQPChannel, prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicQosMethod(
      prefetchSize, prefetchCount, globalQos
    ), 
    @[AMQP_BASIC_QOS_OK_METHOD]
  )


proc consume*(
  queue: Queue,
  cb: ConsumerTagCB = nil,
  noAck=false,
  exclusive=false,
  args: FieldTable=nil,
  consumerTag: string = "",
  noLocal=false,
  noWait=false
  ): Future[ConsumerTag] {.async.} =
  let res = await queue.channel.rpc(
    newBasicConsumeMethod(
      queue.id, consumerTag,
      noLocal, noAck, exclusive, noWait,
      if args.isNil: newFieldTable() else: args
    ), 
    @[AMQP_BASIC_CONSUME_OK_METHOD],
    noWait = noWait
  )
  if cb.isNil:
    raise newException(ValueError, "Either consumer tag callback must present")
  result = newConsumerTag(res.meth.basicObj.consumerTag, cb, noAck)
  queue.channel.addConsumer(result)


proc cancel*(ch: AMQPChannel, consumerTag: ConsumerTag, noWait=false): Future[ConsumerTag] {.async.} =
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
  if not exchange.channel.active or exchange.channel.blocked:
    let waiter = newFuture[void]("Waiting for flow to activate")
    exchange.channel.addOnFlow(waiter)
    await waiter
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


proc ack*(channel: AMQPChannel, deliveryTag: uint64, multiple: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicAckMethod(deliveryTag, multiple),
    @[]
  )


proc reject*(channel: AMQPChannel, deliveryTag: uint64, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicRejectMethod(deliveryTag, requeue),
    @[]
  )


proc nack*(channel: AMQPChannel, deliveryTag: uint64, multiple: bool = false, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicNackMethod(deliveryTag, multiple, requeue),
    @[]
  )


proc recover*(channel: AMQPChannel, requeue: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicRecoverMethod(requeue),
    @[AMQP_BASIC_RECOVER_OK_METHOD]
  )


proc queue*(consumerTag: ConsumerTag): Queue = consumerTag.queue
