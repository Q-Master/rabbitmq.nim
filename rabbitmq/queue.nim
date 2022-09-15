import std/[asyncdispatch]
import ./internal/methods/all
import ./internal/[exceptions, field]
import ./connection
import ./exchange

type
  Queue* = ref QueueObj
  QueueObj* = object of RootObj
    id: string
    channel: Channel

proc queueDeclare*(
  channel: Channel, queue: string,
  passive = false, durable = false, exclusive = false, autoDelete = false, noWait = false,
  args: FieldTable = nil
  ): Future[Queue] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  let res = await channel.rpc(
    newQueueDeclareMethod(
      queue, passive, durable, exclusive, autoDelete, noWait, args
    ), 
    @[AMQP_QUEUE_DECLARE_OK_METHOD],
    noWait = noWait
  )
  if noWait:
    result = Queue(id: queue, channel: channel)
  else:
    result = Queue(id: res.meth.queueObj.queue, channel: channel)

proc delete*(queue: Queue, ifUnused = false, ifEmpty = false, noWait = false): Future[uint32] {.async.} =
  let res = await queue.channel.rpc(
    newQueueDeleteMethod(
      queue.id, ifUnused, ifEmpty, noWait
    ), 
    @[AMQP_QUEUE_DELETE_OK_METHOD],
    noWait = noWait
  )
  result = if noWait: 0.uint32 else: res.meth.queueObj.messageCount

proc queueBind*(queue: Queue, exchange: Exchange, routingKey: string = "", noWait = false, args: FieldTable = nil): Future[bool] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  var routingKey = routingKey
  if routingKey == "":
    routingKey = queue.id
  try:
    let res {.used.} = await queue.channel.rpc(
      newQueueBindMethod(
        queue.id, exchange.id, routingKey, noWait, args
      ), 
      @[AMQP_QUEUE_BIND_OK_METHOD],
      noWait = noWait
    )
    result = true
  except AMQPNotFound:
    result = false

proc unbind*(queue: Queue, exchange: Exchange, routingKey: string = "", args: FieldTable = nil): Future[bool] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  var routingKey = routingKey
  if routingKey == "":
    routingKey = queue.id
  try:
    let res {.used.} = await queue.channel.rpc(
      newQueueUnbindMethod(
        queue.id, exchange.id, routingKey, args
      ), 
      @[AMQP_QUEUE_UNBIND_OK_METHOD]
    )
    result = true
  except AMQPNotFound:
    result = false

proc purge*(queue: Queue, noWait = false): Future[uint32] {.async.} =
  let res = await queue.channel.rpc(
      newQueuePurgeMethod(
        queue.id, noWait
      ), 
      @[AMQP_QUEUE_PURGE_OK_METHOD],
      noWait = noWait
    )
  result = if noWait: 0.uint32 else: res.meth.queueObj.messageCount

proc channel*(queue: Queue): Channel = queue.channel
proc id*(queue: Queue): string = queue.id