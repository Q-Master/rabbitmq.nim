import std/[asyncdispatch, times]
import ./internal/methods/all
import ./internal/[exceptions, field]
import ./connection
import ./exchange

type
  Queue* = ref QueueObj
  QueueObj* = object of RootObj
    id: string
    channel: Channel

  OverflowKind* = enum
    DROP_HEAD = "drop-head"
    REJECT_PUBLISH = "reject-publish"
    REJECT_PUBLISH_DLX = "reject-publish-dlx"

  QueueTypeKind* = enum
    CLASSIC_QUEUE = "classic"
    QORUM_QUEUE = "quorum"
    STREAM_QUEUE = "stream"

  StreamFromKind* = enum
    FROM_FIRST = "first"
    FROM_LAST = "last"
    FROM_NEXT = "next"

  QueueModeKind* = enum
    DEFAULT_QUEUE = "default"
    LAZY_QUEUE = "lazy"

  QueueDeadLetterStrategyKind* = enum
    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"

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

template checkArgsNil(args: FieldTable): untyped =
  if args.isNil:
    result = newFieldTable()
  else:
    result = args

proc `messageTTL=`*(args: FieldTable, ms: int64): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-message-ttl"] = ms.uint64.asField

proc `messageTTL=`*(args: FieldTable, ms: Duration): FieldTable {.discardable.} = args.messageTTL = ms.inMilliseconds

proc `expires=`*(args: FieldTable, ms: int64): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-expires"] = ms.uint64.asField

proc `expires=`*(args: FieldTable, ms: Duration): FieldTable {.discardable.} = args.expires = ms.inMilliseconds

proc `overflow=`*(args: FieldTable, behaviour: OverflowKind): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-overflow"] = ($behaviour).asField

proc `singleActiveConsumer=`*(args: FieldTable, value: bool): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-single-active-consumer"] = value.asField

proc `deadLetterExchange=`*(args: FieldTable, exchange: string): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-dead-letter-exchange"] = exchange.asField

proc `deadLetterExchange=`*(args: FieldTable, exchange: Exchange): FieldTable {.discardable.} = args.deadLetterExchange = exchange.id

proc `deadLetterRoutingKey=`*(args: FieldTable, routingKey: string): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-dead-letter-routing-key"] = routingKey.asField

proc `maxLength=`*(args: FieldTable, maxLength: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-max-length"] = maxLength.asField

proc `maxLengthBytes=`*(args: FieldTable, maxLengthBytes: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-max-length-bytes"] = maxLengthBytes.asField

proc `maxPriority=`*(args: FieldTable, maxPriority: uint8): FieldTable {.discardable.} =
  if maxPriority > 10:
    raise newException(ValueError, $maxPriority & " maximum priority is not recommended")
  checkArgsNil(args)
  result["x-max-priority"] = maxPriority.asField

proc `queueMode=`*(args: FieldTable, mode: QueueModeKind): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-queue-mode"] = ($mode).asField

proc `deadLetterStrategy=`*(args: FieldTable, strategy: QueueDeadLetterStrategyKind): FieldTable {.discardable.} =
  checkArgsNil(args)
  result["x-dead-letter-strategy"] = ($strategy).asField

const xQueueType = "x-queue-type"

proc `queueType=`*(args: FieldTable, queueType: QueueTypeKind): FieldTable {.discardable.} =
  checkArgsNil(args)
  result[xQueueType] = ($queueType).asField

template checkQueueType(args: FieldTable, queueType: QueueTypeKind): untyped =
  let qt = args.getOrDefault(xQueueType, nil)
  if qt.isNil or (qt.kind == dtString and qt.stringVal != $queueType):
    raise newException(KeyError, "Only available for " & $queueType & "queues")

proc `maxInMemoryLength=`*(args: FieldTable, length: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, QORUM_QUEUE)
  result["x-max-in-memory-length"] = length.asField

proc `maxInMemoryBytes=`*(args: FieldTable, bytes: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, QORUM_QUEUE)
  result["x-max-in-memory-bytes"] = bytes.asField

proc `deliveryLimit=`*(args: FieldTable, limit: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, QORUM_QUEUE)
  result["x-delivery-limit"] = limit.asField

proc `maxAge=`*(args: FieldTable, interval: TimeInterval): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  if interval.years > 0:
    result["x-max-age"] = ($(interval.years)&"Y").asField
  elif interval.months > 0:
    result["x-max-age"] = ($(interval.months)&"M").asField
  elif interval.days > 0:
    result["x-max-age"] = ($(interval.days)&"D").asField
  elif interval.hours > 0:
    result["x-max-age"] = ($(interval.hours)&"h").asField
  elif interval.minutes > 0:
    result["x-max-age"] = ($(interval.minutes)&"m").asField
  elif interval.seconds > 0:
    result["x-max-age"] = ($(interval.seconds)&"s").asField
  else:
    raise newException(ValueError, "Only Y, M, D, h, m, and s are acceptable")

proc `streamMaxSegmentSizeBytes=`*(args: FieldTable, bytes: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  result["x-stream-max-segment-size-bytes"] = bytes.asField

proc `initialClusterSize=`*(args: FieldTable, size: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  result["x-initial-cluster-size"] = size.asField

const xStreamOffset = "x-stream-offset"

proc `streamOffset=`*(args: FieldTable, size: int): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  result[xStreamOffset] = size.asField

proc `streamOffset=`*(args: FieldTable, ts: Time): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  result[xStreamOffset] = ts.asField

proc `streamOffset=`*(args: FieldTable, startFrom: StreamFromKind): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  result[xStreamOffset] = ($startFrom).asField

proc `streamOffset=`*(args: FieldTable, interval: TimeInterval): FieldTable {.discardable.} =
  checkArgsNil(args)
  checkQueueType(args, STREAM_QUEUE)
  if interval.years > 0:
    result[xStreamOffset] = ($(interval.years)&"Y").asField
  elif interval.months > 0:
    result[xStreamOffset] = ($(interval.months)&"M").asField
  elif interval.days > 0:
    result[xStreamOffset] = ($(interval.days)&"D").asField
  elif interval.hours > 0:
    result[xStreamOffset] = ($(interval.hours)&"h").asField
  elif interval.minutes > 0:
    result[xStreamOffset] = ($(interval.minutes)&"m").asField
  elif interval.seconds > 0:
    result[xStreamOffset] = ($(interval.seconds)&"s").asField
  else:
    raise newException(ValueError, "Only Y, M, D, h, m, and s are acceptable")

proc deliveryCount*(args: FieldTable): uint = 
  let res = args.getOrDefault("x-delivery-count", nil)
  if res.isNil:
    result = 0
  else:
    result = res.uIntVal
