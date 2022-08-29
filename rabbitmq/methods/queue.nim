#[
Class Grammar:
    queue = C:DECLARE  S:DECLARE-OK
          / C:BIND     S:BIND-OK
          / C:UNBIND   S:UNBIND-OK
          / C:PURGE    S:PURGE-OK
          / C:DELETE   S:DELETE-OK
]#

import std/[asyncdispatch]
import ../internal/methods/all
import ../internal/[exceptions, field]
import ../connection
import ./exchange

type
  Queue* = ref QueueObj
  QueueObj* = object of RootObj
    queueId*: string
    channel: Channel
    exchange: Exchange

proc queueDeclare*(
  channel: Channel, queue: string,
  passive = false, durable = false, exclusive = false, autoDelete = false, noWait = false,
  args: FieldTable = nil
  ): Future[Queue] {.async.} =
  let res = await channel.rpc(
    newQueueDeclareMethod(
      queue, passive, durable, exclusive, autoDelete, noWait, args
    ), 
    @[QUEUE_DECLARE_OK_METHOD_ID]
  )
  result = Queue(queueId: res.queueObj.queue, channel: channel)

proc queueDelete*(queue: Queue, ifUnused = false, ifEmpty = false, noWait = false): Future[uint32] {.async.} =
  let res = await queue.channel.rpc(
    newQueueDeleteMethod(
      queue.queueId, ifUnused, ifEmpty, noWait
    ), 
    @[QUEUE_DELETE_OK_METHOD_ID]
  )
  result = res.queueObj.messageCount

proc queueBind*(queue: Queue, exchange: Exchange, routingKey: string, noWait = false, args: FieldTable = nil): Future[bool] {.async.} =
  try:
    let res {.used.} = await queue.channel.rpc(
      newQueueBindMethod(
        queue.queueId, exchange.exchangeId, routingKey, noWait, args
      ), 
      @[QUEUE_BIND_OK_METHOD_ID]
    )
    result = true
    queue.exchange = exchange
  except AMQPNotFound:
    result = false

proc queueUnbind*(queue: Queue, exchange: Exchange, routingKey: string, args: FieldTable = nil): Future[bool] {.async.} =
  try:
    let res {.used.} = await queue.channel.rpc(
      newQueueUnbindMethod(
        queue.queueId, exchange.exchangeId, routingKey, args
      ), 
      @[QUEUE_UNBIND_OK_METHOD_ID]
    )
    result = true
    queue.exchange = nil
  except AMQPNotFound:
    result = false

proc queuePurge*(queue: Queue, noWait = false): Future[uint32] {.async.} =
  let res = await queue.channel.rpc(
      newQueuePurgeMethod(
        queue.queueId, noWait
      ), 
      @[QUEUE_PURGE_OK_METHOD_ID]
    )
  result = res.queueObj.messageCount

proc exchange*(queue: Queue): Exchange = queue.exchange
proc channel*(queue: Queue): Channel = queue.channel