#[
Class Grammar:
    exchange  = C:DECLARE  S:DECLARE-OK
              / C:DELETE   S:DELETE-OK
              / C:BIND     S:BIND-OK
              / C:UNBIND   S:UNBIND-OK
]#

import std/[asyncdispatch]
import ./internal/methods/all
import ./internal/[exceptions, field]
import ./connection

export AMQPExchangeType

type
  Exchange* = ref ExchangeObj
  ExchangeObj* = object of RootObj
    id: string
    channel: Channel

proc newExchange*(id: string, channel: Channel): Exchange =
  result.new
  result.id = id
  result.channel = channel

proc exchangeDeclare*(
    channel: Channel, exchange: string, exchangeType: AMQPExchangeType,
    passive = false, durable = false, autoDelete = false, internal = false, noWait = false,
    args: FieldTable = nil
  ): Future[Exchange] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  let res {.used.} = await channel.rpc(
    newExchangeDeclareMethod(
      exchange, exchangeType, passive, durable, autoDelete, internal, noWait, args
    ), 
    @[AMQP_EXCHANGE_DECLARE_OK_METHOD],
    noWait = noWait
  )
  result = newExchange(exchange, channel)

proc delete*(exchange: Exchange, ifUnused = false, noWait = false) {.async.} =
  let res {.used.} = await exchange.channel.rpc(
    newExchangeDeleteMethod(
      exchange.id, ifUnused, noWait
    ), 
    @[AMQP_EXCHANGE_DELETE_OK_METHOD],
    noWait = noWait
  )

proc exchangeBind*(src: Exchange, destination: string, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  try:
    let res {.used.} = await src.channel.rpc(
      newExchangeBindMethod(
        destination, src.id, routingKey, noWait, args
      ), 
      @[AMQP_EXCHANGE_BIND_OK_METHOD],
      noWait = noWait
    )
    result = true
  except AMQPNotFound:
    result = false

proc exchangeBind*(src, destination : Exchange, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] =
  result = src.exchangeBind(destination.id, routingKey, noWait, args)

proc unbind*(src: Exchange, destination: string, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] {.async.} =
  var args = args
  if args.isNil:
    args = newFieldTable()
  try:
    let res {.used.} = await src.channel.rpc(
      newExchangeUnbindMethod(
        destination, src.id, routingKey, noWait, args
      ), 
      @[AMQP_EXCHANGE_UNBIND_OK_METHOD],
      noWait = noWait
    )
    result = true
  except AMQPNotFound:
    result = false

proc unbind*(src, destination : Exchange, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] =
  result = src.unbind(destination.id, routingKey, noWait, args)

proc id*(exchange: Exchange): string = exchange.id
proc channel*(exchange: Exchange): Channel = exchange.channel
