#[
Class Grammar:
    exchange  = C:DECLARE  S:DECLARE-OK
              / C:DELETE   S:DELETE-OK
              / C:BIND     S:BIND-OK
              / C:UNBIND   S:UNBIND-OK
]#

import std/[asyncdispatch]
import ../internal/methods/all
import ../internal/[exceptions, field]
import ../connection

type
  Exchange* = ref ExchangeObj
  ExchangeObj* = object of RootObj
    exchangeId*: string
    channel: Channel

proc newExchange*(exchangeId: string, channel: Channel): Exchange =
  result.new
  result.exchangeId = exchangeId
  result.channel = channel

proc exchangeDeclare*(
    channel: Channel, exchange: string, exchangeType: AMQPExchangeType,
    passive = false, durable = false, autoDelete = false, internal = false, noWait = false,
    args: FieldTable = nil
  ): Future[Exchange] {.async.} =
  let res {.used.} = await channel.rpc(
    newExchangeDeclareMethod(
      exchange, exchangeType, passive, durable, autoDelete, internal, noWait, args
    ), 
    @[EXCHANGE_DECLARE_OK_METHOD_ID]
  )
  result = newExchange(exchange, channel)

proc exchangeDelete*(exchange: Exchange, ifUnused = false, noWait = false) {.async.} =
  let res {.used.} = await exchange.channel.rpc(
    newExchangeDeleteMethod(
      exchange.exchangeId, ifUnused, noWait
    ), 
    @[EXCHANGE_DELETE_OK_METHOD_ID]
  )

proc exchangeBind*(src: Exchange, destination: string, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] {.async.} =
  try:
    let res {.used.} = await src.channel.rpc(
      newExchangeBindMethod(
        destination, src.exchangeId, routingKey, noWait, args
      ), 
      @[EXCHANGE_BIND_OK_METHOD_ID]
    )
    result = true
  except AMQPNotFound:
    result = false

proc exchangeBind*(src, destination : Exchange, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] =
  result = src.exchangeBind(destination.exchangeId, routingKey, noWait, args)

proc exchangeUnbind*(src: Exchange, destination: string, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] {.async.} =
  try:
    let res {.used.} = await src.channel.rpc(
      newExchangeUnbindMethod(
        destination, src.exchangeId, routingKey, noWait, args
      ), 
      @[EXCHANGE_UNBIND_OK_METHOD_ID]
    )
    result = true
  except AMQPNotFound:
    result = false

proc exchangeUnbind*(src, destination : Exchange, routingKey: string, noWait=false, args: FieldTable = nil): Future[bool] =
  result = src.exchangeUnbind(destination.exchangeId, routingKey, noWait, args)
