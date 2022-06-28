#[
Class Grammar:
    channel       = open-channel *use-channel close-channel
    open-channel  = C:OPEN S:OPEN-OK
    use-channel   = C:FLOW S:FLOW-OK
                  / S:FLOW C:FLOW-OK
                  / functional-class
    close-channel = C:CLOSE S:CLOSE-OK
                  / S:CLOSE C:CLOSE-OK
]#

import std/[asyncdispatch]
import ../internal/methods/all
import ../internal/[connection, exceptions, spec]

type 
  Channel* = ref ChannelObj
  ChannelObj* = object of RootObj
    connection: RabbitMQConn
    channelId*: uint16
    opened: bool

proc channelRPC*(channel: Channel, meth: AMQPMethod, expectedMethods: sink seq[uint32]): Future[AMQPMethod] =
  if channel.opened:
    result = channel.connection.simpleRPC(meth, channel.channelId, expectedMethods)
  else:
    raise newException(RMQChannelClosed, "Channel is closed")

proc openChannel*(conn: RabbitMQConn, channel: uint16 = 0): Future[Channel] {.async.} =
  var chn: uint16 = channel
  try:
    chn = conn.acquireChannel(channel)
    try:
      let res = await conn.simpleRPC(newChannelOpenMethod(""), chn, @[CHANNEL_OPEN_OK_METHOD_ID])
      echo res.kind
    except:
      releaseChannel(conn, chn)
      raise
    result = Channel(connection: conn, channelId: chn, opened: true)
  except AMQPChannelsExhausted as e:
    result = Channel(connection: conn, channelId: e.code.uint16)

proc closeChannel*(channel: Channel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.} =
  if channel.connection.channelExists(channel.channelId):
    let res = await channel.connection.simpleRPC(
      newChannelCloseMethod(ord(kind).uint16, $kind, 0, 0,), channel.channelId, @[CHANNEL_CLOSE_OK_METHOD_ID]
    )
    echo res.kind
    channel.connection.releaseChannel(channel.channelId)
    channel.opened = false
  raise newException(AMQPChannelError, "Channel is already freed")

proc flow*(channel: Channel, active: bool) {.async.} =
  let res {.used.} = await channel.channelRPC(newChannelFlowMethod(active), @[CHANNEL_FLOW_OK_METHOD_ID])
