import std/[asyncdispatch, tables, net]
import pkg/networkutils/buffered_socket
import ./methods/all
import ./frame
import ./address
import ./exceptions
import ./spec
import ./field
import ./auth

type
  RabbitMQConn* = ref RabbitMQConnObj
  RabbitMQConnObj* = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket
    serverProps: FieldTable
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16
    channels: Table[uint16, proc(f:Frame): Future[void]]
    timeout: int
    stopper: Future[void]

const
  AMQP_REPLY_CODE = 200

proc newRabbitMQConn*(timeout: int): RabbitMQConn
proc sendMethod*(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.}
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}
proc readFrame(rmq: RabbitMQConn): Future[Frame] {.async.}
proc reader(rmq: RabbitMQConn) {.async.}
proc closeConnection(conn: RabbitMQConn) {.async.}
proc closeOkConnection(conn: RabbitMQConn) {.async.}

proc connect*(conn: RabbitMQConn, adr: RMQAddress) {.async.} =
  if not conn.connected:
    let connFut = conn.sock.connect(adr.host, adr.port)
    var success = false
    if conn.timeout > 0:
      success = await connFut.withTimeout(conn.timeout)
    else:
      await connFut
      success = true
    if success:
      await needLogin(conn, adr)
      conn.connected = true
    else:
      conn.connected = false
      raise newException(RMQConnectionFailed, "Timeout connecting to RabbitMQ")

proc disconnect*(rabbit: RabbitMQConn, fromClose: bool = false) {.async.} =
  if rabbit.connected:
    rabbit.connected = false
    if rabbit.authenticated:
      if fromClose:
        await rabbit.closeOkConnection()
      else:
        await rabbit.closeConnection()
    if rabbit.stopper != nil:
      await rabbit.stopper
      rabbit.stopper = nil
    result = rabbit.sock.close()
    for k,v in rabbit.channels:
      if k != 0:
        await v(newMethodFrame(k, newConnectionCloseMethod(200, "Closing")))
  rabbit.inuse = false
  rabbit.authenticated = false

proc channelExists*(rabbit: RabbitMQConn, chId: uint16): bool =
  result = rabbit.channels.hasKey(chId)

proc waitMethods*(rmq: RabbitMQConn, methods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  let frame = await rmq.readFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId notin methods:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc waitMethod*(rmq: RabbitMQConn, meth: uint32): Future[AMQPMethod] =
  result = rmq.waitMethods(@[meth])

proc simpleRPC*(conn: RabbitMQConn, channel: uint16, meth: AMQPMethod, expectedMethods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  await conn.sendMethod(meth, channel)
  let frame = await conn.readFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId in expectedMethods:
      result = frame.meth
    elif frame.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
      await conn.disconnect(fromClose=true)
    else:  
      raise newAMQPException(AMQPUnexpectedMethod, "Unexpected method", frame.meth.methodId.int)
  else:
    raise newAMQPException(AMQPUnexpectedFrame, "Unexpected frame", frame.frameType.int)

proc closeConnection(conn: RabbitMQConn) {.async.} =
  let closeOk {.used.} = await conn.simpleRPC(0, newConnectionCloseMethod(AMQP_REPLY_CODE, $AMQP_REPLY_CODE, 0, 0), @[CONNECTION_CLOSE_OK_METHOD_ID])

proc closeOkConnection(conn: RabbitMQConn) {.async.} =
  await conn.sendMethod(newConnectionCloseOkMethod(), 0)

proc sendMethod*(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.} =
  let frame = newMethodFrame(channel, meth)
  await conn.sock.encodeFrame(frame)

proc `=destroy`(rabbit: var RabbitMQConnObj) =
  if rabbit.connected:
    let r = RabbitMQConn.new
    r[] = rabbit
    waitFor(r.disconnect())

proc newRabbitMQConn*(timeout: int): RabbitMQConn =
  result.new()
  result.inuse = false
  result.connected = false
  result.authenticated = false
  result.timeout = timeout
  result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, outBufSize = AMQP_FRAME_MAX)
  #result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX)
  result.serverProps = nil
  result.channelMax = 0
  result.frameMax = AMQP_FRAME_MAX
  result.heartbeat = 0

proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.} =
  let header = newProtocolHeaderFrame(PROTOCOL_VERSION[0], PROTOCOL_VERSION[1], PROTOCOL_VERSION[2])
  await conn.sock.encodeFrame(header)
  let start = await conn.waitMethod(CONNECTION_START_METHOD_ID)
  if start.connObj.versionMajor != PROTOCOL_VERSION[0] or start.connObj.versionMinor != PROTOCOL_VERSION[1]:
    raise newException(AMQPIncompatibleProtocol, "Incompatible protocol version")
  conn.serverProps = start.connObj.serverProperties
  let authMethod = getCheckAuthSupported(start.connObj.mechanisms)
  let authBytes = authMethod.encodeAuth(user = info.login.username, password = info.login.password)
  let clientProps: FieldTable = asFieldTable({
    "platform": PLATFORM,
    "product": PRODUCT,
    "version": RMQVERSION,
    "copyright": AUTHOR,
    "information": INFORMATION,
    "capabilities": {
      "authentication_failure_close": true,
      "exchange_exchange_bindings": true,
      "basic.nack": true,
      "connection.blocked": false,
      "consumer_cancel_notify": true,
      "publisher_confirms": true
    }
  })
  await conn.sendMethod(newConnectionStartOkMethod(clientProps, $authMethod, authBytes))
  let tuneOrClose = await conn.waitMethods(@[CONNECTION_TUNE_METHOD_ID, CONNECTION_CLOSE_METHOD_ID])
  if tuneOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")
  
  template serverChannelMax: untyped = tuneOrClose.connObj.channelMax
  template serverFrameMax: untyped = tuneOrClose.connObj.frameMax
  template serverHeatbeat: untyped = tuneOrClose.connObj.heartbeat
  template clientChannelMax: untyped = conn.channelMax
  template clientFrameMax: untyped = conn.frameMax
  template clientHeatbeat: untyped = conn.heartbeat

  if serverChannelMax != 0 and (serverChannelMax < clientChannelMax or clientChannelMax == 0):
    clientChannelMax = serverChannelMax
  elif serverChannelMax == 0 and clientChannelMax == 0:
    clientChannelMax = uint16.high
  if serverFrameMax != 0 and serverFrameMax < clientFrameMax:
    clientFrameMax = serverFrameMax
  if serverHeatbeat != 0 and serverHeatbeat < clientHeatbeat:
    clientHeatbeat = serverHeatbeat
  await conn.sendMethod(newConnectionTuneOkMethod(clientChannelMax, clientFrameMax, clientHeatbeat))
  let openOkOrClose = await conn.simpleRPC(0, newConnectionOpenMethod(info.vhost, "", true), @[CONNECTION_OPEN_OK_METHOD_ID, CONNECTION_CLOSE_METHOD_ID])
  if openOkOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")
  conn.authenticated = true
  conn.stopper = reader(conn)

proc readFrame(rmq: RabbitMQConn): Future[Frame] {.async.} =
  result = await rmq.sock.decodeFrame()
  if result.frameType == ftMethod:
    if result.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
      raiseException(result.meth.connObj.replyCode)
    if result.meth.methodId == CHANNEL_CLOSE_METHOD_ID:
      raiseException(result.meth.channelObj.replyCode)

proc reader(rmq: RabbitMQConn) {.async.} =
  while rmq.connected:
    var frame: Frame
    try:
      frame = await rmq.sock.decodeFrame()
    except TimeoutError:
      continue
    if frame.channelNum == 0:
      case frame.frameType
      of ftMethod:
        asyncCheck rmq.channels[0](frame)
      of ftHeartBeat:
        echo "HeartBeat"
      else:
        discard
    else:
      if rmq.channels.hasKey(frame.channelNum):
        asyncCheck rmq.channels[frame.channelNum](frame)
      else:
        raise newAMQPException(AMQPNoSuchChannel, "Channel doesn't exist or is closing", frame.channelNum.int)


#[

static inline int amqp_heartbeat_send(amqp_connection_state_t state) {
  return state->heartbeat;
}

static inline int amqp_heartbeat_recv(amqp_connection_state_t state) {
  return 2 * state->heartbeat;
}

]#
