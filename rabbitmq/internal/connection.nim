import std/[asyncdispatch, times, tables, random, sets]
import pkg/networkutils/buffered_socket
import ./util/lists
import ./methods/all
import ./frame
import ./address
import ./exceptions
import ./spec
import ./field
import ./auth

type
  RabbitMQ* = ref RabbitMQObj
  RabbitMQObj* = object of RootObj
    pool: seq[RabbitMQConn]
    current: int
    timeout: int
    hosts: RMQAddresses
    closing: bool

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
    frames: SinglyLinkedList[Frame]
    openedChannels: set[uint16]

const
  DEFAULT_POOLSIZE = 5
  AMQP_REPLY_CODE = 200

proc newRabbitMQConn(): RabbitMQConn
proc disconnect(rabbit: RabbitMQConn): Future[void]
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}
proc sendMethod*(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.}
proc enqueueFrame*(conn: RabbitMQConn, f: Frame)
proc requeueFrame*(conn: RabbitMQConn, f: Frame)

proc connect*(pool: RabbitMQ, callback: proc (conn: RabbitMQConn): Future[void] {.closure, gcsafe.} = nil) {.async.} =
  for i in 0 ..< pool.pool.len:
    template conn: untyped = pool.pool[i]
    if conn.isNil:
      conn = newRabbitMQConn()
    if not conn.connected:
      let adr = pool.hosts.next()
      let connFut = conn.sock.connect(adr.host, adr.port)
      var success = false
      if pool.timeout > 0:
        success = await connFut.withTimeout(pool.timeout)
      else:
        await connFut
        success = true
      if success:
        await needLogin(conn, adr)
        conn.connected = true
        if not callback.isNil:
          asyncCheck callback(conn)
      else:
        conn.connected = false
        raise newException(RMQConnectionFailed, "Timeout connecting to RabbitMQ")

proc acquire*(pool: RabbitMQ): Future[RabbitMQConn] {.async.} =
  ## Retrieves next non-in-use async socket for request
  if pool.closing:
    raise newException(RMQConnectionClosed, "Connection is safely closing now")
  let stime = getTime()
  while true:
    template s: untyped = pool.pool[pool.current]
    if s.connected:
      if not s.inuse:
        s.inuse = true
        return s
    pool.current.inc()
    pool.current = pool.current.mod(pool.pool.len)
    if pool.timeout > 0:
      let diff = (getTime() - stime).inMilliseconds()
      if diff > pool.timeout:
        raise newException(RMQConnectionFailed, "Failed to acquire connection")
    if pool.current == 0:
      await sleepAsync(100)

proc release*(rabbit: RabbitMQConn) =
  rabbit.inuse = false

proc acquireChannel*(rabbit: RabbitMQConn, chId: uint16 = 0): uint16 = 
  result = chId
  if chId == 0:
    if rabbit.openedChannels.len >= rabbit.channelMax.int - 1:
      var res: int
      for i in rabbit.openedChannels:
        res = i.int
        break
      raise newAMQPException(AMQPChannelsExhausted, "Channels are out", res)
    while true:
      result = rand(rabbit.openedChannels.len).uint16
      if result notin rabbit.openedChannels:
        break
  else:
    if result in rabbit.openedChannels:
      raise newException(AMQPChannelInUse, "Channed already in use")
  rabbit.openedChannels.incl(result)

proc releaseChannel*(rabbit: RabbitMQConn, chId: uint16) =
  if chId notin rabbit.openedChannels:
    raise newException(AMQPChannelError, "Channel already released")
  rabbit.openedChannels.excl(chId)

proc channelExists*(rabbit: RabbitMQConn, chId: uint16): bool =
  result = chId in rabbit.openedChannels

proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): RabbitMQ =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.timeout = timeout
  result.closing = false
  result.hosts = addresses
  randomize()

proc newRabbitMQ*(address: RMQAddress, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): RabbitMQ =
  let addrs = RMQAddresses()
  addrs.addresses.add(address)
  addrs.current = 0
  addrs.order = RMQ_CONNECTION_DIRECT
  result = newRabbitMQ(addrs, poolsize, timeout)

proc close*(pool: RabbitMQ) {.async.}=
  pool.closing = true
  var closed: bool = false
  while not closed:
    closed = true
    for i in 0 ..< pool.pool.len:
      template s: untyped = pool.pool[i]
      if not s.isNil:
        if s.inuse:
          closed = false
        else:
          await s.disconnect()
    await sleepAsync(200)

template withRabbit*(t: RabbitMQ, x: untyped) =
  var rabbit {.inject.} = await t.acquire()
  x
  rabbit.release()

proc waitMethods*(rmq: RabbitMQConn, methods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  let frame = await rmq.sock.decodeFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId notin methods:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc waitMethod*(rmq: RabbitMQConn, meth: uint32): Future[AMQPMethod] =
  result = rmq.waitMethods(@[meth])

proc waitOrGetFrame*(conn: RabbitMQConn): Future[Frame] {.async.} =
  if conn.frames.empty():
    result = await conn.sock.decodeFrame()
  else:
    result = conn.frames.popFront().value

proc waitOrGetFrameOnChannel*(conn: RabbitMQConn, channel: uint16): Future[Frame] {.async.} =
  if conn.frames.empty():
    while true:
      result = await conn.sock.decodeFrame()
      if channel != result.channelNum:
        conn.enqueueFrame(result)
      else:
        break
  else:
    for node in conn.frames.nodes:
      if channel == node.value.channelNum:
        result = node.value
        conn.frames.reset(node)
        break

proc simpleRPC*(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16, expectedMethods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  await conn.sendMethod(meth, channel)
  var frame: Frame 
  while true:
    frame = await conn.sock.decodeFrame()
    if not (
      frame.frameType == ftMethod and
      (
        (
          frame.channelNum == channel and
          (frame.meth.methodId in expectedMethods or frame.meth.methodId == CHANNEL_CLOSE_METHOD_ID)
        ) or
        (frame.channelNum == 0 and frame.meth.methodId == CONNECTION_CLOSE_METHOD_ID)
      )
    ):
      conn.enqueueFrame(frame)
    else:
      break
  if frame.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
    raiseException(frame.meth.connObj.replyCode)
  if frame.meth.methodId == CHANNEL_CLOSE_METHOD_ID:
    raiseException(frame.meth.channelObj.replyCode)
  result = frame.meth

proc closeConnection(conn: RabbitMQConn): Future[void] {.async.} =
  let closeOk {.used.} = await conn.simpleRPC(newConnectionCloseMethod(AMQP_REPLY_CODE, $AMQP_REPLY_CODE, 0, 0), 0, @[CONNECTION_CLOSE_OK_METHOD_ID])

proc disconnect(rabbit: RabbitMQConn): Future[void] {.async.} =
  if rabbit.connected:
    if rabbit.authenticated:
      await rabbit.closeConnection()
    result = rabbit.sock.close()
    #TODO Add calling all waiting callbacks
  rabbit.connected = false
  rabbit.inuse = false
  rabbit.authenticated = false

proc enqueueFrame*(conn: RabbitMQConn, f: Frame) =
  conn.frames.add(f)

proc requeueFrame*(conn: RabbitMQConn, f: Frame) =
  conn.frames.prepend(f)

proc sendMethod*(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.} =
  let frame = newMethodFrame(channel, meth)
  await conn.sock.encodeFrame(frame)

proc `=destroy`(rabbit: var RabbitMQConnObj) =
  if rabbit.connected:
    let r = RabbitMQConn.new
    r[] = rabbit
    waitFor(r.disconnect())

proc newRabbitMQConn(): RabbitMQConn =
  result.new()
  result.inuse = false
  result.connected = false
  result.authenticated = false
  result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, outBufSize = AMQP_FRAME_MAX)
  #result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX)
  result.serverProps = nil
  result.channelMax = 0
  result.frameMax = AMQP_FRAME_MAX
  result.heartbeat = 0
  result.frames = initSinglyLinkedList[Frame]()

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
  let openOkOrClose = await conn.simpleRPC(newConnectionOpenMethod(info.vhost, "", true), 0, @[CONNECTION_OPEN_OK_METHOD_ID, CONNECTION_CLOSE_METHOD_ID])
  if openOkOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")
  conn.authenticated = true
  #TODO Start consumer here, set callback on close.


#[

static inline int amqp_heartbeat_send(amqp_connection_state_t state) {
  return state->heartbeat;
}

static inline int amqp_heartbeat_recv(amqp_connection_state_t state) {
  return 2 * state->heartbeat;
}

]#
