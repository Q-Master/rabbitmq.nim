import std/[asyncdispatch, times, tables, random, net]
import pkg/networkutils/buffered_socket
import internal/[address, frame, field, exceptions, spec, auth]
import internal/methods/all

export address

const
  DEFAULT_POOLSIZE = 5
  DEFAULT_NET_TIMEOUT = 500

type
  RabbitMQ* = ref RabbitMQObj
  RabbitMQObj* = object
    pool: seq[RabbitMQConn]
    current: int
    hosts: RMQAddresses
    closing: bool

  RabbitMQConn = ref RabbitMQConnObj
  RabbitMQConnObj = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket
    serverProps: FieldTable
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16
    channels: Table[uint16, Channel]
    timeout: int
  
  Channel* = ref ChannelObj
  ChannelObj* = object of RootObj
    connection: RabbitMQConn
    channelId*: uint16
    opened: bool
    expectedMethods: seq[uint32]
    resultFuture: Future[AMQPMethod]
    busy: Future[void]

proc acquire(pool: RabbitMQ): Future[RabbitMQConn] {.async.}
proc release(conn: RabbitMQConn)
proc `=destroy`(rabbit: var RabbitMQConnObj)
proc newRabbitMQConn(timeout: int): RabbitMQConn
proc connect(conn: RabbitMQConn, adr: RMQAddress) {.async.}
proc disconnect(rabbit: RabbitMQConn, fromClose: bool = false) {.async.}
proc closeConnection(conn: RabbitMQConn) {.async.}
proc closeOkConnection(conn: RabbitMQConn) {.async.}
proc nextChannel(conn: RabbitMQConn): uint16
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}
proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.}
proc waitMethods(rmq: RabbitMQConn, methods: sink seq[uint32]): Future[AMQPMethod] {.async.}
proc syncRPC0(conn: RabbitMQConn, meth: AMQPMethod, expectedMethods: sink seq[uint32]): Future[AMQPMethod] {.async.}
proc onClose(ch: Channel) {.async.}

proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE): RabbitMQ =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.closing = false
  result.hosts = addresses
  randomize()

proc newRabbitMQ*(address: RMQAddress, poolsize: int = DEFAULT_POOLSIZE): RabbitMQ =
  let addrs = RMQAddresses()
  addrs.addresses.add(address)
  addrs.current = 0
  addrs.order = RMQ_CONNECTION_DIRECT
  result = newRabbitMQ(addrs, poolsize)

proc connect*(pool: RabbitMQ) {.async.} =
  for i in 0 ..< pool.pool.len:
    template conn: untyped = pool.pool[i]
    if conn.isNil:
      conn = newRabbitMQConn(DEFAULT_NET_TIMEOUT)
      let adr = pool.hosts.next()
      await conn.connect(adr)

proc close*(pool: RabbitMQ) {.async.}=
  pool.closing = true
  var closed: bool = false
  while not closed:
    closed = true
    for i in 0 ..< pool.pool.len:
      template s: untyped = pool.pool[i]
      if not s.isNil:
        await s.disconnect()
    await sleepAsync(200)

# ---  Channel

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

proc newChannel*(connection: RabbitMQConn, channelId: uint16): Channel =
  result.new
  result.connection = connection
  result.channelId = channelId
  result.opened = false
  result.expectedMethods = @[]
  result.resultFuture = nil
  result.busy = nil

proc rpc*(ch: Channel, meth: AMQPMethod, expectedMethods: sink seq[uint32]): Future[AMQPMethod] =
  let wasOpened = ch.opened
  var retFuture = newFuture[AMQPMethod]("Resulting future")
  if ch.opened:
    var sendFuture = newFuture[void]("Send data")
    sendFuture.callback =
      proc() {.gcsafe.} =
        discard ch.connection.sendMethod(meth, ch.channelId)

    proc busyWaiter(fut: Future[bool] = nil) {.gcsafe.} =
      var success: bool = true
      if not fut.isNil:
        if fut.failed:
          retFuture.fail(fut.readError())
        else:
          #TODO check if too long waiting
          success = fut.read
      if not ch.opened and wasOpened:
        raise newAMQPException(AMQPChannelClosed, "Channel closed unexpectedly", 200)
      if (ch.busy.isNil or ch.busy.finished) and success:
        ch.busy = newFuture[void]("Channel busy future")
        ch.resultFuture = retFuture
        ch.expectedMethods = expectedMethods
        sendFuture.complete()
      else:
        retFuture.withTimeout(ch.connection.timeout).callback = busyWaiter
      busyWaiter()
  else:
    var sendFuture = ch.connection.sendMethod(meth, ch.channelId)
    var receiveFuture = ch.connection.waitMethods(expectedMethods)
    sendFuture.callback =
      proc() =
        if sendFuture.failed:
          retFuture.fail(sendFuture.readError())
    receiveFuture.callback =
      proc() =
        if receiveFuture.failed:
          retFuture.fail(receiveFuture.readError())
        else:
          retFuture.complete(receiveFuture.read())
  return retFuture

proc openChannel*(pool: RabbitMQ): Future[Channel] {.async.} =
  let conn = await pool.acquire()
  let chId = conn.nextChannel()
  let chan = newChannel(conn, chId)
  conn.channels[chId] = chan
  try:
    let res = await chan.rpc(newChannelOpenMethod(""), @[CHANNEL_OPEN_OK_METHOD_ID])
    #chan.stopper = chan.reader()
    echo res.kind
  except:
    discard
  finally:
    conn.release()

proc close*(channel: Channel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.} =
  if channel.connection.channels.hasKey(channel.channelId):
    let res {.used.} = await channel.rpc(newChannelCloseMethod(ord(kind).uint16, $kind, 0, 0,), @[CHANNEL_CLOSE_OK_METHOD_ID])
    channel.connection.channels.del(channel.channelId)
    channel.opened = false
    channel.busy = nil
    channel.resultFuture = nil
  else:
    raise newException(AMQPChannelError, "Channel is already freed")

proc flow*(channel: Channel, active: bool) {.async.} =
  let res {.used.} = await channel.rpc(newChannelFlowMethod(active), @[CHANNEL_FLOW_OK_METHOD_ID])

# -- pvt RMQ

proc acquire(pool: RabbitMQ): Future[RabbitMQConn] {.async.} =
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
    let diff = (getTime() - stime).inMilliseconds()
    if diff > DEFAULT_NET_TIMEOUT:
      raise newException(RMQConnectionFailed, "Failed to acquire connection")
    if pool.current == 0:
      await sleepAsync(100)

proc release(conn: RabbitMQConn) =
  conn.inuse = false

proc `=destroy`(rabbit: var RabbitMQConnObj) =
  if rabbit.connected:
    let r = RabbitMQConn.new
    r[] = rabbit
    waitFor(r.disconnect())

proc newRabbitMQConn(timeout: int): RabbitMQConn =
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

proc connect(conn: RabbitMQConn, adr: RMQAddress) {.async.} =
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

proc disconnect(rabbit: RabbitMQConn, fromClose: bool = false) {.async.} =
  if rabbit.connected:
    rabbit.connected = false
    if rabbit.authenticated:
      for k,v in rabbit.channels:
        if k != 0:
          await v.onClose()
      if fromClose:
        await rabbit.closeOkConnection()
      else:
        await rabbit.closeConnection()
    result = rabbit.sock.close()
  rabbit.inuse = false
  rabbit.authenticated = false

const
  AMQP_REPLY_CODE = 200

proc closeConnection(conn: RabbitMQConn) {.async.} =
  let closeOk {.used.} = await conn.syncRPC0(newConnectionCloseMethod(AMQP_REPLY_CODE, $AMQP_REPLY_CODE, 0, 0), @[CONNECTION_CLOSE_OK_METHOD_ID])

proc closeOkConnection(conn: RabbitMQConn) {.async.} =
  await conn.sendMethod(newConnectionCloseOkMethod(), 0)

proc nextChannel(conn: RabbitMQConn): uint16 =
  let limit = (if conn.channelMax > 0: conn.channelMax else: AMQP_CHANNELS_MAX).int
  if conn.channels.len >= limit:
    raise newAMQPException(AMQPChannelsExhausted, "Channels are out", limit)
  for num in 1.uint16..(conn.channels.len+1).uint16:
    if conn.channels.hasKey(num):
      continue
    else:
      return num
  let num = (conn.channels.len+1).uint16
  return num

proc readFrame(rmq: RabbitMQConn): Future[Frame] {.async.} =
  result = await rmq.sock.decodeFrame()
  if result.frameType == ftMethod:
    if result.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
      raiseException(result.meth.connObj.replyCode)
    if result.meth.methodId == CHANNEL_CLOSE_METHOD_ID:
      raiseException(result.meth.channelObj.replyCode)

proc waitMethods(rmq: RabbitMQConn, methods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  let frame = await rmq.readFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId notin methods:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc waitMethod(rmq: RabbitMQConn, meth: uint32): Future[AMQPMethod] =
  result = rmq.waitMethods(@[meth])

proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0) {.async.} =
  let frame = newMethodFrame(channel, meth)
  await conn.sock.encodeFrame(frame)

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
  let openOkOrClose = await conn.syncRPC0(newConnectionOpenMethod(info.vhost, "", true), @[CONNECTION_OPEN_OK_METHOD_ID, CONNECTION_CLOSE_METHOD_ID])
  if openOkOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")
  conn.authenticated = true

proc syncRPC0(conn: RabbitMQConn, meth: AMQPMethod, expectedMethods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  await conn.sendMethod(meth, 0)
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

# -- pvt Channel

proc onClose(ch: Channel) {.async.} =
  #TODO need to cancel all the consumers and stop them
  discard