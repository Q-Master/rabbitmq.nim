import std/[asyncdispatch, times, tables, random, net]
import pkg/networkutils/buffered_socket
import ./internal/[address, frame, field, exceptions, spec, auth, properties]
import ./internal/methods/all
import ./message

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
    consumer: Future[void]
    retFuture: Future[AMQPMethod]
  
  Channel* = ref ChannelObj
  ChannelObj* = object of RootObj
    connection: RabbitMQConn
    channelId*: uint16
    opened: bool
    expectedMethods: seq[AMQPMethods]
    resultFuture: Future[AMQPMethod]

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
proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0, payload: Message = nil) {.async, gcsafe.}
proc waitMethods(rmq: RabbitMQConn, expectedMethods: sink seq[AMQPMethods]): Future[AMQPMethod] {.async.}
proc syncRPC0(conn: RabbitMQConn, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods]): Future[AMQPMethod] {.async.}
proc consume(rmq: RabbitMQConn) {.async.}
proc onClose(ch: Channel) {.async.}
proc onFrame(ch: Channel, frame: Frame) {.async.}

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

proc rpc*(ch: Channel, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods], payload: Message = nil, noWait: bool = false): Future[AMQPMethod] =
  let wasOpened = ch.opened
  var retFuture = newFuture[AMQPMethod]("Resulting future")
  var sendFuture = newFuture[void]("Send data")
  proc busyWaiter(fut: Future[bool] = nil) {.gcsafe.} =
    var success: bool = true
    if not fut.isNil:
      if fut.failed:
        retFuture.fail(fut.readError())
      else:
        #TODO check if too long waiting
        success = fut.read
    if not ch.opened and wasOpened:
      retFuture.fail(newAMQPException(AMQPChannelClosed, "Channel closed unexpectedly", 200))
    if (ch.resultFuture.isNil or ch.resultFuture.finished) and success:
      ch.resultFuture = retFuture
      ch.expectedMethods = expectedMethods
      sendFuture = ch.connection.sendMethod(meth, ch.channelId, payload)
      sendFuture.callback=
        proc() =
          if sendFuture.failed:
            retFuture.fail(sendFuture.readError())
          elif noWait or expectedMethods.len == 0:
            ch.expectedMethods = @[]
            retFuture.complete(nil)
    else:
      var timeoutFuture = retFuture.withTimeout(ch.connection.timeout)
      timeoutFuture.callback = busyWaiter
  busyWaiter()
  return retFuture

proc openChannel*(pool: RabbitMQ): Future[Channel] {.async.} =
  let conn = await pool.acquire()
  let chId = conn.nextChannel()
  result = conn.newChannel(chId)
  conn.channels[chId] = result
  try:
    let res {.used.} = await result.rpc(newChannelOpenMethod(""), @[AMQP_CHANNEL_OPEN_OK_METHOD])
    result.opened = true
  except:
    conn.channels.del(chId)
    raise
  finally:
    conn.release()

proc close*(channel: Channel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.} =
  if channel.connection.channels.hasKey(channel.channelId):
    let res {.used.} = await channel.rpc(newChannelCloseMethod(ord(kind).uint16, $kind, 0, 0,), @[AMQP_CHANNEL_CLOSE_OK_METHOD])
    channel.connection.channels.del(channel.channelId)
    channel.opened = false
  else:
    raise newException(AMQPChannelError, "Channel is already freed")

proc flow*(channel: Channel, active: bool) {.async.} =
  let res {.used.} = await channel.rpc(newChannelFlowMethod(active), @[AMQP_CHANNEL_FLOW_OK_METHOD])

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
  result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, outBufSize = AMQP_FRAME_MAX, timeout = timeout)
  #result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, timeout = timeout)
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
      conn.consumer = conn.consume
      asyncCheck conn.consumer
    else:
      conn.connected = false
      raise newException(RMQConnectionFailed, "Timeout connecting to RabbitMQ")

proc disconnect(rabbit: RabbitMQConn, fromClose: bool = false) {.async.} =
  if rabbit.connected:
    if rabbit.authenticated:
      for k,v in rabbit.channels:
        if k != 0:
          await v.onClose()
      if fromClose:
        await rabbit.closeOkConnection()
      else:
        await rabbit.closeConnection()
      rabbit.connected = false
      if rabbit.consumer != nil and not rabbit.consumer.finished:
        await rabbit.consumer
      rabbit.consumer = nil
    result = rabbit.sock.close()
  rabbit.inuse = false
  rabbit.authenticated = false

const
  AMQP_REPLY_CODE = 200

proc closeConnection(conn: RabbitMQConn) {.async.} =
  let closeOk {.used.} = await conn.syncRPC0(newConnectionCloseMethod(AMQP_REPLY_CODE, $AMQP_REPLY_CODE, 0, 0), @[AMQP_CONNECTION_CLOSE_OK_METHOD])

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

proc waitMethods(rmq: RabbitMQConn, expectedMethods: sink seq[AMQPMethods]): Future[AMQPMethod] {.async.} =
  let frame = await rmq.readFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId.idToAMQPMethod notin expectedMethods:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc waitMethod(rmq: RabbitMQConn, expectedMethod: AMQPMethods): Future[AMQPMethod] =
  result = rmq.waitMethods(@[expectedMethod])

proc sendHeader(conn: RabbitMQConn, channel: uint16 = 0, bodySize: uint64, props: Properties) {.async.} =
  let frame = newHeaderFrame(channel, bodySize, props)
  await conn.sock.encodeFrame(frame)

proc sendBody(conn: RabbitMQConn, channel: uint16 = 0, data: sink openArray[byte]) {.async.} =
  var bLen = data.len
  var start = 0
  while bLen > 0:
    let chunkSize = if bLen > conn.frameMax.int: conn.frameMax.int else: bLen
    let body = newStringOfCap(chunkSize)
    copyMem(body[0].unsafeAddr, data[start].addr, chunkSize)
    let frame = newBodyFrame(channel, body)
    await conn.sock.encodeFrame(frame)
    start.inc(chunkSize)
    bLen.dec(chunkSize)

proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0, payload: Message = nil) {.async.} =
  let frame = newMethodFrame(channel, meth)
  await conn.sock.encodeFrame(frame)
  if not payload.isNil:
    await conn.sendHeader(channel, payload.data.len.uint64, payload.props)
    await conn.sendBody(channel, payload.data)

proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.} =
  let header = newProtocolHeaderFrame(PROTOCOL_VERSION[0], PROTOCOL_VERSION[1], PROTOCOL_VERSION[2])
  await conn.sock.encodeFrame(header)
  let start = await conn.waitMethod(AMQP_CONNECTION_START_METHOD)
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
  let tuneOrClose = await conn.waitMethods(@[AMQP_CONNECTION_TUNE_METHOD, AMQP_CONNECTION_CLOSE_METHOD])
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
  let openOkOrClose = await conn.syncRPC0(newConnectionOpenMethod(info.vhost, "", true), @[AMQP_CONNECTION_OPEN_OK_METHOD, AMQP_CONNECTION_CLOSE_METHOD])
  if openOkOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")
  conn.authenticated = true
  
proc syncRPC0(conn: RabbitMQConn, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods]): Future[AMQPMethod] =
  conn.retFuture = newFuture[AMQPMethod]("syncRPC0")
  
  proc send() {.async.} =
    await conn.sendMethod(meth, 0)
  
  proc recv(): Future[AMQPMethod] {.async.} =
    let frame = await conn.readFrame()
    if frame.frameType == ftMethod:
      if frame.meth.methodId.idToAMQPMethod in expectedMethods:
        result = frame.meth
      elif frame.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
        await conn.disconnect(fromClose=true)
      else:  
        raise newAMQPException(AMQPUnexpectedMethod, "Unexpected method", frame.meth.methodId.int)
    else:
      raise newAMQPException(AMQPUnexpectedFrame, "Unexpected frame", frame.frameType.int)
  
  proc both(): Future[AMQPMethod] {.async.} =
    await send()
    result = await recv()
  
  if conn.authenticated:
    let sfut {.used.} = send()
  else:
    var res = both()
    res.callback =
      proc() =
        if res.failed:
          conn.retFuture.fail(res.readError())
        else:
          conn.retFuture.complete(res.read())
  return conn.retFuture

proc consume(rmq: RabbitMQConn) {.async.} =
  while rmq.connected:
    var frame: Frame
    try:
      frame = await rmq.sock.decodeFrame()
    except TimeoutError:
      continue
    if frame.channelNum == 0:
      case frame.frameType
      of ftMethod:
        if frame.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
          echo "CLOSING: ", frame.meth.connObj.replyText
          await rmq.disconnect(fromClose=true)
        if rmq.retFuture.isNil or rmq.retFuture.finished:
          raise newAMQPException(AMQPUnexpectedMethod, "Unexpected method", frame.meth.methodId.int)
        else:
          rmq.retFuture.complete(frame.meth)
      of ftHeartBeat:
        echo "HeartBeat"
      else:
        discard
    else:
      if rmq.channels.hasKey(frame.channelNum):
        asyncCheck rmq.channels[frame.channelNum].onFrame(frame)
      else:
        raise newAMQPException(AMQPNoSuchChannel, "Channel doesn't exist or is closing", frame.channelNum.int)

# -- pvt Channel

proc onFrame(ch: Channel, frame: Frame) {.async.} =
  case frame.frameType
  of ftMethod:
    let methEnum = frame.meth.methodId.idToAMQPMethod
    if methEnum in [AMQP_BASIC_DELIVER_METHOD, AMQP_BASIC_RETURN_METHOD, AMQP_BASIC_ACK_METHOD, AMQP_BASIC_NACK_METHOD]:
      #TODO implement consuming
      discard
    if ch.resultFuture.isNil or ch.resultFuture.finished:
      discard
    elif ch.expectedMethods.len == 0:
      ch.resultFuture.complete(nil)
    elif methEnum notin ch.expectedMethods:
      ch.resultFuture.fail(newException(AMQPUnexpectedMethod, "Method " & $methEnum & " is unexpected"))
    else:
      ch.resultFuture.complete(frame.meth)
  of ftHeader:
    echo "HEADER"
    discard
  of ftBody:
    echo "BODY"
    discard
  else:
    ch.resultFuture.fail(newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected"))

proc onClose(ch: Channel) {.async.} =
  #TODO need to cancel all the consumers and stop them
  echo "Channel " & $ch.channelId & " closing"
  if not ch.resultFuture.isNil and not ch.resultFuture.finished:
    ch.resultFuture.fail(newAMQPException(AMQPInternalError, "Connection closing unexpectedly", 503))
  discard
