import std/[asyncdispatch, tables, random, net]
import pkg/networkutils/buffered_socket
import ./internal/[address, frame, field, exceptions, spec, auth]
import ./internal/methods/all
import ./[message, consumertag]


export address


const
  DEFAULT_POOLSIZE = 5
  DEFAULT_NET_TIMEOUT = 500


type
  RabbitMQ* = ref object
    pool: seq[RabbitMQConn]
    waiters: seq[Future[RabbitMQConn]]
    hosts: RMQAddresses
    closing: bool

  RabbitMQConn = ref RabbitMQConnObj
  RabbitMQConnObj = object
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket
    serverProps: FieldTable
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16
    channels: Table[uint16, AMQPChannel]
    timeout: int
    consumer: Future[void]
    heartbeater: Future[void]
    retFuture: Future[AMQPMethod]
    blocked: bool
  
  AMQPChannel* = ref object
    connection: RabbitMQConn
    channelId*: uint16
    opened: bool
    closed: bool
    active: bool
    expectedMethods: seq[AMQPMethods]
    resultFuture: FutureVar[Payload]
    dataFrame: Payload
    consumers: Table[string, ConsumerTag]
    onReturn: proc(msg: Message): Future[void]
    flowWaiters: seq[Future[void]]
  
  Payload* = ref object
    meth*: AMQPMethod
    msg*: Message


proc acquire(pool: RabbitMQ): Future[RabbitMQConn] {.async.}
proc release(pool: RabbitMQ, conn: RabbitMQConn)
proc newRabbitMQConn(timeout: int): RabbitMQConn
proc connect(conn: RabbitMQConn, adr: RMQAddress) {.async.}
proc disconnect(conn: RabbitMQConn) {.async.}
proc onBlocked(conn: RabbitMQConn, blocked: bool)
proc closeConnection(conn: RabbitMQConn) {.async.}
proc closeOkConnection(conn: RabbitMQConn) {.async.}
proc nextChannel(conn: RabbitMQConn): uint16
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}
proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0, payload: Message = nil) {.async, gcsafe.}
proc waitMethods(conn: RabbitMQConn, expectedMethods: seq[AMQPMethods]): Future[AMQPMethod] {.async.}
proc syncRPC0(conn: RabbitMQConn, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods]): Future[AMQPMethod] {.async.}
proc consume(conn: RabbitMQConn) {.async.}
proc sendHeartbeats(conn: RabbitMQConn) {.async.}
proc onFrame(ch: AMQPChannel, frame: Frame) {.async.}
proc onDeliverReturn(ch: AMQPChannel) {.async.}
proc onFlow(ch: AMQPChannel, active: bool) {.async.}
proc onServerClose(ch: AMQPChannel, code: uint16, text: string) {.async.}
proc processFlowWaiters(channel: AMQPChannel)
proc cancelFlowWaiters(channel: AMQPChannel, exc: ref CatchableError = nil)
proc closeGracefully(channel: AMQPChannel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.}
proc abortChannel(ch: AMQPChannel, exc: ref CatchableError)


proc openChannel*(pool: RabbitMQ): Future[AMQPChannel] {.async.}


proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE): RabbitMQ =
  result.new
  result.pool.setLenUninit(poolsize)
  result.waiters = @[]
  result.closing = false
  result.hosts = addresses
  randomize()


proc newRabbitMQ*(address: RMQAddress, poolsize: int = DEFAULT_POOLSIZE): RabbitMQ =
  let addrs = RMQAddresses()
  addrs.addresses.add(address)
  addrs.current = 0
  addrs.order = RMQ_CONNECTION_DIRECT
  result = newRabbitMQ(addrs, poolsize)


proc connect*(pool: RabbitMQ): Future[AMQPChannel] {.async.} =
  for i in 0 ..< pool.pool.len:
    template conn: untyped = pool.pool[i]
    if conn.isNil:
      conn = newRabbitMQConn(DEFAULT_NET_TIMEOUT)
      let adr = pool.hosts.next()
      await conn.connect(adr)
  result = await pool.openChannel()


proc close*(pool: RabbitMQ) {.async.} =
  pool.closing = true
  var closed: bool = false
  while not closed:
    closed = true
    for i in 0 ..< pool.pool.len:
      template s: untyped = pool.pool[i]
      if not s.isNil:
        await s.disconnect()
    await sleepAsync(200)


# --- Payload


proc newPayload(meth: AMQPMethod, msg: Message): Payload =
  result = Payload(meth: meth, msg: msg)


# ---  AMQPChannel


proc newChannel*(connection: RabbitMQConn, channelId: uint16): AMQPChannel =
  result = AMQPChannel(
    connection: connection,
    channelId: channelId,
    opened: false,
    active: true,
    expectedMethods: @[],
    resultFuture: newFutureVar[Payload]("Resulting future"),
    dataFrame: nil,
    flowWaiters: @[]
  )
  result.resultFuture.complete() # needed to RPC to work at first time.


proc rpc*(ch: AMQPChannel, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods], payload: Message = nil, noWait: bool = false): Future[Payload] =
  if ch.closed:
    result = newFuture[Payload]("rpc")
    result.fail(newAMQPException(AMQPChannelError, "AMQPChannel is closed", 504))
  else:
    let wasOpened = ch.opened
    let retFuture = Future[Payload](ch.resultFuture)
    var sendFuture = newFuture[void]("Send data")
    var busyCounter: int = 0
    proc busyWaiter(fut: Future[bool] = nil) {.gcsafe.} =
      var success: bool = true
      if not fut.isNil:
        if fut.failed:
          if not retFuture.finished:
            retFuture.fail(fut.readError())
          return
        else:
          success = fut.read
          if not success and busyCounter > 1:
            if not retFuture.finished:
              retFuture.fail(newException(RMQConnectionFailed, "Failed to send data to RabbitMQ. Queue stuck."))
            return
      if not ch.opened and wasOpened:
        if not retFuture.finished:
          retFuture.fail(newAMQPException(AMQPChannelError, "AMQPChannel closed unexpectedly", 504))
        return
      if ch.resultFuture.finished:
        ch.resultFuture.clean()
        ch.expectedMethods = expectedMethods
        sendFuture = ch.connection.sendMethod(meth, ch.channelId, payload)
        sendFuture.callback =
          proc() =
            if sendFuture.failed:
              if not retFuture.finished:
                retFuture.fail(sendFuture.readError())
            elif noWait or expectedMethods.len == 0:
              ch.expectedMethods = @[]
              if not ch.resultFuture.finished:
                ch.resultFuture.complete(nil)
      else:
        var timeoutFuture = retFuture.withTimeout(ch.connection.timeout)
        timeoutFuture.callback = busyWaiter
        busyCounter.inc()
    busyWaiter()
    return retFuture


proc openChannel*(pool: RabbitMQ): Future[AMQPChannel] {.async.} =
  let conn = await pool.acquire()
  let chId = conn.nextChannel()
  result = conn.newChannel(chId)
  conn.channels[chId] = result
  try:
    let res {.used.} = await result.rpc(newChannelOpenMethod(""), @[AMQP_CHANNEL_OPEN_OK_METHOD])
    result.opened = true
    result.active = true
  except:
    conn.channels.del(chId)
    raise
  finally:
    pool.release(conn)


proc close*(channel: AMQPChannel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.} =
  if channel.connection.channels.hasKey(channel.channelId):
    await channel.closeGracefully(kind)
    channel.connection.channels.del(channel.channelId)


proc flow*(channel: AMQPChannel, active: bool) {.async.} =
  let res = await channel.rpc(newChannelFlowMethod(active), @[AMQP_CHANNEL_FLOW_OK_METHOD])
  channel.active = res.meth.channelObj.active
  if channel.active:
    channel.processFlowWaiters()


proc addOnFlow*(channel: AMQPChannel, fut: Future[void]) =
  channel.flowWaiters.add(fut)


proc active*(channel: AMQPChannel): bool = channel.active


proc blocked*(channel: AMQPChannel): bool = channel.connection.blocked


proc addConsumer*(ch: AMQPChannel, consumer: ConsumerTag) =
  ch.consumers[consumer.id] = consumer


proc removeConsumer*(ch: AMQPChannel, consumer: ConsumerTag) =
  ch.consumers.del(consumer.id)


proc `onReturn=`*(ch: AMQPChannel, cb: proc(msg: Message): Future[void]) =
  ch.onReturn = cb


# -- pvt RMQ


proc acquire(pool: RabbitMQ): Future[RabbitMQConn] {.async.} =
  ## Retrieves next non-in-use async socket for request
  if pool.closing:
    raise newException(RMQConnectionClosed, "Connection is safely closing now")
  for conn in pool.pool:
    if conn.connected and not conn.inuse:
      conn.inuse = true
      return conn

  let fut = newFuture[RabbitMQConn]("acquire")
  fut.addCallback do (f: Future[RabbitMQConn]):
    let idx = pool.waiters.find(f)
    if idx >= 0:
      pool.waiters.delete(idx)
  pool.waiters.add(fut)
  let isCompleted = await fut.withTimeout(DEFAULT_NET_TIMEOUT)
  if isCompleted:
    result = await fut
  else:
    let exc = newException(RMQConnectionFailed, "Timeout acquiring connection from pool")
    fut.fail(exc)
    raise exc


proc release(pool: RabbitMQ, conn: RabbitMQConn) =
  conn.inuse = false

  if pool.closing or not conn.connected:
    return

  while pool.waiters.len > 0:
    let waiter = pool.waiters[0]
    pool.waiters.delete(0)

    if not waiter.finished:
      conn.inuse = true
      waiter.complete(conn) # Мгновенно будим задачу!
      break


proc newRabbitMQConn(timeout: int): RabbitMQConn =
  result.new()
  result.inuse = false
  result.connected = false
  result.authenticated = false
  result.timeout = timeout
  result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, outBufSize = AMQP_FRAME_MAX, timeout = timeout)
  result.serverProps = nil
  result.channelMax = 0
  result.frameMax = AMQP_FRAME_MAX
  result.heartbeat = 0
  result.blocked = false


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
      try:
        await needLogin(conn, adr)
      except AMQPError:
        try:
          await conn.closeOkConnection()
        except CatchableError:
          discard
        try:
          await conn.sock.close()
        except CatchableError:
          discard
        raise
      conn.connected = true
      conn.consumer = conn.consume
      asyncCheck conn.consumer
      if conn.heartbeat > 0:
        conn.heartbeater = conn.sendHeartbeats()
        asyncCheck conn.heartbeater
    else:
      conn.connected = false
      raise newException(RMQConnectionFailed, "Timeout connecting to RabbitMQ")


proc disconnect(conn: RabbitMQConn) {.async.} =
  if conn.connected and conn.authenticated:
    let exc = newException(RMQConnectionClosed, "Connection is closing")
    for ch in conn.channels.values:
      ch.abortChannel(exc)
    conn.channels.clear()
    try:
      discard await conn.closeConnection().withTimeout(2000)
    except CatchableError:
      discard
  conn.connected = false
  if conn.consumer != nil and not conn.consumer.finished:
    try:
      await conn.consumer
    except CatchableError:
      discard
  conn.consumer = nil
  conn.heartbeater = nil
  try:
    await conn.sock.close()
  except CatchableError:
    discard
  conn.inuse = false
  conn.authenticated = false


const AMQP_REPLY_CODE = 200


proc closeConnection(conn: RabbitMQConn) {.async.} =
  let closeOk {.used.} = await conn.syncRPC0(newConnectionCloseMethod(AMQP_REPLY_CODE, $AMQP_REPLY_CODE, 0, 0), @[AMQP_CONNECTION_CLOSE_OK_METHOD])


proc closeOkConnection(conn: RabbitMQConn) {.async.} =
  await conn.sendMethod(newConnectionCloseOkMethod(), 0)


proc onServerClose(conn: RabbitMQConn, code: uint16, text: string) {.async.} =
  conn.connected = false
  let exc = newAMQPExceptionForCode(code, text)
  for ch in conn.channels.values:
    ch.abortChannel(exc)
  conn.channels.clear()
  if not conn.retFuture.isNil and not conn.retFuture.finished:
    conn.retFuture.fail(exc)
  try:
    await conn.closeOkConnection()
  except CatchableError:
    discard
  try:
    await conn.sock.close()
  except CatchableError:
    discard
  conn.authenticated = false
  conn.inuse = false


proc nextChannel(conn: RabbitMQConn): uint16 =
  let limit = (if conn.channelMax > 0: conn.channelMax else: AMQP_CHANNELS_MAX).int
  if conn.channels.len >= limit:
    raise newException(RMQChannelsExhausted, "Channels are out of limit: " & $limit)
  for num in 1.uint16..(conn.channels.len+1).uint16:
    if conn.channels.hasKey(num):
      continue
    else:
      return num
  let num = (conn.channels.len+1).uint16
  return num


proc readFrame(conn: RabbitMQConn): Future[Frame] {.async.} =
  result = await conn.sock.decodeFrame()
  if result.frameType == ftMethod:
    if result.meth.methodId == CONNECTION_CLOSE_METHOD_ID:
      raiseException(result.meth.connObj.replyCode, result.meth.connObj.replyText)
    if result.meth.methodId == CHANNEL_CLOSE_METHOD_ID:
      raiseException(result.meth.channelObj.replyCode, result.meth.channelObj.replyText)


proc waitMethods(conn: RabbitMQConn, expectedMethods: seq[AMQPMethods]): Future[AMQPMethod] {.async.} =
  let frame = await conn.readFrame()
  if frame.frameType == ftMethod:
    if frame.meth.idToAMQPMethod() notin expectedMethods:
      raise newException(RMQUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")


proc waitMethod(conn: RabbitMQConn, expectedMethod: AMQPMethods): Future[AMQPMethod] =
  result = conn.waitMethods(@[expectedMethod])


proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0, payload: Message = nil) {.async.} =
  let frame = newMethodFrame(channel, meth)
  await conn.sock.encodeFrame(frame)
  if not payload.isNil:
    let frame = newHeaderFrame(channel, payload.data.len.uint64, payload.props)
    await conn.sock.encodeFrame(frame)
    let maxBody = conn.frameMax.int - (FRAME_HEADER_SIZE + 1)
    if maxBody <= 0:
      raise newException(AMQPFrameError, "Negotiated frame_max is too small: " & $conn.frameMax)
    var bLen = payload.data.len
    var start = 0
    while bLen > 0:
      let chunkSize = min(bLen, maxBody)
      let frame = newBodyFrame(channel, chunkSize)
      copyMem(frame.fragment[0].addr, payload.data[start].addr, chunkSize)
      await conn.sock.encodeFrame(frame)
      start.inc(chunkSize)
      bLen.dec(chunkSize)


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
  if clientHeatbeat == 0:
    clientHeatbeat = serverHeatbeat
  elif serverHeatbeat != 0 and serverHeatbeat < clientHeatbeat:
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
      if frame.meth.idToAMQPMethod() in expectedMethods:
        result = frame.meth
      else:  
        raise newException(RMQUnexpectedMethod, "Unexpected method: " & $frame.meth.methodId.int)
    else:
      raise newException(AMQPUnexpectedFrame, "Unexpected frame: " & $frame.frameType.int)
  
  proc both(): Future[AMQPMethod] {.async.} =
    await send()
    result = await recv()
  
  if conn.authenticated:
    let sfut = send()
    asyncCheck sfut
  else:
    var res = both()
    res.callback =
      proc() =
        if conn.retFuture.finished:
          return
        if res.failed:
          conn.retFuture.fail(res.readError())
        else:
          conn.retFuture.complete(res.read())
  return conn.retFuture


proc consume(conn: RabbitMQConn) {.async.} =
  while conn.connected:
    var frame: Frame
    try:
      frame = await conn.sock.decodeFrame()
    except TimeoutError:
      continue
    if frame.channelNum == 0:
      case frame.frameType
      of ftMethod:
        let methId = frame.meth.idToAMQPMethod
        if methId == AMQP_CONNECTION_CLOSE_METHOD:
          let m = frame.meth.connObj
          await conn.onServerClose(m.replyCode, m.replyText)
          break
        if methId == AMQP_CONNECTION_BLOCKED_METHOD or methId == AMQP_CONNECTION_UNBLOCKED_METHOD:
          conn.onBlocked(methId == AMQP_CONNECTION_BLOCKED_METHOD)
          continue
        if conn.retFuture.isNil or conn.retFuture.finished:
          raise newException(RMQUnexpectedMethod, "Unexpected method: " & $frame.meth.methodId.int)
        else:
          conn.retFuture.complete(frame.meth)
      of ftHeartBeat:
        #echo "HeartBeat"
        discard
      else:
        discard
    else:
      if conn.channels.hasKey(frame.channelNum):
        asyncCheck conn.channels[frame.channelNum].onFrame(frame)
      else:
        raise newException(RMQPNoSuchChannel, "AMQPChannel " & $frame.channelNum.int & " doesn't exist or is closing")


proc onBlocked(conn: RabbitMQConn, blocked: bool) =
  conn.blocked = blocked
  if not blocked:
    for ch in conn.channels.values:
      if ch.active:
        ch.processFlowWaiters()


proc sendHeartbeats(conn: RabbitMQConn) {.async.} =
  if conn.heartbeat == 0:
    return

  let hbInterval = conn.heartbeat.int * 1000

  while conn.connected and conn.authenticated:
    await sleepAsync(hbInterval)

    if conn.connected and conn.authenticated:
      try:
        let frame = newHeartBeatFrame()
        await conn.sock.encodeFrame(frame)
      except Exception:
        # Если сокет умер, прерываем цикл
        break


# -- pvt AMQPChannel


proc onFrame(ch: AMQPChannel, frame: Frame) {.async.} =
  template fut: untyped = Future[Payload](ch.resultFuture)
  #echo "Got frame: " & $frame.frameType
  case frame.frameType
  of ftMethod:
    let methEnum = frame.meth.idToAMQPMethod()
    #echo "Method: " & $methEnum
    if methEnum == AMQP_CHANNEL_CLOSE_METHOD:
      let m = frame.meth.channelObj
      await ch.onServerClose(m.replyCode, m.replyText)
      return
    if methEnum == AMQP_CHANNEL_FLOW_METHOD:
      await ch.onFlow(frame.meth.channelObj.active)
      return
    if methEnum in [AMQP_BASIC_DELIVER_METHOD, AMQP_BASIC_RETURN_METHOD]:
      ch.dataFrame = newPayload(frame.meth, newMessage())
      return
    if ch.resultFuture.finished:
      discard
    elif ch.expectedMethods.len == 0:
      ch.resultFuture.complete(nil)
    elif methEnum notin ch.expectedMethods:
      fut.fail(newException(RMQUnexpectedMethod, "Method " & $methEnum & " is unexpected"))
    else:
      if methEnum == AMQP_BASIC_GET_OK_METHOD:
        ch.resultFuture.mget() = newPayload(frame.meth, newMessage())
      else:
        ch.resultFuture.complete(newPayload(frame.meth, nil))
  of ftHeader:
    #echo "Header: " & $ch.resultFuture.finished & " " & $ch.dataFrame.isNil
    if ch.resultFuture.finished and ch.dataFrame.isNil:
      raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")
    elif ch.dataFrame.isNil:
      if ch.resultFuture.mget().msg.build(frame):
        ch.resultFuture.complete()
    else:
      if ch.dataFrame.msg.build(frame):
        await ch.onDeliverReturn()
        ch.dataFrame = nil
  of ftBody:
    #echo "Body: " & $ch.resultFuture.finished & " " & $ch.dataFrame.isNil
    if ch.resultFuture.finished and ch.dataFrame.isNil:
      if frame.fragment.len > 0:
        raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")
    elif ch.dataFrame.isNil:
      if ch.resultFuture.mget.msg.build(frame):
        ch.resultFuture.complete()
    else:
      if ch.dataFrame.msg.build(frame):
        await ch.onDeliverReturn()
        ch.dataFrame = nil
  else:
    fut.fail(newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected"))


proc abortChannel(ch: AMQPChannel, exc: ref CatchableError) =
  ch.opened = false
  ch.closed = true
  ch.active = false
  if not ch.resultFuture.finished:
    Future[Payload](ch.resultFuture).fail(exc)
  ch.cancelFlowWaiters(exc)
  ch.consumers.clear()
  ch.dataFrame = nil


proc onServerClose(ch: AMQPChannel, code: uint16, text: string) {.async.} =
  ch.abortChannel(newAMQPExceptionForCode(code, text))
  ch.connection.channels.del(ch.channelId)
  try:
    await ch.connection.sendMethod(newChannelCloseOkMethod(), ch.channelId)
  except CatchableError:
    discard


proc onDeliverReturn(ch: AMQPChannel) {.async.} =
  case ch.dataFrame.meth.idToAMQPMethod
  of AMQP_BASIC_DELIVER_METHOD:
    let ct = ch.dataFrame.meth.basicObj.consumerTag()
    let consumerTag = ch.consumers.getOrDefault(ct, nil)
    if consumerTag.isNil:
      raise newException(KeyError, "No such consumer with id " & ct)
    if consumerTag.onDeliver.isNil:
      return
    let basicMethod = ch.dataFrame.meth.basicObj
    asyncCheck consumerTag.call(basicMethod.deliveryTag, basicMethod.exchange, basicMethod.routingKey, ch.dataFrame.msg, basicMethod.redelivered)
  of AMQP_BASIC_RETURN_METHOD:
    if ch.onReturn.isNil:
      return
    asyncCheck ch.onReturn(ch.dataFrame.msg)
  else:
    discard
  ch.dataFrame = nil


proc onFlow(ch: AMQPChannel, active: bool) {.async.} =
  ch.active = active
  let res {.used.} = await ch.rpc(
    newChannelFlowOkMethod(active), @[]
  )
  if active:
    ch.processFlowWaiters()


proc processFlowWaiters(channel: AMQPChannel) =
  let flowWaitersCopy = channel.flowWaiters
  channel.flowWaiters = @[]
  for fut in flowWaitersCopy:
    fut.complete()


proc cancelFlowWaiters(channel: AMQPChannel, exc: ref CatchableError = nil) =
  let flowWaitersCopy = channel.flowWaiters
  channel.flowWaiters = @[]
  var e: ref CatchableError = exc
  if e.isNil:
    e = newAMQPException(AMQPChannelError, "AMQPChannel is closing", 504)
  for fut in flowWaitersCopy:
    if not fut.finished:
      fut.fail(e)


proc closeGracefully(channel: AMQPChannel, kind: AMQP_CODES = AMQP_SUCCESS) {.async.} =
  if channel.connection.channels.hasKey(channel.channelId):
    let res {.used.} = await channel.rpc(newChannelCloseMethod(ord(kind).uint16, $kind, 0, 0,), @[AMQP_CHANNEL_CLOSE_OK_METHOD])
    channel.opened = false
    channel.closed = true
    channel.cancelFlowWaiters()
