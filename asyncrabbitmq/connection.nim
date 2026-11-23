import std/[asyncdispatch, tables, random, net, deques]
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
    rpcTimeout: int
    writeTail: Future[void]
  
  AMQPChannel* = ref object
    connection: RabbitMQConn
    channelId*: uint16
    opened: bool
    closed: bool
    active: bool
    dataFrame: Payload
    consumers: Table[string, ConsumerTag]
    onReturn: proc(msg: Message): Future[void]
    flowWaiters: seq[Future[void]]
    closing: bool
    rpcQueue: Deque[PendingRPC]
    confirms: bool
    publishSeq: uint64
    pendingConfirms: OrderedTable[uint64, Future[void]]
    confirmWaiters: seq[Future[void]]
    confirmTimeout: int

  Payload* = ref object
    meth*: AMQPMethod
    msg*: Message

  PendingRPC = ref object
    meth: AMQPMethod
    expected: seq[AMQPMethods]
    fut: Future[Payload]
    data: Message
    content: Payload


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
proc sendHead(ch: AMQPChannel) {.async.}
proc completeHead(ch: AMQPChannel, content: Payload)
proc failHead(ch: AMQPChannel, exc: ref CatchableError)
proc enqueue(ch: AMQPChannel, meth: AMQPMethod, expected: sink seq[AMQPMethods], data: Message): Future[Payload]
proc onConfirmAck(ch: AMQPChannel, tag: uint64, multiple: bool)
proc onConfirmNack(ch: AMQPChannel, tag: uint64, multiple: bool)
proc checkConfirmsDrained(ch: AMQPChannel)
proc confirmWatchdog(ch: AMQPChannel, tag: uint64, fut: Future[void]) {.async.}


proc openChannel*(pool: RabbitMQ): Future[AMQPChannel] {.async.}


proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE): RabbitMQ =
  result.new
  result.pool.setLen(poolsize)
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
    dataFrame: nil,
    flowWaiters: @[],
    rpcQueue: initDeque[PendingRPC](),
    pendingConfirms: initOrderedTable[uint64, Future[void]](),
    confirmWaiters: @[],
  )


proc rpc*(ch: AMQPChannel, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods], payload: Message = nil, noWait: bool = false): Future[Payload] =
  if ch.closed or ch.closing:
    result = newFuture[Payload]()
    result.fail(newAMQPException(AMQPChannelError, "AMQPChannel is closed", 504))
  else:
    result = ch.enqueue(meth, (if noWait: @[] else: expectedMethods), payload)


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
    channel.closing = true
    while channel.rpcQueue.len > 1:
      let p = channel.rpcQueue.popLast()
      if not p.fut.finished:
        p.fut.fail(newAMQPException(AMQPChannelError, "AMQPChannel is closing", 504))
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


proc confirms*(ch: AMQPChannel): bool = ch.confirms


proc pendingConfirms*(ch: AMQPChannel): int = ch.pendingConfirms.len


proc `confirmTimeout=`*(ch: AMQPChannel, ms: int) = ch.confirmTimeout = ms


proc addConfirmWaiter*(ch: AMQPChannel, fut: Future[void]) =
  ch.confirmWaiters.add(fut)


proc enableConfirms*(ch: AMQPChannel) =
  if ch.confirms and ch.pendingConfirms.len > 0:
    let exc = newException(RMQConnectionFailed, "Confirm mode re-selected")
    for fut in ch.pendingConfirms.values:
      if not fut.finished:
        fut.fail(exc)
    ch.pendingConfirms.clear()
    ch.checkConfirmsDrained()
  ch.confirms = true
  ch.publishSeq = 0


proc registerConfirm*(ch: AMQPChannel): Future[void] =
  if ch.confirms:
    ch.publishSeq.inc()
    result = newFuture[void]("publishConfirm")
    ch.pendingConfirms[ch.publishSeq] = result
    if ch.confirmTimeout > 0:
      asyncCheck ch.confirmWatchdog(ch.publishSeq, result)
  else:
    return nil


proc unregisterConfirm*(ch: AMQPChannel, fut: Future[void]) =
  if not fut.isNil:
    var toDel: uint64 = 0
    for tag, f in ch.pendingConfirms.pairs:
      if f == fut:
        toDel = tag
        break
    if toDel > 0:
      ch.pendingConfirms.del(toDel)
    if not fut.finished:
      fut.fail(newException(RMQConnectionFailed, "Publish was not sent"))
    ch.checkConfirmsDrained()


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

  if not pool.closing and conn.connected:
    while pool.waiters.len > 0:
      let waiter = pool.waiters[0]
      pool.waiters.delete(0)

      if not waiter.finished:
        conn.inuse = true
        waiter.complete(conn)
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
  result.rpcTimeout = DEFAULT_RPC_TIMEOUT


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
  result = (conn.channels.len+1).uint16
  for num in 1.uint16..(conn.channels.len+1).uint16:
    if conn.channels.hasKey(num):
      continue
    else:
      result = num
      break


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


template withWriteLock(conn: RabbitMQConn, body: untyped) =
  let prev = conn.writeTail
  let mine = newFuture[void]("writeLock")
  conn.writeTail = mine
  if not prev.isNil and not prev.finished:
    await prev
  try:
    body
  finally:
    mine.complete()


proc sendMethod(conn: RabbitMQConn, meth: AMQPMethod, channel: uint16 = 0, payload: Message = nil) {.async.} =
  withWriteLock(conn):
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
    except CatchableError as e:
      await conn.onServerClose(320, "Connection lost: " & e.msg)
      break
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
        if not conn.retFuture.isNil and not conn.retFuture.finished:
          conn.retFuture.complete(frame.meth)
      of ftHeartBeat:
        discard # TODO: heartbeat!
      else:
        discard
    else:
      let ch = conn.channels.getOrDefault(frame.channelNum, nil)
      if ch.isNil:
        discard
      else:
        try:
          await ch.onFrame(frame)
        except CatchableError as e:
          conn.channels.del(frame.channelNum)
          ch.abortChannel(e)


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
        break


# -- pvt AMQPChannel


proc onFrame(ch: AMQPChannel, frame: Frame) {.async.} =
  case frame.frameType
  of ftMethod:
    let methEnum = frame.meth.idToAMQPMethod()
    case methEnum
    of AMQP_CHANNEL_CLOSE_METHOD:
      let m = frame.meth.channelObj
      await ch.onServerClose(m.replyCode, m.replyText)
    of AMQP_CHANNEL_FLOW_METHOD:
      await ch.onFlow(frame.meth.channelObj.active)
    of AMQP_BASIC_DELIVER_METHOD, AMQP_BASIC_RETURN_METHOD:
      ch.dataFrame = newPayload(frame.meth, newMessage())
    of AMQP_BASIC_CANCEL_METHOD:
      let ct = frame.meth.basicObj.consumerTag
      if ch.consumers.hasKey(ct):
        ch.consumers.del(ct)
    of AMQP_BASIC_ACK_METHOD:
      ch.onConfirmAck(frame.meth.basicObj.deliveryTag, frame.meth.basicObj.multiple)
    of AMQP_BASIC_NACK_METHOD:
      ch.onConfirmNack(frame.meth.basicObj.deliveryTag, frame.meth.basicObj.multiple)
    else:
      if ch.rpcQueue.len == 0:
        discard
      else:
        let p = ch.rpcQueue[0]
        if methEnum notin p.expected:
          ch.failHead(newException(RMQUnexpectedMethod, "Method " & $methEnum & " is unexpected"))
        elif methEnum == AMQP_BASIC_GET_OK_METHOD:
          p.content = newPayload(frame.meth, newMessage())
        else:
          ch.completeHead(newPayload(frame.meth, nil))
  of ftHeader, ftBody:
    var target: Payload = nil
    if not ch.dataFrame.isNil:
      target = ch.dataFrame
    elif ch.rpcQueue.len > 0:
      target = ch.rpcQueue[0].content
    if target.isNil or target.msg.isNil:
      if frame.frameType == ftBody and frame.fragment.len == 0:
        discard
      else:
        ch.failHead(newException(AMQPUnexpectedFrame, "Content frame without active content"))
    else:
      if target.msg.build(frame):
        if not ch.dataFrame.isNil:
          await ch.onDeliverReturn()
          ch.dataFrame = nil
        else:
          ch.completeHead(target)
  else:
    ch.failHead(newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected"))


proc abortChannel(ch: AMQPChannel, exc: ref CatchableError) =
  ch.opened = false
  ch.closed = true
  ch.closing = true
  ch.active = false
  while ch.rpcQueue.len > 0:
    let p = ch.rpcQueue.popFirst()
    if not p.fut.finished:
      p.fut.fail(exc)
  for fut in ch.pendingConfirms.values:
    if not fut.finished:
      fut.fail(exc)
  ch.pendingConfirms.clear()
  for fut in ch.confirmWaiters:
    if not fut.finished:
      fut.fail(exc)
  ch.confirmWaiters = @[]
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

proc watchCallback(fut: Future[void], context: string) =
  fut.addCallback proc() =
    if fut.failed:
      stderr.writeLine("[asyncrabbitmq] " & context & " failed: " & fut.readError().msg)

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
    watchCallback(consumerTag.call(basicMethod.deliveryTag, basicMethod.exchange, basicMethod.routingKey, ch.dataFrame.msg, basicMethod.redelivered), "consumer " & ct)
  of AMQP_BASIC_RETURN_METHOD:
    if ch.onReturn.isNil:
      return
    watchCallback(ch.onReturn(ch.dataFrame.msg), "onReturn")
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
    let res {.used.} = await channel.enqueue(
      newChannelCloseMethod(ord(kind).uint16, $kind, 0, 0),
      @[AMQP_CHANNEL_CLOSE_OK_METHOD], nil)
    channel.opened = false
    channel.closed = true
    while channel.rpcQueue.len > 0:
      let p = channel.rpcQueue.popFirst()
      if not p.fut.finished:
        p.fut.fail(newAMQPException(AMQPChannelError, "AMQPChannel is closed", 504))
    channel.cancelFlowWaiters()


proc enqueue(ch: AMQPChannel, meth: AMQPMethod, expected: sink seq[AMQPMethods], data: Message): Future[Payload] =
  result = newFuture[Payload]("rpc")
  let p = PendingRPC(meth: meth, expected: expected, fut: result, data: data)
  ch.rpcQueue.addLast(p)
  if ch.rpcQueue.len == 1:
    asyncCheck ch.sendHead()


proc rpcWatchdog(ch: AMQPChannel, p: PendingRPC) {.async.} =
  let ok = await p.fut.withTimeout(ch.connection.rpcTimeout)
  if not ok and not p.fut.finished:
    let exc = newException(RMQConnectionFailed, "RPC timed out: " & $p.meth.idToAMQPMethod())
    ch.connection.channels.del(ch.channelId)
    ch.abortChannel(exc)


proc sendHead(ch: AMQPChannel) {.async.} =
  if ch.rpcQueue.len > 0 and not ch.closed:
    let p = ch.rpcQueue[0]
    if p.expected.len > 0:
      asyncCheck ch.rpcWatchdog(p)
    try:
      await ch.connection.sendMethod(p.meth, ch.channelId, p.data)
      if p.expected.len == 0:
        ch.completeHead(nil)
    except CatchableError as e:
      ch.failHead(e)


proc completeHead(ch: AMQPChannel, content: Payload) =
  if ch.rpcQueue.len > 0:
    let p = ch.rpcQueue.popFirst()
    if not p.fut.finished:
      p.fut.complete(content)
    if ch.rpcQueue.len > 0:
      asyncCheck ch.sendHead()


proc failHead(ch: AMQPChannel, exc: ref CatchableError) =
  if ch.rpcQueue.len > 0:
    let p = ch.rpcQueue.popFirst()
    if not p.fut.finished:
      p.fut.fail(exc)
    if ch.rpcQueue.len > 0:
      asyncCheck ch.sendHead()


proc checkConfirmsDrained(ch: AMQPChannel) =
  if ch.pendingConfirms.len == 0 and ch.confirmWaiters.len > 0:
    let waiters = ch.confirmWaiters
    ch.confirmWaiters = @[]
    for fut in waiters:
      if not fut.finished:
        fut.complete()


proc confirmWatchdog(ch: AMQPChannel, tag: uint64, fut: Future[void]) {.async.} =
  let ok = await fut.withTimeout(ch.confirmTimeout)
  if not ok and not fut.finished:
    ch.pendingConfirms.del(tag)
    fut.fail(newException(RMQConnectionFailed, "Publish confirm timed out, tag " & $tag))
    ch.checkConfirmsDrained()


proc settleConfirms(ch: AMQPChannel, tag: uint64, multiple: bool, exc: ref CatchableError) =
  if multiple:
    var settled: seq[uint64] = @[]
    for k, fut in ch.pendingConfirms.pairs:
      if k > tag:
        break
      if not fut.finished:
        if exc.isNil: fut.complete() else: fut.fail(exc)
      settled.add(k)
    for k in settled:
      ch.pendingConfirms.del(k)
  else:
    let fut = ch.pendingConfirms.getOrDefault(tag, nil)
    if not fut.isNil:
      if not fut.finished:
        if exc.isNil: fut.complete() else: fut.fail(exc)
      ch.pendingConfirms.del(tag)
  ch.checkConfirmsDrained()


proc onConfirmAck(ch: AMQPChannel, tag: uint64, multiple: bool) =
  if ch.confirms:
    ch.settleConfirms(tag, multiple, nil)


proc onConfirmNack(ch: AMQPChannel, tag: uint64, multiple: bool) =
  if ch.confirms:
    ch.settleConfirms(tag, multiple,
      newException(RMQPublishNacked, "Broker nacked publish, tag " & $tag))
