import std/[asyncdispatch, asyncnet, times, tables]
import pkg/networkutils/buffered_socket
import ./methods/all
import ./frame
import ./address
import ./exceptions
import ./spec
import ./field
import ./auth

type
  RabbitMQ = ref RabbitMQObj
  RabbitMQObj = object of RootObj
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
    info: RMQAddress
    serverProps: TableRef[string, Field]

const
  RABBITMQ_MAX_BUF: int = 1024*131
  DEFAULT_POOLSIZE = 5

proc newRabbitMQConn(): RabbitMQConn
proc disconnect[T: RabbitMQConn | RabbitMQConnObj](rabbit: var T): Future[void]
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}

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
        conn.info = adr
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

proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): RabbitMQ =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.timeout = timeout
  result.closing = false
  result.hosts = addresses

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
    await sleepAsync(1)

template withRabbit*(t: RabbitMQ, x: untyped) =
  var rabbit {.inject.} = await t.acquire()
  x
  rabbit.release()

proc waitMethod*(rmq: RabbitMQConn, meth: uint32): Future[AMQPMethod] {.async.} =
  let frame = await rmq.sock.decodeFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId != meth:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc disconnect[T: RabbitMQConn | RabbitMQConnObj](rabbit: var T): Future[void] =
  if rabbit.connected:
    result = rabbit.sock.close()
    #TODO Add calling all waiting callbacks
  rabbit.connected = false
  rabbit.inuse = false

proc `=destroy`(rabbit: var RabbitMQConnObj) =
  if rabbit.connected:
    waitFor(rabbit.disconnect())

proc newRabbitMQConn(): RabbitMQConn =
  result.new()
  result.inuse = false
  result.connected = false
  result.sock = newAsyncBufferedSocket(inBufSize = RABBITMQ_MAX_BUF, outBufSize = RABBITMQ_MAX_BUF)

proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.} =
  let header = newProtocolHeaderFrame(PROTOCOL_VERSION[0], PROTOCOL_VERSION[1], PROTOCOL_VERSION[2])
  await conn.sock.encodeFrame(header)
  let start = await conn.waitMethod(CONNECTION_START_METHOD_ID)
  if start.connObj.versionMajor != PROTOCOL_VERSION[0] or start.connObj.versionMinor != PROTOCOL_VERSION[1]:
    raise newException(AMQPIncompatibleProtocol, "Incompatible protocol version")
  conn.serverProps = start.connObj.serverProperties
  let authMethod = getCheckAuthSupported(start.connObj.mechanisms)
  let authBytes = authMethod.encodeAuth(user = info.login.username, password = info.login.password)
  let clientProps: FieldTable = {
    "platform": PLATFORM.asField,
    "product": PRODUCT.asField,
    "version": RMQVERSION.asField,
    "copyright": AUTHOR.asField,
    "information": INFORMATION.asField,
    "capabilities": {
      "authentication_failure_close": true.asField,
      "exchange_exchange_bindings": true.asField,
      "basic.nack": true.asField,
      "connection.blocked": false.asField,
      "consumer_cancel_notify": true.asField,
      "publisher_confirms": true.asField
    }.newTable.asField
  }.newTable
  let meth = newMethod(CONNECTION_START_OK_METHOD_ID)
  meth.connObj = newConnectionStartOk(clientProps, $authMethod, authBytes)
  await conn.sock.encodeMethod(meth)
