import asyncdispatch, asyncnet
import lists
import tables
import strutils
import locks
import net
import times
import options
import ./auth
import ./async_socket_adapters
import ./url
import ./channel
import ./data
import ./methods
import ./spec
import ./frame
import ./streams
import ./exceptions

type
  ConnectionState = enum
    csNone
    csProtocol
    csTune
    csStart
    csOpen
    csClosing
    csClosed
  ConnectionInfo* = ref ConnectionInfoObj
  ConnectionInfoObj = object
    host*: string
    port*: Port
    login*: string
    password*: string
    vhost*: string
    hbMonitoringEnabled*: bool
    hbMonitoringTimeout*: uint
    connectionName*: string
    sslCtx: SslContext
  AsyncConnection* = ref AsyncConnectionObj
  AsyncConnectionObj = object
    sock: AsyncSocket
    inUse: bool
    started: bool
    state: ConnectionState
    queue: SinglyLinkedList[string]
    channels: Table[int, Channel]
    serverProps: TableRef[string, DataTable]
    serverCaps: TableRef[string, DataTable]
    tune: ConnectionTuneOk
    lastChannelLock: Lock
    lastChannel {.guard: lastChannelLock.} : int
    hearbeatMonitoring: bool
    heartbeatTimeout: uint16
    heartbeatLastReceived: Time
    auth: AuthMechanism
    connected: AsyncEvent
    connectionName: string
  ConnectionPool* = ref ConnectionPoolObj
  ConnectionPoolObj* = object
    connectionInfo: ConnectionInfo
    pool: seq[AsyncConnection]
    current: int

proc newAsyncConnection(): AsyncConnection =
  result.new()
  result.sock = newAsyncSocket()
  result.inUse = false
  result.started = false
  result.state = csNone
  result.queue = initSinglyLinkedList[string]()
  result.serverProps = nil
  result.serverCaps = nil
  result.tune = nil
  result.lastChannelLock.initLock()
  withLock(result.lastChannelLock):
    result.lastChannel = 1
  result.hearbeatMonitoring = true
  result.heartbeatTimeout = 0
  result.heartbeatLastReceived = fromUnix(0)
  result.auth = AUTH_NOT_SET
  result.connected = newAsyncEvent()
  result.connectionName = ""

proc createSSLContext(caFile: string, caPath: string, key: string, cert: string, verify: bool): SslContext
proc newConnectionInfo(
  host: string = "localhost", port: Port = Port(DEFAULT_PORTS["amqp"]),
  login: string = "guest", password: string = "guest",
  vhost: string = "/",
  hbEnabled: bool = true, hbTimeout: uint = 0,
  cName = "",
  sslCtx: SslContext = nil
  ): ConnectionInfo
proc newConnectionInfoWithURL(url: string): ConnectionInfo
proc connect(cInfo: ConnectionInfo): Future[AsyncConnection] {.async.}
proc receiveFrame(conn: AsyncConnection): Future[Method] {.async.}
proc rpc(conn: AsyncConnection, request: Method, waitResponce: bool = true): Future[Method] {.async.}

proc newConnectionPool*(host: string, port: Port, maxConnections = 5): ConnectionPool =
  result.new()
  result.pool.setLen(maxConnections)
  result.current = -1
  for i in 0..<maxConnections:
    result.pool[i] = newAsyncConnection()

proc acquire*(p: ConnectionPool): Future[AsyncConnection] {.async.} =
  while true:
    for i in 0..<p.pool.len:
      if p.pool[i].isNil():
        p.pool[i] = await connect(p.connectionInfo)
      if not p.pool[i].inUse and p.pool[i].state == csOpen:
        p.pool[i].inUse = true
        return p.pool[i]
    await sleepAsync(333)

#proc connect*(p: ConnectionPool) {.async.} =
#  

proc connect*(
  host: string = "localhost", port: Port = Port(5672), 
  login: string = "guest", password: string = "guest",
  virtualHost: string = "/",
  sslCtx: SslContext = nil,
  timeout: Option[float] = none(float),
  clientProps: TableRef[string, string] = nil,
  connectionName: string = "",
  hbMonitoringEnabled = true, hbTimeout = 0.uint,
  ): Future[AsyncConnection] {.async.} =
  let cInfo = newConnectionInfo(host, port, login, password, virtualHost, hbMonitoringEnabled, hbTimeout, connectionName, sslCtx)
  result = await connect(cInfo)

proc connect*(url: string): Future[AsyncConnection] {.async.} =
  let cInfo = newConnectionInfoWithURL(url)
  result = await connect(cInfo)

proc createSSLContext(caFile: string, caPath: string, key: string, cert: string, verify: bool): SslContext =
  result = newContext(
      verifyMode = (if verify: CVerifyPeer else: CVerifyNone),
      certFile = cert, keyFile = key,
      caDir = caPath, caFile = caFile
    )

proc newConnectionInfo(
  host: string = "localhost", port: Port = Port(DEFAULT_PORTS["amqp"]),
  login: string = "guest", password: string = "guest",
  vhost: string = "/",
  hbEnabled: bool = true, hbTimeout: uint = 0,
  cName = "",
  sslCtx: SslContext = nil
  ): ConnectionInfo =
  result.new
  result.host = host
  result.port = port
  result.login = login
  result.password = password
  result.vhost = vhost
  result.hbMonitoringEnabled = hbEnabled
  result.hbMonitoringTimeout = hbTimeout
  result.connectionName = cName
  result.sslCtx = sslCtx

proc newConnectionInfoWithURL(url: string): ConnectionInfo =
  var rmqUri = parseUri(url)
  let port = (if rmqUri.isAbsolute() and rmqUri.port.len == 0: Port(DEFAULT_PORTS[rmqUri.scheme]) else: Port(rmqUri.port.parseInt))
  var vhost = "/"
  if rmqUri.path.len > 0 and rmqUri.path != "/":
    vhost = rmqUri.path
    vhost.removePrefix('/')
  let args = rmqUri.query.decodeQuery()
  var sslCtx: SslContext = nil
  if rmqUri.scheme == "amqps":
    sslCtx = createSSLContext(
      args.getOrDefault("cafile", ""),
      args.getOrDefault("capath", ""),
      args.getOrDefault("keyfile", ""),
      args.getOrDefault("certfile", ""),
      (args.getOrDefault("no_verify_ssl", "0") == "0")
    )
  result = newConnectionInfo(
    rmqUri.hostname, port,
    rmqUri.username, rmqUri.password,
    vhost,
    (args.getOrDefault("heartbeat_monitoring", "1") == "1"), args.getOrDefault("heartbeat", "0").parseInt().uint,
    args.getOrDefault("name", ""),
    sslCtx
  )

#[
        # noinspection PyAsyncCall
        self._reader_task = self.create_task(self.__reader())

        # noinspection PyAsyncCall
        heartbeat_task = self.create_task(self.__heartbeat_task())
        heartbeat_task.add_done_callback(self._on_heartbeat_done)
        self.loop.call_soon(self.connected.set)

        return True
]#
proc connect(cInfo: ConnectionInfo): Future[AsyncConnection] {.async.} =
  result = newAsyncConnection()
  if not cInfo.sslCtx.isNil():
    cInfo.sslCtx.wrapSocket(result.sock)
  await result.sock.connect(cInfo.host, cInfo.port)
  let pHeader = newProtocolHeader()
  var outStream = newOutputStream()
  pHeader.encodeFrame(outStream)
  let res: ConnectionStart = cast[ConnectionStart](await result.receiveFrame())
  result.heartbeatLastReceived = getTime()
  result.auth = getAuthMechanism(res.mechanisms)
  result.serverProps = res.properties
  let connectionTune: ConnectionTune = cast[ConnectionTune](await result.rpc(
    newConnectionStartOk(
      clientProps = newClientProps(cInfo.connectionName), 
      mechanisms=getAuthMechanismName(result.auth), 
      response=encodeAuth(result.auth, cInfo.login, cInfo.password)
    )
  ))
  if result.heartbeatTimeout > 0:
    connectionTune.heartbeat = result.heartbeatTimeout
  discard waitFor result.rpc(
    newConnectionTuneOk(
      connectionTune.channelMax, 
      connectionTune.frameMax, 
      connectionTune.heartbeat
    ),
    false
  )
  discard waitFor result.rpc(
    newConnectionOpen(virtualHost=cInfo.vhost)
  )

proc receiveFrame(conn: AsyncConnection): Future[Method] {.async.} =
  var frameHeader = await conn.sock.recv(1)
  if frameHeader[0] == '\x00':
    raise newException(AMQPFrameError, await conn.sock.recv(4096))
  frameHeader.add(await conn.sock.recv(6))
  let startFrame = frameHeader.startsWith("AMQP")
  if not conn.started and startFrame:
    raise newException(AMQPSyntaxError, "Already started")
  else:
    conn.started = true
  if not startFrame:
    let headerStream = newInputStream(frameHeader)
    let(_, _, fSize) = getFrameHeaderInfo(headerStream)
    frameHeader.add(await conn.sock.recv(fSize.int+1))
    headerStream.close()
  let frameStream = newInputStream(frameHeader)
  let frame: Frame = decodeFrame(frameStream, startFrame)
  frameStream.close()
  case frame.frameType
  of ftMethod:
    result = frame.meth
  else:
    # TODO Fix for other frames incl. dataframes
    discard

proc rpc(conn: AsyncConnection, request: Method, waitResponce: bool = true): Future[Method] {.async.} =
  let stream = newOutputStream()
  let frame: Frame = newMethod(0, request)
  frame.encodeFrame(stream)
  asyncCheck conn.sock.send(stream)
  if not waitResponce:
    return nil
  let result = await conn.receiveFrame()
  if result.syncronous and not result.index in request.validResponses:
    raise newException(AMQPInternalError, "Wrong reply")
  elif result.name == "Connection.Close":
    await conn.close()
    checkCloseReason(cast[ConnectionClose](result))

