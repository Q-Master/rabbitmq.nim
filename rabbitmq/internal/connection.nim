import asyncdispatch, asyncnet
import lists
import tables
import strutils
import locks
import net
import faststreams/[inputs, outputs]
import times
import ./auth
import ./async_socket_adapters
import ./url
import ./channel
import ./data
import ./methods
import ./spec
import ./frame

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
    inStream: AsyncInputStream
    outStream: AsyncOutputStream
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
    heartbeatTimeout: int
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
proc receiveFrame(conn: AsyncConnection): Future[Frame] {.async.}

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
        # noinspection PyTypeChecker
        self.connection_tune = await self.__rpc(
            spec.Connection.StartOk(
                client_properties=self._client_properties(
                    **(client_properties or {}),
                ),
                mechanism=credentials.name,
                response=credentials.value(self).marshal(),
            ),
        )  # type: spec.Connection.Tune

        if self.heartbeat_timeout > 0:
            self.connection_tune.heartbeat = self.heartbeat_timeout

        await self.__rpc(
            spec.Connection.TuneOk(
                channel_max=self.connection_tune.channel_max,
                frame_max=self.connection_tune.frame_max,
                heartbeat=self.connection_tune.heartbeat,
            ),
            wait_response=False,
        )

        await self.__rpc(spec.Connection.Open(virtual_host=self.vhost))

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
  result.inStream = asyncSocketInput(result.sock)
  result.outStream = asyncSocketOutput(result.sock)
  let pHeader = newProtocolHeader()
  await pHeader.encodeFrame(result.outStream)
  let res: ConnectionStart = cast[ConnectionStart](await result.receiveFrame())
  result.heartbeatLastReceived = getTime()
  result.auth = getAuthMechanism(res.mechanisms)
  result.serverProps = res.properties


proc receiveFrame(conn: AsyncConnection): Future[Frame] {.async.} =
  var frameHeader: openArray[byte] = await conn.inStream.read(1)
  if frameHeader[0] == 0x00:
    raise newException(AMQPFrameError, await encoded.read())
  frameHeader.add(await conn.inStream.read(6))
  let startFrame = frameHeader.startsWith("AMQP")
  if not conn.started and startFrame:
    raise newException(AMQPSyntaxError)
  else:
    conn.started = true
  if not startFrame:
    let headerStream = unsafeMemoryInput(frameHeader)
    let(_, _, fSize) = getFrameHeaderInfo(headerStream.s)
    frameHeader.add(await conn.reader.read(fSize+1))
    headerStream.close()
  let frameStream = unsafeMemoryInput(frameHeader)
  result = decodeFrame(frameStream.s, startFrame)
  frameStream.close()
