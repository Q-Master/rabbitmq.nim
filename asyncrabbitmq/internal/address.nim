import std/[tables, net, uri, strutils, algorithm, random]

type
  LoginData = ref LoginDataObj
  LoginDataObj = object
    username*: string
    password*: string

  RMQAddress* = ref RMQAddressObj
  RMQAddressObj* = object
    host*: string
    port*: Port
    login*: LoginData
    vhost*: string
    secure*: bool
    sslCtx*: SslContext
    options*: Table[string, string]

  RMQConnectionOrder* = enum
    RMQ_CONNECTION_DIRECT
    RMQ_CONNECTION_RANDOM
    RMQ_CONNECTION_ASCENDING
    RMQ_CONNECTION_DESCENDING

  RMQAddresses* = ref RMQAddressesObj
  RMQAddressesObj* = object
    current*: int
    order*: RMQConnectionOrder
    addresses*: seq[RMQAddress]

let DEFAULT_PORTS = {
  "amqp": 5672,
  "amqps": 5671,
}.toTable()

const DEFAULT_USER* = "guest"
const DEFAULT_PASSWORD* = "guest"

proc newLogin(): LoginData =
  result.new
  result.username = DEFAULT_USER
  result.password = DEFAULT_PASSWORD

proc fromURL*(url: string): RMQAddress =
  result.new
  let rmqUri = parseUri(url)
  if not rmqUri.isAbsolute():
    raise newException(UriParseError, "AMQP address should start with \"amqp://\" or \"amqps://\"")
  if rmqUri.scheme notin ["amqp", "amqps"]:
    raise newException(UriParseError, "AMQP address should start with \"amqp://\" or \"amqps://\"")
  result.host = rmqUri.hostname
  result.port = (if rmqUri.port.len == 0: Port(DEFAULT_PORTS[rmqUri.scheme]) else: Port(rmqUri.port.parseInt))
  result.login = newLogin()

  if url.contains('@'):
    result.login.username = rmqUri.username
    result.login.password = rmqUri.password
  if rmqUri.path[0] == '/' and rmqUri.path.len == 1:
    result.vhost = rmqUri.path
  else:
    result.vhost = (if rmqUri.path.len == 0: "/" else: rmqUri.path[1 .. ^1])

  if rmqUri.query.len > 0:
    let kvarr = rmqUri.query.split('&')
    for pair in kvarr:
      let kvargs = pair.split('=')
      case kvargs.len()
      of 2:
        let k = kvargs[0].decodeUrl()
        let v = kvargs[1].decodeUrl()
        result.options[k] = v
      of 1:
        let k = kvargs[0].decodeUrl()
        result.options[k] = ""
      else:
        discard
  result.secure = false

  if rmqUri.scheme == "amqps":
    result.secure = true
    let caFile = result.options.getOrDefault("cafile", "")
    let caDir = result.options.getOrDefault("capath", "")
    let keyFile = result.options.getOrDefault("keyfile", "")
    let certFile = result.options.getOrDefault("certfile", "")
    let verifyMode = (result.options.getOrDefault("no_verify_ssl", "0") == "0")
    try:
      result.sslCtx = newContext(
        protTLSv1,
        verifyMode = (if verifyMode: CVerifyPeer else: CVerifyNone),
        certFile = certFile, keyFile = keyFile,
        caDir = caDir, caFile = caFile
      )
    except SslError:
      result.sslCtx = newContext(
        verifyMode = (if verifyMode: CVerifyPeer else: CVerifyNone),
        certFile = certFile, keyFile = keyFile,
        caDir = caDir, caFile = caFile
      )

proc fromUrls*(urls: seq[string], order: RMQConnectionOrder = RMQ_CONNECTION_DIRECT): RMQAddresses =
  randomize()
  result.new
  result.order = order
  result.current = 0
  var urllist = urls
  if urls.len == 0:
    raise newException(UriParseError, "Addresses list can't be empty")
  elif urls.len > 1:
    case order
    of RMQ_CONNECTION_ASCENDING:
      sort(urllist, order=SortOrder.Ascending)
    of RMQ_CONNECTION_DESCENDING:
      sort(urllist, order=SortOrder.Descending)
    of RMQ_CONNECTION_RANDOM:
      shuffle(urllist)
    else:
      discard
  for url in urllist:
    result.addresses.add(fromURL(url))

proc next*(addrs: RMQAddresses): RMQAddress =
  result = addrs.addresses[addrs.current]
  addrs.current.inc()
  addrs.current = addrs.current.mod(addrs.addresses.len)
