import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../field
import ../exceptions

const CONNECTION_METHODS* = 0x000A.uint16
const CONNECTION_START_METHOD_ID* = 0x000A000A.uint32
const CONNECTION_START_OK_METHOD_ID* = 0x000A000B.uint32
const CONNECTION_SECURE_METHOD_ID* = 0x000A0014.uint32
const CONNECTION_SECURE_OK_METHOD_ID* = 0x000A0015.uint32
const CONNECTION_TUNE_METHOD_ID* = 0x000A001E.uint32
const CONNECTION_TUNE_OK_METHOD_ID* = 0x000A001F.uint32
const CONNECTION_OPEN_METHOD_ID* = 0x000A0028.uint32
const CONNECTION_OPEN_OK_METHOD_ID* = 0x000A0029.uint32
const CONNECTION_CLOSE_METHOD_ID* = 0x000A0032.uint32
const CONNECTION_CLOSE_OK_METHOD_ID* = 0x000A0033.uint32
const CONNECTION_BLOCKED_METHOD_ID* = 0x000A003C.uint32
const CONNECTION_UNBLOCKED_METHOD_ID* = 0x000A003D.uint32

type
  AMQPConnectionKind = enum
    AMQP_CONNECTION_NONE = 0
    AMQP_CONNECTION_START_SUBMETHOD = (CONNECTION_START_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_START_OK_SUBMETHOD = (CONNECTION_START_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_SECURE_SUBMETHOD = (CONNECTION_SECURE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_SECURE_OK_SUBMETHOD = (CONNECTION_SECURE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_TUNE_SUBMETHOD = (CONNECTION_TUNE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_TUNE_OK_SUBMETHOD = (CONNECTION_TUNE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_OPEN_SUBMETHOD = (CONNECTION_OPEN_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_OPEN_OK_SUBMETHOD = (CONNECTION_OPEN_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_CLOSE_SUBMETHOD = (CONNECTION_CLOSE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_CLOSE_OK_SUBMETHOD = (CONNECTION_CLOSE_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_BLOCKED_SUBMETHOD = (CONNECTION_BLOCKED_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CONNECTION_UNBLOCKED_SUBMETHOD = (CONNECTION_UNBLOCKED_METHOD_ID and 0x0000FFFF).uint16

  AMQPConnection* = ref AMQPConnectionObj
  AMQPConnectionObj* = object of RootObj
    case kind: AMQPConnectionKind
    of AMQP_CONNECTION_START_SUBMETHOD:
      versionMajor*: uint8
      versionMinor*: uint8
      serverProperties*: TableRef[string, Field]
      mechanisms*: string
      locales*: string
    of AMQP_CONNECTION_START_OK_SUBMETHOD:
      clientProps*: TableRef[string, Field]
      mechanism*: string
      response*: string
      locale*: string
    else:
      discard

proc decode*(s: AsyncBufferedSocket, t: uint32): Future[AMQPConnection] {.async.} =
  case t:
  of CONNECTION_START_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_START_SUBMETHOD)
    result.versionMajor = await s.readU8()
    result.versionMinor = await s.readU8()
    result.serverProperties = await s.decodeTable()
    result.mechanisms = await s.decodeString()
    result.locales = await s.decodeString()
  of CONNECTION_START_OK_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_START_OK_SUBMETHOD)
    result.clientProps = await s.decodeTable()
    result.mechanism = await s.decodeString()
    result.response = await s.decodeString()
    result.locale = await s.decodeShortString()
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPConnection, dst: AsyncBufferedSocket) {.async.} =
  case meth.kind:
  of AMQP_CONNECTION_START_SUBMETHOD:
    await dst.write(meth.versionMajor)
    await dst.write(meth.versionMinor)
    await dst.encodeTable(meth.serverProperties)
    await dst.encodeString(meth.mechanisms)
    await dst.encodeString(meth.locales)
  of AMQP_CONNECTION_START_OK_SUBMETHOD:
    await dst.encodeTable(meth.clientProps)
    await dst.encodeString(meth.mechanism)
    await dst.encodeString(meth.response)
    await dst.encodeShortString(meth.locale)
  else:
      raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc newConnectionStartOk*(clientProps: TableRef[string, Field], mechanism="PLAIN", response="", locale="en_US"): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_START_OK_SUBMETHOD, 
    clientProps: clientProps, 
    mechanism: mechanism, 
    response: response, 
    locale: locale
  )

#[
type
  ConnectionVariants* = enum
    NONE = 0
    CONNECTION_START_METHOD = (CONNECTION_START_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_START_OK_METHOD = (CONNECTION_START_OK_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_SECURE_METHOD = (CONNECTION_SECURE_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_SECURE_OK_METHOD = (CONNECTION_SECURE_OK_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_TUNE_METHOD = (CONNECTION_TUNE_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_TUNE_OK_METHOD = (CONNECTION_TUNE_OK_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_OPEN_METHOD = (CONNECTION_OPEN_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_OPEN_OK_METHOD = (CONNECTION_OPEN_OK_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_CLOSE_METHOD = (CONNECTION_CLOSE_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_CLOSE_OK_METHOD = (CONNECTION_CLOSE_OK_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_BLOCKED_METHOD = (CONNECTION_BLOCKED_METHOD_ID and 0x0000FFFF).uint16
    CONNECTION_UNBLOCKED_METHOD = (CONNECTION_UNBLOCKED_METHOD_ID and 0x0000FFFF).uint16

type 
  ConnectionMethod* = ref object of SubMethod
    case indexLo*: ConnectionVariants
    of CONNECTION_START_METHOD:
      major*: uint8
      minor*: uint8
      properties*: TableRef[string, Field]
      mechanisms*: string
      locales*: string
    of CONNECTION_START_OK_METHOD, CONNECTION_SECURE_OK_METHOD:
      clientProps*: TableRef[string, Field]
      mechanism*: string
      response*: string
      locale*: string
    of CONNECTION_SECURE_METHOD:
      challenge*: string
    of CONNECTION_TUNE_METHOD, CONNECTION_TUNE_OK_METHOD:
      channelMax*: uint16
      frameMax*: uint32
      heartbeat*: uint16
    of CONNECTION_OPEN_METHOD:  
      virtualHost*: string
      capabilities*: string
      insist*: bool
    of CONNECTION_OPEN_OK_METHOD:  
      knownHosts*: string
    of CONNECTION_CLOSE_METHOD:
      replyCode*: uint16
      replyText*: string
      classId*: uint16
      methodId*: uint16
    of CONNECTION_CLOSE_OK_METHOD:  
      discard
    of CONNECTION_BLOCKED_METHOD:
      reason*: string
    of CONNECTION_UNBLOCKED_METHOD:
      discard
    else:
      discard

proc decodeConnectionStart(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionStart(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionStartOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionStartOk(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionSecure(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionSecure(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionSecureOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionSecureOk(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionTune(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionTune(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionTuneOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionTuneOk(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionOpen(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionOpen(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionOpenOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionOpenOk(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionClose(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionClose(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionCloseOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionCloseOk(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionBlocked(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionBlocked(to: OutputStream, data: ConnectionMethod)
proc decodeConnectionUnblocked(encoded: InputStream): (bool, seq[uint16], ConnectionMethod)
proc encodeConnectionUnblocked(to: OutputStream, data: ConnectionMethod)

proc decode*(_: type[ConnectionMethod], submethodId: ConnectionVariants, encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  case submethodId
  of CONNECTION_START_METHOD:
    result = decodeConnectionStart(encoded)
  of CONNECTION_START_OK_METHOD:
    result = decodeConnectionStartOk(encoded)
  of CONNECTION_SECURE_METHOD:
    result = decodeConnectionSecure(encoded)
  of CONNECTION_SECURE_OK_METHOD:
    result = decodeConnectionSecureOk(encoded)
  of CONNECTION_TUNE_METHOD:
    result = decodeConnectionTune(encoded)
  of CONNECTION_TUNE_OK_METHOD:
    result = decodeConnectionTuneOk(encoded)
  of CONNECTION_OPEN_METHOD:
    result = decodeConnectionOpen(encoded)
  of CONNECTION_OPEN_OK_METHOD:
    result = decodeConnectionOpenOk(encoded)
  of CONNECTION_CLOSE_METHOD:
    result = decodeConnectionClose(encoded)
  of CONNECTION_CLOSE_OK_METHOD:
    result = decodeConnectionCloseOk(encoded)
  of CONNECTION_BLOCKED_METHOD:
    result = decodeConnectionBlocked(encoded)
  of CONNECTION_UNBLOCKED_METHOD:
    result = decodeConnectionUnblocked(encoded)
  else:
      discard

proc encode*(to: OutputStream, data: ConnectionMethod) =
  case data.indexLo
  of CONNECTION_START_METHOD:
    to.encodeConnectionStart(data)
  of CONNECTION_START_OK_METHOD:
    to.encodeConnectionStartOk(data)
  of CONNECTION_SECURE_METHOD:
    to.encodeConnectionSecure(data)
  of CONNECTION_SECURE_OK_METHOD:
    to.encodeConnectionSecureOk(data)
  of CONNECTION_TUNE_METHOD:
    to.encodeConnectionTune(data)
  of CONNECTION_TUNE_OK_METHOD:
    to.encodeConnectionTuneOk(data)
  of CONNECTION_OPEN_METHOD:
    to.encodeConnectionOpen(data)
  of CONNECTION_OPEN_OK_METHOD:
    to.encodeConnectionOpenOk(data)
  of CONNECTION_CLOSE_METHOD:
    to.encodeConnectionClose(data)
  of CONNECTION_CLOSE_OK_METHOD:
    to.encodeConnectionCloseOk(data)
  of CONNECTION_BLOCKED_METHOD:
    to.encodeConnectionBlocked(data)
  of CONNECTION_UNBLOCKED_METHOD:
    to.encodeConnectionUnblocked(data)
  else:
    discard
#--------------- Connection.Start ---------------#

proc newConnectionStart*(major = PROTOCOL_VERSION[0], minor = PROTOCOL_VERSION[1], properties: TableRef[string, Field]=nil, mechanisms="PLAIN", locales="en_US"): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_START_METHOD)
  res.major = major
  res.minor = minor
  res.properties = properties
  res.mechanisms = mechanisms
  res.locales = locales
  result = (true, @[ord(CONNECTION_START_OK_METHOD).uint16], res)

proc decodeConnectionStart(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, major) = encoded.readBigEndianU8()
  let (_, minor) = encoded.readBigEndianU8()
  let (_, properties) = decodeTable(encoded)
  let (_, mechanisms) = encoded.readString()
  let (_, locales) = encoded.readString()
  result = newConnectionStart(major, minor, properties, mechanisms, locales)

proc encodeConnectionStart(to: OutputStream, data: ConnectionMethod) =
  to.writeBigEndian8(data.major)
  to.writeBigEndian8(data.minor)
  to.encodeTable(data.properties)
  to.writeString(data.mechanisms)
  to.writeString(data.locales)

#--------------- Connection.StartOk ---------------#

proc newConnectionStartOk*(clientProps: TableRef[string, Field] = nil, mechanisms="PLAIN", response="", locales="en_US"): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_START_OK_METHOD)
  res.clientProps = clientProps
  res.mechanism = mechanisms
  res.response = response
  res.locale = locales
  result = (false, @[], res)

proc decodeConnectionStartOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, clientProps) = decodeTable(encoded)
  let (_, mechanism) = encoded.readShortString()
  let (_, response) = encoded.readString()
  let (_, locale) = encoded.readShortString()
  result = newConnectionStartOk(clientProps, mechanism, response, locale)

proc encodeConnectionStartOk(to: OutputStream, data: ConnectionMethod) =
  to.encodeTable(data.clientProps)
  to.writeShortString(data.mechanism)
  to.writeString(data.response)
  to.writeShortString(data.locale)

proc newClientProps*(connName: string): TableRef[string, Field] =
  result = {
    "platform": Field(dtype: dtString, stringVal: PLATFORM),
    "version": Field(dtype: dtString, stringVal: "0.0.1"),
    "product": Field(dtype: dtString, stringVal: PRODUCT),
    "capabilities": Field(dtype: dtTable, tableVal: {
      "authentication_failure_close": Field(dtype: dtBool, boolVal: true),
      "basic.nack": Field(dtype: dtBool, boolVal: true),
      "connection.blocked": Field(dtype: dtBool, boolVal: false),
      "consumer_cancel_notify": Field(dtype: dtBool, boolVal: true),
      "publisher_confirms": Field(dtype: dtBool, boolVal: true),
    }.newTable()),
    "information": Field(dtype: dtString, stringVal: "See https://github.com/Q-Master/rabbitmq.nim"),
  }.newTable()
  if connName != "":
    result["connection_name"] = Field(dtype: dtString, stringVal: connName)

#--------------- Connection.Secure ---------------#

proc newConnectionSecure*(challenge: string = ""): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_SECURE_METHOD)
  res.challenge = challenge
  result = (true, @[ord(CONNECTION_SECURE_OK_METHOD).uint16], res)

proc decodeConnectionSecure(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, challenge) = encoded.readString()
  result = newConnectionSecure(challenge)

proc encodeConnectionSecure(to: OutputStream, data: ConnectionMethod) =
  to.writeString(data.challenge)

#--------------- Connection.SecureOk ---------------#

proc newConnectionSecureOk*(response: string = ""): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_SECURE_OK_METHOD)
  res.response = response
  result = (false, @[], res)

proc decodeConnectionSecureOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, response) = encoded.readString()
  result = newConnectionSecureOk(response)

proc encodeConnectionSecureOk(to: OutputStream, data: ConnectionMethod) =
  to.writeString(data.response)

#--------------- Connection.Tune ---------------#

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_TUNE_METHOD)
  res.channelMax = channelMax
  res.frameMax = frameMax
  res.heartbeat = heartbeat
  result = (true, @[ord(CONNECTION_TUNE_OK_METHOD).uint16], res)

proc decodeConnectionTune(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, channelMax) = encoded.readBigEndianU16()
  let (_, frameMax) = encoded.readBigEndianU32()
  let (_, heartbeat) = encoded.readBigEndianU16()
  result = newConnectionTune(channelMax, frameMax, heartbeat)

proc encodeConnectionTune(to: OutputStream, data: ConnectionMethod) =
  to.writeBigEndian16(data.channelMax)
  to.writeBigEndian32(data.frameMax)
  to.writeBigEndian16(data.heartbeat)

#--------------- Connection.TuneOk ---------------#

proc newConnectionTuneOk*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_TUNE_OK_METHOD)
  res.channelMax = channelMax
  res.frameMax = frameMax
  res.heartbeat = heartbeat
  result = (false, @[], res)

proc decodeConnectionTuneOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, channelMax) = encoded.readBigEndianU16()
  let (_, frameMax) = encoded.readBigEndianU32()
  let (_, heartbeat) = encoded.readBigEndianU16()
  result = newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc encodeConnectionTuneOk(to: OutputStream, data: ConnectionMethod) =
  to.writeBigEndian16(data.channelMax)
  to.writeBigEndian32(data.frameMax)
  to.writeBigEndian16(data.heartbeat)

#--------------- Connection.Open ---------------#

proc newConnectionOpen*(virtualHost = "/", capabilities = "", insist = false): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_OPEN_METHOD)
  res.virtualHost = virtualHost
  res.capabilities = capabilities
  res.insist = insist
  result = (true, @[ord(CONNECTION_OPEN_OK_METHOD).uint16], res)

proc decodeConnectionOpen(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, virtualHost) = encoded.readShortString()
  let (_, capabilities) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let insist = (bbuf and 0x01) != 0
  result = newConnectionOpen(virtualHost, capabilities, insist)

proc encodeConnectionOpen(to: OutputStream, data: ConnectionMethod) =
  let bbuf: uint8 = (if data.insist: 0x01 else: 0x00)
  to.writeShortString(data.virtualHost)
  to.writeShortString(data.capabilities)
  to.writeBigEndian8(bbuf)

#--------------- Connection.OpenOk ---------------#

proc newConnectionOpenOk*(knownHosts = ""): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_OPEN_OK_METHOD)
  res.knownHosts = knownHosts
  result = (false, @[], res)

proc decodeConnectionOpenOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, knownHosts) = encoded.readShortString()
  result = newConnectionOpenOk(knownHosts)

proc encodeConnectionOpenOk(to: OutputStream, data: ConnectionMethod) =
  to.writeShortString(data.knownHosts)

#--------------- Connection.Close ---------------#

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_CLOSE_METHOD)
  res.replyCode = replyCode
  res.replyText = replyText
  res.classId = classId
  res.methodId = methodId
  result = (true, @[ord(CONNECTION_CLOSE_OK_METHOD).uint16], res)

proc decodeConnectionClose(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, classId) = encoded.readBigEndianU16()
  let (_, methodId) = encoded.readBigEndianU16()
  result = newConnectionClose(replyCode, replyText, classId, methodId)

proc encodeConnectionClose(to: OutputStream, data: ConnectionMethod) =
  to.writeBigEndian16(data.replyCode)
  to.writeShortString(data.replyText)
  to.writeBigEndian16(data.classId)
  to.writeBigEndian16(data.methodId)

proc checkCloseReason*(data: ConnectionMethod) =
  if data.indexLo == CONNECTION_CLOSE_METHOD:
    case data.replyCode
    of 501:
      raise newException(ConnectionFrameError, data.replyText)
    of 502:
      raise newException(ConnectionSyntaxError, data.replyText)
    of 503:
      raise newException(ConnectionCommandInvalid, data.replyText)
    of 504:
      raise newException(ConnectionChannelError, data.replyText)
    of 505:
      raise newException(ConnectionUnexpectedFrame, data.replyText)
    of 506:
      raise newException(ConnectionResourceError, data.replyText)
    of 530:
      raise newException(ConnectionNotAllowed, data.replyText)
    of 540:
      raise newException(ConnectionNotImplemented, data.replyText)
    of 541:
      raise newException(ConnectionInternalError, data.replyText)
    else:
      raise newException(ConnectionClosed, data.replyText)

#--------------- Connection.CloseOk ---------------#

proc newConnectionCloseOk*(): (bool, seq[uint16], ConnectionMethod) =
  result = (false, @[], ConnectionMethod(indexLo: CONNECTION_CLOSE_OK_METHOD))

proc decodeConnectionCloseOk(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) = newConnectionCloseOk()

proc encodeConnectionCloseOk(to: OutputStream, data: ConnectionMethod) = discard

#--------------- Connection.Blocked ---------------#

proc newConnectionBlocked*(reason = ""): (bool, seq[uint16], ConnectionMethod) =
  var res = ConnectionMethod(indexLo: CONNECTION_BLOCKED_METHOD)
  res.reason = reason
  result = (false, @[], res)

proc decodeConnectionBlocked(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) =
  let (_, reason) = encoded.readShortString()
  result = newConnectionBlocked(reason)

proc encodeConnectionBlocked(to: OutputStream, data: ConnectionMethod) =
  to.writeShortString(data.reason)

#--------------- Connection.Unblocked ---------------#

proc newConnectionUnblocked*(): (bool, seq[uint16], ConnectionMethod) =
  result = (false, @[], ConnectionMethod(indexLo: CONNECTION_UNBLOCKED_METHOD))

proc decodeConnectionUnblocked(encoded: InputStream): (bool, seq[uint16], ConnectionMethod) = newConnectionUnblocked()

proc encodeConnectionUnblocked(to: OutputStream, data: ConnectionMethod) = discard
]#