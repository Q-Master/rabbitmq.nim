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

  AMQPConnectionOpenBits = object
    insist {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8
    
  AMQPConnection* = ref AMQPConnectionObj
  AMQPConnectionObj* = object of RootObj
    case kind*: AMQPConnectionKind
    of AMQP_CONNECTION_START_SUBMETHOD:
      versionMajor*: uint8
      versionMinor*: uint8
      serverProperties*: FieldTable
      mechanisms*: string
      locales*: string
    of AMQP_CONNECTION_START_OK_SUBMETHOD:
      clientProps*: FieldTable
      mechanism*: string
      response*: string
      locale*: string
    of AMQP_CONNECTION_TUNE_SUBMETHOD, AMQP_CONNECTION_TUNE_OK_SUBMETHOD:
      channelMax*: uint16
      frameMax*: uint32
      heartbeat*: uint16
    of AMQP_CONNECTION_CLOSE_SUBMETHOD:
      replyCode*: uint16
      replyText*: string
      classId*: uint16
      methodId*: uint16
    of AMQP_CONNECTION_CLOSE_OK_SUBMETHOD, AMQP_CONNECTION_UNBLOCKED_SUBMETHOD:
      discard
    of AMQP_CONNECTION_OPEN_SUBMETHOD:
      virtualHost*: string
      capabilities*: string
      openFlags: AMQPConnectionOpenBits
    of AMQP_CONNECTION_OPEN_OK_SUBMETHOD:
      knownHosts: string
    of AMQP_CONNECTION_BLOCKED_SUBMETHOD:
      reason: string
    else:
      discard

proc len*(meth: AMQPConnection): int =
  result = 0
  case meth.kind:
  of AMQP_CONNECTION_START_SUBMETHOD:
    result.inc(sizeInt8Uint8)
    result.inc(sizeInt8Uint8)
    result.inc(meth.serverProperties.len)
    result.inc(meth.mechanisms.stringLen)
    result.inc(meth.locales.stringLen)
  of AMQP_CONNECTION_START_OK_SUBMETHOD:
    result.inc(meth.clientProps.len)
    result.inc(meth.mechanism.shortStringLen)
    result.inc(meth.response.stringLen)
    result.inc(meth.locale.shortStringLen)
  of AMQP_CONNECTION_TUNE_SUBMETHOD, AMQP_CONNECTION_TUNE_OK_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(sizeInt32Uint32)
    result.inc(sizeInt16Uint16)
  of AMQP_CONNECTION_CLOSE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.replyText.shortStringLen)
    result.inc(sizeInt16Uint16)
    result.inc(sizeInt16Uint16)
  of AMQP_CONNECTION_CLOSE_OK_SUBMETHOD, AMQP_CONNECTION_UNBLOCKED_SUBMETHOD:
    result.inc(0)
  of AMQP_CONNECTION_OPEN_SUBMETHOD:
    result.inc(meth.virtualHost.shortStringLen)
    result.inc(meth.capabilities.shortStringLen)
    result.inc(sizeInt8Uint8)
  of AMQP_CONNECTION_OPEN_OK_SUBMETHOD:
    result.inc(meth.knownHosts.shortStringLen)
  of AMQP_CONNECTION_BLOCKED_SUBMETHOD:
    result.inc(meth.capabilities.shortStringLen)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc decode*(_: typedesc[AMQPConnection], s: AsyncBufferedSocket, t: uint32): Future[AMQPConnection] {.async.} =
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
    result.mechanism = await s.decodeShortString()
    result.response = await s.decodeString()
    result.locale = await s.decodeShortString()
  of CONNECTION_TUNE_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_TUNE_SUBMETHOD)
    result.channelMax = await s.readBEU16()
    result.frameMax = await s.readBEU32()
    result.heartbeat = await s.readBEU16()
  of CONNECTION_TUNE_OK_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_TUNE_OK_SUBMETHOD)
    result.channelMax = await s.readBEU16()
    result.frameMax = await s.readBEU32()
    result.heartbeat = await s.readBEU16()
  of CONNECTION_CLOSE_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_CLOSE_SUBMETHOD)
    result.replyCode = await s.readBEU16()
    result.replyText = await s.decodeShortString()
    result.classId = await s.readBEU16()
    result.methodId = await s.readBEU16()
  of CONNECTION_CLOSE_OK_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_CLOSE_OK_SUBMETHOD)
  of CONNECTION_OPEN_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_OPEN_SUBMETHOD)
    result.virtualHost = await s.decodeShortString()
    result.capabilities = await s.decodeShortString()
    let insist: uint8 = await s.readU8()
    result.openFlags = cast[AMQPConnectionOpenBits](insist)
  of CONNECTION_OPEN_OK_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_OPEN_OK_SUBMETHOD)
    result.knownHosts = await s.decodeShortString()
  of CONNECTION_BLOCKED_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_BLOCKED_SUBMETHOD)
    result.reason = await s.decodeShortString()
  of CONNECTION_UNBLOCKED_METHOD_ID:
    result = AMQPConnection(kind: AMQP_CONNECTION_UNBLOCKED_SUBMETHOD)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPConnection, dst: AsyncBufferedSocket) {.async.} =
  #echo $meth.kind
  case meth.kind:
  of AMQP_CONNECTION_START_SUBMETHOD:
    await dst.write(meth.versionMajor)
    await dst.write(meth.versionMinor)
    await dst.encodeTable(meth.serverProperties)
    await dst.encodeString(meth.mechanisms)
    await dst.encodeString(meth.locales)
  of AMQP_CONNECTION_START_OK_SUBMETHOD:
    await dst.encodeTable(meth.clientProps)
    await dst.encodeShortString(meth.mechanism)
    await dst.encodeString(meth.response)
    await dst.encodeShortString(meth.locale)
  of AMQP_CONNECTION_TUNE_SUBMETHOD, AMQP_CONNECTION_TUNE_OK_SUBMETHOD:
    await dst.writeBE(meth.channelMax)
    await dst.writeBE(meth.frameMax)
    await dst.writeBE(meth.heartbeat)
  of AMQP_CONNECTION_CLOSE_SUBMETHOD:
    await dst.writeBE(meth.replyCode)
    await dst.encodeShortString(meth.replyText)
    await dst.writeBE(meth.classId)
    await dst.writeBE(meth.methodId)
  of AMQP_CONNECTION_CLOSE_OK_SUBMETHOD, AMQP_CONNECTION_UNBLOCKED_SUBMETHOD:
    discard
  of AMQP_CONNECTION_OPEN_SUBMETHOD:
    await dst.encodeShortString(meth.virtualHost)
    await dst.encodeShortString(meth.capabilities)
    await dst.write(cast[uint8](meth.openFlags))
  of AMQP_CONNECTION_OPEN_OK_SUBMETHOD:
    await dst.encodeShortString(meth.knownHosts)
  of AMQP_CONNECTION_BLOCKED_SUBMETHOD:
    await dst.encodeShortString(meth.reason)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc newConnectionStart*(major, minor: uint8, serverprops: FieldTable, mechanisms, locales: string): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_START_SUBMETHOD, 
    versionMajor: major,
    versionMinor: minor,
    serverProperties: serverprops, 
    mechanisms: mechanisms, 
    locales: locales
  )

proc newConnectionStartOk*(clientProps: FieldTable, mechanism="PLAIN", response="", locale="en_US"): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_START_OK_SUBMETHOD, 
    clientProps: clientProps, 
    mechanism: mechanism, 
    response: response, 
    locale: locale
  )

proc newConnectionTune*(channelMax: uint16, frameMax: uint32, heartbeat: uint16): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_TUNE_SUBMETHOD, 
    channelMax: channelMax, 
    frameMax: frameMax, 
    heartbeat: heartbeat
  )

proc newConnectionTuneOk*(channelMax: uint16, frameMax: uint32, heartbeat: uint16): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_TUNE_OK_SUBMETHOD, 
    channelMax: channelMax, 
    frameMax: frameMax, 
    heartbeat: heartbeat
  )

proc newConnectionOpen*(virtualHost: string, caps: string, insist: bool): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_OPEN_SUBMETHOD, 
    virtualHost: virtualHost, 
    capabilities: caps,
    openFlags: AMQPConnectionOpenBits(
      insist: insist
    )
  )

proc newConnectionOpenOk*(knownHosts: string): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_OPEN_OK_SUBMETHOD, 
    knownHosts: knownHosts
  )

proc newConnectionClose*(replyCode: uint16, replyText: string, classId: uint16, methodId: uint16): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_CLOSE_SUBMETHOD,
    replyCode: replyCode,
    replyText: replyText,
    classId: classId,
    methodId: methodId
  )

proc newConnectionCloseOk*(): AMQPConnection =
  result = AMQPConnection(kind: AMQP_CONNECTION_CLOSE_OK_SUBMETHOD)

proc newConnectionBlocked*(reason: string): AMQPConnection =
  result = AMQPConnection(
    kind: AMQP_CONNECTION_BLOCKED_SUBMETHOD,
    reason: reason
  )

proc newConnectionUnblocked*(): AMQPConnection =
  result = AMQPConnection(kind: AMQP_CONNECTION_UNBLOCKED_SUBMETHOD)

proc insist*(self: AMQPConnection): bool =
  case self.kind
  of AMQP_CONNECTION_OPEN_SUBMETHOD:
    result = self.openFlags.insist
  else:
    raise newException(FieldDefect, "No such field")

proc `insist=`*(self: AMQPConnection, insist: bool) =
  case self.kind
  of AMQP_CONNECTION_OPEN_SUBMETHOD:
    self.openFlags.insist = insist
  else:
    raise newException(FieldDefect, "No such field")
