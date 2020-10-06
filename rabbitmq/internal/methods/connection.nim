import asyncdispatch
import faststreams/[inputs, outputs]
import tables
import ./mthd
import ../spec
import ../data
import ../exceptions

type 
  ConnectionStart* = ref object of Method
    major: uint8
    minor: uint8
    properties: TableRef[string, DataTable]
    mechanisms: string
    locales: string
  ConnectionStartOk* = ref object of Method
    clientProps: TableRef[string, DataTable]
    mechanism: string
    response: string
    locale: string
  ConnectionSecure* = ref object of Method
    challenge: string
  ConnectionSecureOk* = ref object of Method
    response: string
  ConnectionTune* = ref object of Method
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16
  ConnectionTuneOk* = ref object of Method
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16
  ConnectionOpen* = ref object of Method
    virtualHost: string
    capabilities: string
    insist: bool
  ConnectionOpenOk* = ref object of Method
    knownHosts: string
  ConnectionClose* = ref object of Method
    replyCode: uint16
    replyText: string
    classId: uint16
    methodId: uint16
  ConnectionCloseOk* = ref object of Method
  ConnectionBlocked* = ref object of Method
    reason: string
  ConnectionUnblocked* = ref object of Method

#--------------- Connection.Start ---------------#

proc newConnectionStart*(major = PROTOCOL_VERSION[0], minor = PROTOCOL_VERSION[1], properties: TableRef[string, DataTable]=nil, mechanisms="PLAIN", locales="en_US"): ConnectionStart =
  result.new
  result.initMethod(true, 0x000A000A)
  result.major = major
  result.minor = minor
  result.properties = properties
  result.mechanisms = mechanisms
  result.locales = locales

proc decode*(_: type[ConnectionStart], encoded: AsyncInputStream): Future[ConnectionStart] {.async.} =
  let (_, major) = await encoded.readBigEndianU8()
  let (_, minor) = await encoded.readBigEndianU8()
  let (_, properties) = await decodeTable(encoded)
  let (_, mechanisms) = await encoded.readString()
  let (_, locales) = await encoded.readString()
  result = newConnectionStart(major, minor, properties, mechanisms, locales)

proc encode*(self: ConnectionStart, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian8(self.major)
  discard await to.writeBigEndian8(self.minor)
  discard await to.encodeTable(self.properties)
  discard await to.writeString(self.mechanisms)
  discard await to.writeString(self.locales)

#--------------- Connection.StartOk ---------------#

proc newConnectionStartOk*(clientProps: TableRef[string, DataTable] = nil, mechanisms="PLAIN", response="", locales="en_US"): ConnectionStartOk =
  result.new
  result.initMethod(false, 0x000A000B)
  result.clientProps = clientProps
  result.mechanism = mechanisms
  result.response = response
  result.locale = locales

proc decode*(_: type[ConnectionStartOk], encoded: AsyncInputStream): Future[ConnectionStartOk] {.async.} =
  let (_, clientProps) = await decodeTable(encoded)
  let (_, mechanism) = await encoded.readShortString()
  let (_, response) = await encoded.readString()
  let (_, locale) = await encoded.readShortString()
  result = newConnectionStartOk(clientProps, mechanism, response, locale)

proc encode*(self: ConnectionStartOk, to: AsyncOutputStream) {.async.} =
  discard await to.encodeTable(self.clientProps)
  discard await to.writeShortString(self.mechanism)
  discard await to.writeString(self.response)
  discard await to.writeShortString(self.locale)

#--------------- Connection.Secure ---------------#

proc newConnectionSecure*(challenge: string = ""): ConnectionSecure =
  result.new
  result.initMethod(true, 0x000A0014)
  result.challenge = challenge

proc decode*(_: type[ConnectionSecure], encoded: AsyncInputStream): Future[ConnectionSecure] {.async.} =
  let (_, challenge) = await encoded.readString()
  result = newConnectionSecure(challenge)

proc encode*(self: ConnectionSecure, to: AsyncOutputStream) {.async.} =
  discard await to.writeString(self.challenge)

#--------------- Connection.SecureOk ---------------#

proc newConnectionSecureOk*(response: string = ""): ConnectionSecureOk =
  result.new
  result.initMethod(false, 0x000A0015)
  result.response = response

proc decode*(_: type[ConnectionSecureOk], encoded: AsyncInputStream): Future[ConnectionSecureOk] {.async.} =
  let (_, response) = await encoded.readString()
  result = newConnectionSecureOk(response)

proc encode*(self: ConnectionSecureOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeString(self.response)

#--------------- Connection.Tune ---------------#

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTune =
  result.new
  result.initMethod(true, 0x000A001E)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

proc decode*(_: type[ConnectionTune], encoded: AsyncInputStream): Future[ConnectionTune] {.async.} =
  let (_, channelMax) = await encoded.readBigEndianU16()
  let (_, frameMax) = await encoded.readBigEndianU32()
  let (_, heartbeat) = await encoded.readBigEndianU16()
  result = newConnectionTune(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTune, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian16(self.channelMax)
  discard await to.writeBigEndian32(self.frameMax)
  discard await to.writeBigEndian16(self.heartbeat)

#--------------- Connection.TuneOk ---------------#

proc newConnectionTuneOk*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTuneOk =
  result.new
  result.initMethod(false, 0x000A001F)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

proc decode*(_: type[ConnectionTuneOk], encoded: AsyncInputStream): Future[ConnectionTuneOk] {.async.} =
  let (_, channelMax) = await encoded.readBigEndianU16()
  let (_, frameMax) = await encoded.readBigEndianU32()
  let (_, heartbeat) = await encoded.readBigEndianU16()
  result = newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTuneOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian16(self.channelMax)
  discard await to.writeBigEndian32(self.frameMax)
  discard await to.writeBigEndian16(self.heartbeat)

#--------------- Connection.Open ---------------#

proc newConnectionOpen*(virtualHost = "/", capabilities = "", insist = false): ConnectionOpen =
  result.new
  result.initMethod(true, 0x000A0028)
  result.virtualHost = virtualHost
  result.capabilities = capabilities
  result.insist = insist

proc decode*(_: type[ConnectionOpen], encoded: AsyncInputStream): Future[ConnectionOpen] {.async.} =
  let (_, virtualHost) = await encoded.readShortString()
  let (_, capabilities) = await encoded.readShortString()
  let (_, bbuf) = await encoded.readUint8()
  let insist = (bbuf and 0x01) != 0
  result = newConnectionOpen(virtualHost, capabilities, insist)

proc encode*(self: ConnectionOpen, to: AsyncOutputStream) {.async.} =
  let bbuf: uint8 = (if self.insist: 0x01 else: 0x00)
  discard await to.writeShortString(self.virtualHost)
  discard await to.writeShortString(self.capabilities)
  discard await to.writeBigEndian8(bbuf)

#--------------- Connection.OpenOk ---------------#

proc newConnectionOpenOk*(knownHosts = ""): ConnectionOpenOk =
  result.new
  result.initMethod(false, 0x000A0029)
  result.knownHosts = knownHosts

proc decode*(_: type[ConnectionOpenOk], encoded: AsyncInputStream): Future[ConnectionOpenOk] {.async.} =
  let (_, knownHosts) = await encoded.readShortString()
  result = newConnectionOpenOk(knownHosts)

proc encode*(self: ConnectionOpenOk, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.knownHosts)

#--------------- Connection.Close ---------------#

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ConnectionClose =
  result.new
  result.initMethod(true, 0x000A0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

proc decode*(_: type[ConnectionClose], encoded: AsyncInputStream): Future[ConnectionClose] {.async.} =
  let (_, replyCode) = await encoded.readBigEndianU16()
  let (_, replyText) = await encoded.readShortString()
  let (_, classId) = await encoded.readUint16()
  let (_, methodId) = await encoded.readUint16()
  result = newConnectionClose(replyCode, replyText, classId, methodId)

proc encode*(self: ConnectionClose, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian16(self.replyCode)
  discard await to.writeShortString(self.replyText)
  discard await to.writeBigEndian16(self.classId)
  discard await to.writeBigEndian16(self.methodId)

proc checkCloseReason*(self: ConnectionClose) =
  case self.replyCode
  of 501:
    raise newException(ConnectionFrameError, self.replyText)
  of 502:
    raise newException(ConnectionSyntaxError, self.replyText)
  of 503:
    raise newException(ConnectionCommandInvalid, self.replyText)
  of 504:
    raise newException(ConnectionChannelError, self.replyText)
  of 505:
    raise newException(ConnectionUnexpectedFrame, self.replyText)
  of 506:
    raise newException(ConnectionResourceError, self.replyText)
  of 530:
    raise newException(ConnectionNotAllowed, self.replyText)
  of 540:
    raise newException(ConnectionNotImplemented, self.replyText)
  of 541:
    raise newException(ConnectionInternalError, self.replyText)
  else:
    raise newException(ConnectionClosed, self.replyText)

#--------------- Connection.CloseOk ---------------#

proc newConnectionCloseOk*(): ConnectionCloseOk =
  result.new
  result.initMethod(false, 0x000A0033)

proc decode*(_: type[ConnectionCloseOk], encoded: AsyncInputStream): Future[ConnectionCloseOk] {.async.} = newConnectionCloseOk()

proc encode*(self: ConnectionCloseOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Connection.Blocked ---------------#

proc newConnectionBlocked*(reason = ""): ConnectionBlocked =
  result.new
  result.initMethod(false, 0x000A003C)
  result.reason = reason

proc decode*(_: type[ConnectionBlocked], encoded: AsyncInputStream): Future[ConnectionBlocked] {.async.} =
  let (_, reason) = await encoded.readShortString()
  result = newConnectionBlocked(reason)

proc encode*(self: ConnectionBlocked, to: AsyncOutputStream) {.async.} =
  discard await to.writeShortString(self.reason)

#--------------- Connection.Unblocked ---------------#

proc newConnectionUnblocked*(): ConnectionUnblocked =
  result.new
  result.initMethod(false, 0x000A003D)

proc decode*(_: type[ConnectionUnblocked], encoded: AsyncInputStream): Future[ConnectionUnblocked] {.async.} = newConnectionUnblocked()

proc encode*(self: ConnectionUnblocked, to: AsyncOutputStream) {.async.} = discard
