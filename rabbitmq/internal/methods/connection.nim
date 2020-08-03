import streams
import tables
import ./mthd
import ../spec
import ../data

type ConnectionStart* = ref object of Method
  major: uint8
  minor: uint8
  properties: TableRef[string, DataTable]
  mechanisms: string
  locales: string

proc newConnectionStart*(major: uint8 = PROTOCOL_VERSION[0], minor: uint8 = PROTOCOL_VERSION[1], mechanisms="PLAIN", locales="en_US"): ConnectionStart =
  result.new
  result.initMethod(true, 0x000A000A)
  result.major = major
  result.minor = minor
  result.mechanisms = mechanisms
  result.locales = locales

method decode*(self: ConnectionStart, encoded: Stream): ConnectionStart =
 self.major = encoded.readBigEndianU8()
 self.minor = encoded.readBigEndianU8()
 self.properties = decodeTable(encoded)
 self.mechanisms = encoded.readString()
 self.locales = encoded.readString()
 return self

method encode*(self: ConnectionStart): string =
  var s = newStringStream("")
  s.writeBigEndian8(self.major)
  s.writeBigEndian8(self.minor)
  s.encodeTable(self.properties)
  s.writeString(self.mechanisms)
  s.writeString(self.locales)
  result = s.readAll()
  s.close()

type ConnectionStartOk* = ref object of Method
  clientProps: TableRef[string, DataTable]
  mechanism: string
  response: string
  locale: string

proc newConnectionStartOk*(clientProps: TableRef[string, DataTable] = nil, mechanisms="PLAIN", response="", locales="en_US"): ConnectionStartOk =
  result.new
  result.initMethod(false, 0x000A000B)
  result.clientProps = clientProps
  result.mechanism = mechanisms
  result.response = response
  result.locale = locales

method decode*(self: ConnectionStartOk, encoded: Stream): ConnectionStartOk =
  self.clientProps = decodeTable(encoded)
  self.mechanism = encoded.readShortString()
  self.response = encoded.readString()
  self.locale = encoded.readShortString()
  return self

method encode*(self: ConnectionStartOk): string =
  var s = newStringStream("")
  s.encodeTable(self.clientProps)
  s.writeShortString(self.mechanism)
  s.writeString(self.response)
  s.writeShortString(self.locale)
  result = s.readAll()
  s.close()

type ConnectionSecure* = ref object of Method
  challenge: string

proc newConnectionSecure*(challenge: string = ""): ConnectionSecure =
  result.new
  result.initMethod(true, 0x000A0014)
  result.challenge = challenge

method decode*(self: ConnectionSecure, encoded: Stream): ConnectionSecure =
  self.challenge = encoded.readString()
  return self

method encode*(self: ConnectionSecure): string =
  var s = newStringStream("")
  s.writeString(self.challenge)
  result = s.readAll()
  s.close()

type ConnectionSecureOk* = ref object of Method
  response: string

proc newConnectionSecureOk*(response: string = ""): ConnectionStartOk =
  result.new
  result.initMethod(false, 0x000A0015)
  result.response = response

method decode*(self: ConnectionSecureOk, encoded: Stream): ConnectionSecureOk =
  self.response = encoded.readString()
  return self

method encode*(self: ConnectionSecureOk): string =
  var s = newStringStream("")
  s.writeString(self.response)
  result = s.readAll()
  s.close()

type ConnectionTune* = ref object of Method
  channelMax: uint16
  frameMax: uint32
  heartbeat: uint16

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTune =
  result.new
  result.initMethod(true, 0x000A001E)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

method decode*(self: ConnectionTune, encoded: Stream): ConnectionTune =
  self.channelMax = encoded.readBigEndianU16()
  self.frameMax = encoded.readBigEndianU32()
  self.heartbeat = encoded.readBigEndianU16()
  return self

method encode*(self: ConnectionTune): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.channelMax)
  s.writeBigEndian32(self.frameMax)
  s.writeBigEndian16(self.heartbeat)
  result = s.readAll()
  s.close()

type ConnectionTuneOk* = ref object of ConnectionTune

proc newConnectionTuneOk*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTuneOk =
  result.new
  result.initMethod(false, 0x000A001F)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

method decode*(self: ConnectionTuneOk, encoded: Stream): ConnectionTuneOk =
  self.channelMax = encoded.readBigEndianU16()
  self.frameMax = encoded.readBigEndianU32()
  self.heartbeat = encoded.readBigEndianU16()
  return self

method encode*(self: ConnectionTuneOk): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.channelMax)
  s.writeBigEndian32(self.frameMax)
  s.writeBigEndian16(self.heartbeat)
  result = s.readAll()
  s.close()

type ConnectionOpen* = ref object of Method
  virtualHost: string
  capabilities: string
  insist: bool

proc newConnectionOpen*(virtualHost = "/", capabilities = "", insist = false): ConnectionOpen =
  result.new
  result.initMethod(true, 0x000A0028)
  result.virtualHost = virtualHost
  result.capabilities = capabilities
  result.insist = insist

method decode*(self: ConnectionOpen, encoded: Stream): ConnectionOpen =
  self.virtualHost = encoded.readShortString()
  self.capabilities = encoded.readShortString()
  let bbuf = encoded.readUint8()
  self.insist = (bbuf and 0x01) != 0
  return self

method encode*(self: ConnectionOpen): string =
  var s = newStringStream("")
  s.writeShortString(self.virtualHost)
  s.writeShortString(self.capabilities)
  let bbuf: uint8 = (if self.insist: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type ConnectionOpenOk* = ref object of Method
  knownHosts: string

proc newConnectionOpenOk*(knownHosts = ""): ConnectionOpenOk =
  result.new
  result.initMethod(false, 0x000A0029)
  result.knownHosts = knownHosts

method decode*(self: ConnectionOpenOk, encoded: Stream): ConnectionOpenOk =
  self.knownHosts = encoded.readShortString()
  return self

method encode*(self: ConnectionOpenOk): string =
  var s = newStringStream("")
  s.writeShortString(self.knownHosts)
  result = s.readAll()
  s.close()

type ConnectionClose* = ref object of Method
  replyCode: uint16
  replyText: string
  classId: uint16
  methodId: uint16

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ConnectionClose =
  result.new
  result.initMethod(true, 0x000A0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

method decode*(self: ConnectionClose, encoded: Stream): ConnectionClose =
  self.replyCode = encoded.readBigEndianU16()
  self.replyText = encoded.readShortString()
  self.classId = encoded.readUint16()
  self.methodId = encoded.readUint16()
  return self

method encode*(self: ConnectionClose): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.replyCode)
  s.writeShortString(self.replyText)
  s.writeBigEndian16(self.classId)
  s.writeBigEndian16(self.methodId)
  result = s.readAll()
  s.close()

type ConnectionCloseOk* = ref object of Method

proc newConnectionCloseOk*(): ConnectionCloseOk =
  result.new
  result.initMethod(false, 0x000A0033)

type ConnectionBlocked* = ref object of Method
  reason: string

proc newConnectionBlocked*(reason = ""): ConnectionBlocked =
  result.new
  result.initMethod(false, 0x000A003C)
  result.reason = reason

method decode*(self: ConnectionBlocked, encoded: Stream): ConnectionBlocked =
  self.reason = encoded.readShortString()
  return self

method encode*(self: ConnectionBlocked): string =
  var s = newStringStream("")
  s.writeShortString(self.reason)
  result = s.readAll()
  s.close()

type ConnectionUnblocked* = ref object of Method

proc newConnectionUnblocked*(): ConnectionUnblocked =
  result.new
  result.initMethod(false, 0x000A003D)
