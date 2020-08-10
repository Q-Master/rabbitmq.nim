import streams
import tables
import ./mthd
import ../spec
import ../data

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

proc decode*(_: type[ConnectionStart], encoded: Stream): ConnectionStart =
  let major = encoded.readBigEndianU8()
  let minor = encoded.readBigEndianU8()
  let properties = decodeTable(encoded)
  let mechanisms = encoded.readString()
  let locales = encoded.readString()
  result = newConnectionStart(major, minor, properties, mechanisms, locales)

proc encode*(self: ConnectionStart, to: Stream) =
  to.writeBigEndian8(self.major)
  to.writeBigEndian8(self.minor)
  to.encodeTable(self.properties)
  to.writeString(self.mechanisms)
  to.writeString(self.locales)

#--------------- Connection.StartOk ---------------#

proc newConnectionStartOk*(clientProps: TableRef[string, DataTable] = nil, mechanisms="PLAIN", response="", locales="en_US"): ConnectionStartOk =
  result.new
  result.initMethod(false, 0x000A000B)
  result.clientProps = clientProps
  result.mechanism = mechanisms
  result.response = response
  result.locale = locales

proc decode*(_: type[ConnectionStartOk], encoded: Stream): ConnectionStartOk =
  let clientProps = decodeTable(encoded)
  let mechanism = encoded.readShortString()
  let response = encoded.readString()
  let locale = encoded.readShortString()
  result = newConnectionStartOk(clientProps, mechanism, response, locale)

proc encode*(self: ConnectionStartOk, to: Stream) =
  to.encodeTable(self.clientProps)
  to.writeShortString(self.mechanism)
  to.writeString(self.response)
  to.writeShortString(self.locale)

#--------------- Connection.Secure ---------------#

proc newConnectionSecure*(challenge: string = ""): ConnectionSecure =
  result.new
  result.initMethod(true, 0x000A0014)
  result.challenge = challenge

proc decode*(_: type[ConnectionSecure], encoded: Stream): ConnectionSecure =
  let challenge = encoded.readString()
  result = newConnectionSecure(challenge)

proc encode*(self: ConnectionSecure, to: Stream) =
  to.writeString(self.challenge)

#--------------- Connection.SecureOk ---------------#

proc newConnectionSecureOk*(response: string = ""): ConnectionStartOk =
  result.new
  result.initMethod(false, 0x000A0015)
  result.response = response

proc decode*(_: type[ConnectionSecureOk], encoded: Stream): ConnectionSecureOk =
  let response = encoded.readString()
  result = newConnectionSecureOk(response)

proc encode*(self: ConnectionSecureOk, to: Stream) =
  to.writeString(self.response)

#--------------- Connection.Tune ---------------#

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTune =
  result.new
  result.initMethod(true, 0x000A001E)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

proc decode*(_: type[ConnectionTune], encoded: Stream): ConnectionTune =
  let channelMax = encoded.readBigEndianU16()
  let frameMax = encoded.readBigEndianU32()
  let heartbeat = encoded.readBigEndianU16()
  result = newConnectionTune(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTune, to: Stream) =
  to.writeBigEndian16(self.channelMax)
  to.writeBigEndian32(self.frameMax)
  to.writeBigEndian16(self.heartbeat)

#--------------- Connection.TuneOk ---------------#

proc newConnectionTuneOk*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTuneOk =
  result.new
  result.initMethod(false, 0x000A001F)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

proc decode*(_: type[ConnectionTuneOk], encoded: Stream): ConnectionTuneOk =
  let channelMax = encoded.readBigEndianU16()
  let frameMax = encoded.readBigEndianU32()
  let heartbeat = encoded.readBigEndianU16()
  result = newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTuneOk, to: Stream) =
  to.writeBigEndian16(self.channelMax)
  to.writeBigEndian32(self.frameMax)
  to.writeBigEndian16(self.heartbeat)

#--------------- Connection.Open ---------------#

proc newConnectionOpen*(virtualHost = "/", capabilities = "", insist = false): ConnectionOpen =
  result.new
  result.initMethod(true, 0x000A0028)
  result.virtualHost = virtualHost
  result.capabilities = capabilities
  result.insist = insist

proc decode*(_: type[ConnectionOpen], encoded: Stream): ConnectionOpen =
  let virtualHost = encoded.readShortString()
  let capabilities = encoded.readShortString()
  let bbuf = encoded.readUint8()
  let insist = (bbuf and 0x01) != 0
  result = newConnectionOpen(virtualHost, capabilities, insist)

proc encode*(self: ConnectionOpen, to: Stream) =
  to.writeShortString(self.virtualHost)
  to.writeShortString(self.capabilities)
  let bbuf: uint8 = (if self.insist: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Connection.OpenOk ---------------#

proc newConnectionOpenOk*(knownHosts = ""): ConnectionOpenOk =
  result.new
  result.initMethod(false, 0x000A0029)
  result.knownHosts = knownHosts

proc decode*(_: type[ConnectionOpenOk], encoded: Stream): ConnectionOpenOk =
  let knownHosts = encoded.readShortString()
  result = newConnectionOpenOk(knownHosts)

proc encode*(self: ConnectionOpenOk, to: Stream) =
  to.writeShortString(self.knownHosts)

#--------------- Connection.Close ---------------#

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ConnectionClose =
  result.new
  result.initMethod(true, 0x000A0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

proc decode*(_: type[ConnectionClose], encoded: Stream): ConnectionClose =
  let replyCode = encoded.readBigEndianU16()
  let replyText = encoded.readShortString()
  let classId = encoded.readUint16()
  let methodId = encoded.readUint16()
  result = newConnectionClose(replyCode, replyText, classId, methodId)

proc encode*(self: ConnectionClose, to: Stream) =
  to.writeBigEndian16(self.replyCode)
  to.writeShortString(self.replyText)
  to.writeBigEndian16(self.classId)
  to.writeBigEndian16(self.methodId)

#--------------- Connection.CloseOk ---------------#

proc newConnectionCloseOk*(): ConnectionCloseOk =
  result.new
  result.initMethod(false, 0x000A0033)

proc decode*(_: type[ConnectionCloseOk], encoded: Stream): ConnectionCloseOk = newConnectionCloseOk()

proc encode*(self: ConnectionCloseOk, to: Stream) = discard

#--------------- Connection.Blocked ---------------#

proc newConnectionBlocked*(reason = ""): ConnectionBlocked =
  result.new
  result.initMethod(false, 0x000A003C)
  result.reason = reason

proc decode*(_: type[ConnectionBlocked], encoded: Stream): ConnectionBlocked =
  let reason = encoded.readShortString()
  result = newConnectionBlocked(reason)

proc encode*(self: ConnectionBlocked, to: Stream) =
  to.writeShortString(self.reason)

#--------------- Connection.Unblocked ---------------#

proc newConnectionUnblocked*(): ConnectionUnblocked =
  result.new
  result.initMethod(false, 0x000A003D)

proc decode*(_: type[ConnectionUnblocked], encoded: Stream): ConnectionUnblocked = newConnectionUnblocked()

proc encode*(self: ConnectionUnblocked, to: Stream) = discard
