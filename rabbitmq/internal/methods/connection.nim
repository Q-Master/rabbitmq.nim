import tables
import ./mthd
import ../spec
import ../data
import ../exceptions
import ../streams

type 
  ConnectionStart* = ref object of Method
    major*: uint8
    minor*: uint8
    properties*: TableRef[string, DataTable]
    mechanisms*: string
    locales*: string
  
  ConnectionStartOk* = ref object of Method
    clientProps*: TableRef[string, DataTable]
    mechanism*: string
    response*: string
    locale*: string
  
  ConnectionSecure* = ref object of Method
    challenge*: string
  
  ConnectionSecureOk* = ref object of Method
    response*: string
  
  ConnectionTune* = ref object of Method
    channelMax*: uint16
    frameMax*: uint32
    heartbeat*: uint16
  
  ConnectionTuneOk* = ref object of Method
    channelMax*: uint16
    frameMax*: uint32
    heartbeat*: uint16
  
  ConnectionOpen* = ref object of Method
    virtualHost*: string
    capabilities*: string
    insist*: bool
  
  ConnectionOpenOk* = ref object of Method
    knownHosts*: string
  
  ConnectionClose* = ref object of Method
    replyCode*: uint16
    replyText*: string
    classId*: uint16
    methodId*: uint16
  
  ConnectionCloseOk* = ref object of Method
  
  ConnectionBlocked* = ref object of Method
    reason*: string
  
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

proc decode*(_: type[ConnectionStart], encoded: InputStream): ConnectionStart =
  let (_, major) = encoded.readBigEndianU8()
  let (_, minor) = encoded.readBigEndianU8()
  let (_, properties) = decodeTable(encoded)
  let (_, mechanisms) = encoded.readString()
  let (_, locales) = encoded.readString()
  result = newConnectionStart(major, minor, properties, mechanisms, locales)

proc encode*(self: ConnectionStart, to: OutputStream) =
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

proc decode*(_: type[ConnectionStartOk], encoded: InputStream): ConnectionStartOk =
  let (_, clientProps) = decodeTable(encoded)
  let (_, mechanism) = encoded.readShortString()
  let (_, response) = encoded.readString()
  let (_, locale) = encoded.readShortString()
  result = newConnectionStartOk(clientProps, mechanism, response, locale)

proc encode*(self: ConnectionStartOk, to: OutputStream) =
  to.encodeTable(self.clientProps)
  to.writeShortString(self.mechanism)
  to.writeString(self.response)
  to.writeShortString(self.locale)

#--------------- Connection.Secure ---------------#

proc newConnectionSecure*(challenge: string = ""): ConnectionSecure =
  result.new
  result.initMethod(true, 0x000A0014)
  result.challenge = challenge

proc decode*(_: type[ConnectionSecure], encoded: InputStream): ConnectionSecure =
  let (_, challenge) = encoded.readString()
  result = newConnectionSecure(challenge)

proc encode*(self: ConnectionSecure, to: OutputStream) =
  to.writeString(self.challenge)

#--------------- Connection.SecureOk ---------------#

proc newConnectionSecureOk*(response: string = ""): ConnectionSecureOk =
  result.new
  result.initMethod(false, 0x000A0015)
  result.response = response

proc decode*(_: type[ConnectionSecureOk], encoded: InputStream): ConnectionSecureOk =
  let (_, response) = encoded.readString()
  result = newConnectionSecureOk(response)

proc encode*(self: ConnectionSecureOk, to: OutputStream) =
  to.writeString(self.response)

#--------------- Connection.Tune ---------------#

proc newConnectionTune*(channelMax: uint16 = 0, frameMax: uint32 = 0, heartbeat: uint16 = 0): ConnectionTune =
  result.new
  result.initMethod(true, 0x000A001E)
  result.channelMax = channelMax
  result.frameMax = frameMax
  result.heartbeat = heartbeat

proc decode*(_: type[ConnectionTune], encoded: InputStream): ConnectionTune =
  let (_, channelMax) = encoded.readBigEndianU16()
  let (_, frameMax) = encoded.readBigEndianU32()
  let (_, heartbeat) = encoded.readBigEndianU16()
  result = newConnectionTune(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTune, to: OutputStream) =
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

proc decode*(_: type[ConnectionTuneOk], encoded: InputStream): ConnectionTuneOk =
  let (_, channelMax) = encoded.readBigEndianU16()
  let (_, frameMax) = encoded.readBigEndianU32()
  let (_, heartbeat) = encoded.readBigEndianU16()
  result = newConnectionTuneOk(channelMax, frameMax, heartbeat)

proc encode*(self: ConnectionTuneOk, to: OutputStream) =
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

proc decode*(_: type[ConnectionOpen], encoded: InputStream): ConnectionOpen =
  let (_, virtualHost) = encoded.readShortString()
  let (_, capabilities) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let insist = (bbuf and 0x01) != 0
  result = newConnectionOpen(virtualHost, capabilities, insist)

proc encode*(self: ConnectionOpen, to: OutputStream) =
  let bbuf: uint8 = (if self.insist: 0x01 else: 0x00)
  to.writeShortString(self.virtualHost)
  to.writeShortString(self.capabilities)
  to.writeBigEndian8(bbuf)

#--------------- Connection.OpenOk ---------------#

proc newConnectionOpenOk*(knownHosts = ""): ConnectionOpenOk =
  result.new
  result.initMethod(false, 0x000A0029)
  result.knownHosts = knownHosts

proc decode*(_: type[ConnectionOpenOk], encoded: InputStream): ConnectionOpenOk =
  let (_, knownHosts) = encoded.readShortString()
  result = newConnectionOpenOk(knownHosts)

proc encode*(self: ConnectionOpenOk, to: OutputStream) =
  to.writeShortString(self.knownHosts)

#--------------- Connection.Close ---------------#

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ConnectionClose =
  result.new
  result.initMethod(true, 0x000A0032)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

proc decode*(_: type[ConnectionClose], encoded: InputStream): ConnectionClose =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, classId) = encoded.readBigEndianU16()
  let (_, methodId) = encoded.readBigEndianU16()
  result = newConnectionClose(replyCode, replyText, classId, methodId)

proc encode*(self: ConnectionClose, to: OutputStream) =
  to.writeBigEndian16(self.replyCode)
  to.writeShortString(self.replyText)
  to.writeBigEndian16(self.classId)
  to.writeBigEndian16(self.methodId)

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

proc decode*(_: type[ConnectionCloseOk], encoded: InputStream): ConnectionCloseOk = newConnectionCloseOk()

proc encode*(self: ConnectionCloseOk, to: OutputStream) = discard

#--------------- Connection.Blocked ---------------#

proc newConnectionBlocked*(reason = ""): ConnectionBlocked =
  result.new
  result.initMethod(false, 0x000A003C)
  result.reason = reason

proc decode*(_: type[ConnectionBlocked], encoded: InputStream): ConnectionBlocked =
  let (_, reason) = encoded.readShortString()
  result = newConnectionBlocked(reason)

proc encode*(self: ConnectionBlocked, to: OutputStream) =
  to.writeShortString(self.reason)

#--------------- Connection.Unblocked ---------------#

proc newConnectionUnblocked*(): ConnectionUnblocked =
  result.new
  result.initMethod(false, 0x000A003D)

proc decode*(_: type[ConnectionUnblocked], encoded: InputStream): ConnectionUnblocked = newConnectionUnblocked()

proc encode*(self: ConnectionUnblocked, to: OutputStream) = discard
