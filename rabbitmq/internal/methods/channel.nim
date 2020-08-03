import streams
import ./mthd
import ../data

type ChannelOpen* = ref object of Method
  outOfBand: string

proc newChannelOpen*(outOfBand = ""): ChannelOpen =
  result.new
  result.initMethod(true, 0x0014000A)
  result.outOfBand = outOfBand

method decode*(self: ChannelOpen, encoded: Stream): ChannelOpen =
  self.outOfBand = encoded.readShortString()

method encode*(self: ChannelOpen): string =
  var s = newStringStream("")
  s.writeShortString(self.outOfBand)
  result = s.readAll()
  s.close()

type ChannelOpenOk* = ref object of Method
  channelId: string

proc newChannelOpenOk*(channelId = ""): ChannelOpenOk =
  result.new
  result.initMethod(false, 0x0014000B)
  result.channelId = channelId

method decode*(self: ChannelOpenOk, encoded: Stream): ChannelOpenOk =
  self.channelId = encoded.readString()

method encode*(self: ChannelOpenOk): string =
  var s = newStringStream("")
  s.writeString(self.channelId)
  result = s.readAll()
  s.close()

type ChannelFlow* = ref object of Method
  active: bool

proc newChannelFlow*(active = false): ChannelFlow =
  result.new
  result.initMethod(true, 0x00140014)
  result.active = active

method decode*(self: ChannelFlow, encoded: Stream): ChannelFlow =
  let bbuf = encoded.readUint8()
  self.active = (bbuf and 0x01) != 0
  return self

method encode*(self: ChannelFlow): string =
  var s = newStringStream("")
  let bbuf: uint8 = (if self.active: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type ChannelFlowOk* = ref object of ChannelFlow

proc newChannelFlowOk*(active = false): ChannelFlowOk =
  result.new
  result.initMethod(false, 0x00140015)

method decode*(self: ChannelFlowOk, encoded: Stream): ChannelFlowOk =
  let bbuf = encoded.readUint8()
  self.active = (bbuf and 0x01) != 0
  return self

method encode*(self: ChannelFlowOk): string =
  var s = newStringStream("")
  let bbuf: uint8 = (if self.active: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type ChannelClose* = ref object of Method
  replyCode: uint16
  replyText: string
  classId: uint16
  methodId: uint16

proc newConnectionClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ChannelClose =
  result.new
  result.initMethod(true, 0x00140028)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

method decode*(self: ChannelClose, encoded: Stream): ChannelClose =
  self.replyCode = encoded.readBigEndianU16()
  self.replyText = encoded.readShortString()
  self.classId = encoded.readUint16()
  self.methodId = encoded.readUint16()
  return self

method encode*(self: ChannelClose): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.replyCode)
  s.writeShortString(self.replyText)
  s.writeBigEndian16(self.classId)
  s.writeBigEndian16(self.methodId)
  result = s.readAll()
  s.close()

type ChannelCloseOk* = ref object of Method

proc newChannelCloseOk*(): ChannelCloseOk =
  result.new
  result.initMethod(false, 0x00140029)
