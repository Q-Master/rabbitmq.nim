import ./mthd
import ../data
import ../streams

type 
  ChannelOpen* = ref object of Method
    outOfBand: string
  ChannelOpenOk* = ref object of Method
    channelId: string
  ChannelFlow* = ref object of Method
    active: bool
  ChannelFlowOk* = ref object of Method
    active: bool
  ChannelClose* = ref object of Method
    replyCode: uint16
    replyText: string
    classId: uint16
    methodId: uint16
  ChannelCloseOk* = ref object of Method

#--------------- Channel.Open ---------------#

proc newChannelOpen*(outOfBand = ""): ChannelOpen =
  result.new
  result.initMethod(true, 0x0014000A)
  result.outOfBand = outOfBand

proc decode*(_: type[ChannelOpen], encoded: InputStream): ChannelOpen =
  let (_, outOfBand) = encoded.readShortString()
  result = newChannelOpen(outOfBand)

proc encode*(self: ChannelOpen, to: OutputStream) =
  to.writeShortString(self.outOfBand)

#--------------- Channel.OpenOk ---------------#

proc newChannelOpenOk*(channelId = ""): ChannelOpenOk =
  result.new
  result.initMethod(false, 0x0014000B)
  result.channelId = channelId

proc decode*(_: type[ChannelOpenOk], encoded: InputStream): ChannelOpenOk =
  let (_, channelId) = encoded.readString()
  result = newChannelOpenOk(channelId)

proc encode*(self: ChannelOpenOk, to: OutputStream) =
  to.writeString(self.channelId)

#--------------- Channel.Flow ---------------#

proc newChannelFlow*(active = false): ChannelFlow =
  result.new
  result.initMethod(true, 0x00140014)
  result.active = active

proc decode*(_: type[ChannelFlow], encoded: InputStream): ChannelFlow =
  let (_, bbuf) = encoded.readBigEndianU8()
  let active = (bbuf and 0x01) != 0
  result = newChannelFlow(active)

proc encode*(self: ChannelFlow, to: OutputStream) =
  let bbuf: uint8 = (if self.active: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Channel.FlowOk ---------------#

proc newChannelFlowOk*(active = false): ChannelFlowOk =
  result.new
  result.initMethod(false, 0x00140015)

proc decode*(_: type[ChannelFlowOk], encoded: InputStream): ChannelFlowOk =
  let (_, bbuf) = encoded.readBigEndianU8()
  let active = (bbuf and 0x01) != 0
  result = newChannelFlowOk(active)

proc encode*(self: ChannelFlowOk, to: OutputStream) =
  let bbuf: uint8 = (if self.active: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Channel.Close ---------------#

proc newChannelClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): ChannelClose =
  result.new
  result.initMethod(true, 0x00140028)
  result.replyCode = replyCode
  result.replyText = replyText
  result.classId = classId
  result.methodId = methodId

proc decode*(_: type[ChannelClose], encoded: InputStream): ChannelClose =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, classId) = encoded.readBigEndianU16()
  let (_, methodId) = encoded.readBigEndianU16()
  result = newChannelClose(replyCode, replyText, classId, methodId)

proc encode*(self: ChannelClose, to: OutputStream) =
  to.writeBigEndian16(self.replyCode)
  to.writeShortString(self.replyText)
  to.writeBigEndian16(self.classId)
  to.writeBigEndian16(self.methodId)

#--------------- Channel.CloseOk ---------------#

proc newChannelCloseOk*(): ChannelCloseOk =
  result.new
  result.initMethod(false, 0x00140029)

proc decode*(_: type[ChannelCloseOk], encoded: InputStream): ChannelCloseOk = newChannelCloseOk()

proc encode*(self: ChannelCloseOk, to: OutputStream) = discard
