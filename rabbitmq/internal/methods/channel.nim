import ./submethods
import ../data
import ../streams

const CHANNEL_METHODS* = 0x0014.uint16
const CHANNEL_OPEN_METHOD_ID = 0x0014000A.uint32
const CHANNEL_OPEN_OK_METHOD_ID = 0x0014000B.uint32
const CHANNEL_FLOW_METHOD_ID = 0x00140014.uint32
const CHANNEL_FLOW_OK_METHOD_ID = 0x00140015.uint32
const CHANNEL_CLOSE_METHOD_ID = 0x00140028.uint32
const CHANNEL_CLOSE_OK_METHOD_ID = 0x00140029.uint32

type
  ChannelVariants* = enum
    NONE = 0
    CHANNEL_OPEN_METHOD = (CHANNEL_OPEN_METHOD_ID and 0x0000FFFF).uint16
    CHANNEL_OPEN_OK_METHOD = (CHANNEL_OPEN_OK_METHOD_ID and 0x0000FFFF).uint16
    CHANNEL_FLOW_METHOD = (CHANNEL_FLOW_METHOD_ID and 0x0000FFFF).uint16
    CHANNEL_FLOW_OK_METHOD = (CHANNEL_FLOW_OK_METHOD_ID and 0x0000FFFF).uint16
    CHANNEL_CLOSE_METHOD = (CHANNEL_CLOSE_METHOD_ID and 0x0000FFFF).uint16
    CHANNEL_CLOSE_OK_METHOD = (CHANNEL_CLOSE_OK_METHOD_ID and 0x0000FFFF).uint16


type 
  ChannelMethod* = ref object of SubMethod
    case indexLo*: ChannelVariants
    of CHANNEL_OPEN_METHOD:
      outOfBand*: string
    of CHANNEL_OPEN_OK_METHOD:
      channelId*: string
    of CHANNEL_FLOW_METHOD, CHANNEL_FLOW_OK_METHOD:
      active*: bool
    of CHANNEL_CLOSE_METHOD:
      replyCode*: uint16
      replyText*: string
      classId*: uint16
      methodId*: uint16
    of CHANNEL_CLOSE_OK_METHOD:
      discard
    else:
      discard

proc decodeChannelOpen(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelOpen(to: OutputStream, data: ChannelMethod)
proc decodeChannelOpenOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelOpenOk(to: OutputStream, data: ChannelMethod)
proc decodeChannelFlow(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelFlow(to: OutputStream, data: ChannelMethod)
proc decodeChannelFlowOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelFlowOk(to: OutputStream, data: ChannelMethod)
proc decodeChannelClose(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelClose(to: OutputStream, data: ChannelMethod)
proc decodeChannelCloseOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod)
proc encodeChannelCloseOk(to: OutputStream, data: ChannelMethod)

proc decode*(_: type[ChannelMethod], submethodId: ChannelVariants, encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  case submethodId
  of CHANNEL_OPEN_METHOD:
    result = decodeChannelOpen(encoded)
  of CHANNEL_OPEN_OK_METHOD:
    result = decodeChannelOpenOk(encoded)
  of CHANNEL_FLOW_METHOD:
    result = decodeChannelFlow(encoded)
  of CHANNEL_FLOW_OK_METHOD:
    result = decodeChannelFlowOk(encoded)
  of CHANNEL_CLOSE_METHOD:
    result = decodeChannelClose(encoded)
  of CHANNEL_CLOSE_OK_METHOD:
    result = decodeChannelCloseOk(encoded)
  else:
    discard

proc encode*(to: OutputStream, data: ChannelMethod) =
  case data.indexLo
  of CHANNEL_OPEN_METHOD:
    to.encodeChannelOpen(data)
  of CHANNEL_OPEN_OK_METHOD:
    to.encodeChannelOpenOk(data)
  of CHANNEL_FLOW_METHOD:
    to.encodeChannelFlow(data)
  of CHANNEL_FLOW_OK_METHOD:
    to.encodeChannelFlowOk(data)
  of CHANNEL_CLOSE_METHOD:
    to.encodeChannelClose(data)
  of CHANNEL_CLOSE_OK_METHOD:
    to.encodeChannelCloseOk(data)
  else:
    discard

#--------------- Channel.Open ---------------#

proc newChannelOpen*(outOfBand = ""): (bool, seq[uint16], ChannelMethod) =
  var res = ChannelMethod(indexLo: CHANNEL_OPEN_METHOD)
  res.outOfBand = outOfBand
  result = (true, @[ord(CHANNEL_OPEN_OK_METHOD).uint16], res)

proc decodeChannelOpen(encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  let (_, outOfBand) = encoded.readShortString()
  result = newChannelOpen(outOfBand)

proc encodeChannelOpen(to: OutputStream, data: ChannelMethod) =
  to.writeShortString(data.outOfBand)

#--------------- Channel.OpenOk ---------------#

proc newChannelOpenOk*(channelId = ""): (bool, seq[uint16], ChannelMethod) =
  var res = ChannelMethod(indexLo: CHANNEL_OPEN_OK_METHOD)
  res.channelId = channelId
  result = (false, @[], res)

proc decodeChannelOpenOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  let (_, channelId) = encoded.readString()
  result = newChannelOpenOk(channelId)

proc encodeChannelOpenOk(to: OutputStream, data: ChannelMethod) =
  to.writeString(data.channelId)

#--------------- Channel.Flow ---------------#

proc newChannelFlow*(active = false): (bool, seq[uint16], ChannelMethod) =
  var res = ChannelMethod(indexLo: CHANNEL_FLOW_METHOD)
  res.active = active
  result = (true, @[ord(CHANNEL_FLOW_OK_METHOD).uint16], res)

proc decodeChannelFlow(encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  let (_, bbuf) = encoded.readBigEndianU8()
  let active = (bbuf and 0x01) != 0
  result = newChannelFlow(active)

proc encodeChannelFlow(to: OutputStream, data: ChannelMethod) =
  let bbuf: uint8 = (if data.active: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Channel.FlowOk ---------------#

proc newChannelFlowOk*(active = false): (bool, seq[uint16], ChannelMethod) =
  result = (false, @[], ChannelMethod(indexLo: CHANNEL_FLOW_OK_METHOD))

proc decodeChannelFlowOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  let (_, bbuf) = encoded.readBigEndianU8()
  let active = (bbuf and 0x01) != 0
  result = newChannelFlowOk(active)

proc encodeChannelFlowOk(to: OutputStream, data: ChannelMethod) =
  let bbuf: uint8 = (if data.active: 0x01 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Channel.Close ---------------#

proc newChannelClose*(replyCode: uint16 = 0, replyText = "", classId: uint16 = 0, methodId: uint16 = 0): (bool, seq[uint16], ChannelMethod) =
  var res = ChannelMethod(indexLo: CHANNEL_CLOSE_METHOD)
  res.replyCode = replyCode
  res.replyText = replyText
  res.classId = classId
  res.methodId = methodId
  result = (true, @[ord(CHANNEL_CLOSE_OK_METHOD).uint16], res)

proc decodeChannelClose(encoded: InputStream): (bool, seq[uint16], ChannelMethod) =
  let (_, replyCode) = encoded.readBigEndianU16()
  let (_, replyText) = encoded.readShortString()
  let (_, classId) = encoded.readBigEndianU16()
  let (_, methodId) = encoded.readBigEndianU16()
  result = newChannelClose(replyCode, replyText, classId, methodId)

proc encodeChannelClose(to: OutputStream, data: ChannelMethod) =
  to.writeBigEndian16(data.replyCode)
  to.writeShortString(data.replyText)
  to.writeBigEndian16(data.classId)
  to.writeBigEndian16(data.methodId)

#--------------- Channel.CloseOk ---------------#

proc newChannelCloseOk*(): (bool, seq[uint16], ChannelMethod) =
  result = (false, @[], ChannelMethod(indexLo: CHANNEL_CLOSE_OK_METHOD))

proc decodeChannelCloseOk(encoded: InputStream): (bool, seq[uint16], ChannelMethod) = newChannelCloseOk()

proc encodeChannelCloseOk(to: OutputStream, data: ChannelMethod) = discard
