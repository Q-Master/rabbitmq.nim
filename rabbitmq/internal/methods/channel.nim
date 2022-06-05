import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import ../field
import ../exceptions

const CHANNEL_METHODS* = 0x0014.uint16
const CHANNEL_OPEN_METHOD_ID* = 0x0014000A.uint32
const CHANNEL_OPEN_OK_METHOD_ID* = 0x0014000B.uint32
const CHANNEL_FLOW_METHOD_ID* = 0x00140014.uint32
const CHANNEL_FLOW_OK_METHOD_ID* = 0x00140015.uint32
const CHANNEL_CLOSE_METHOD_ID* = 0x00140028.uint32
const CHANNEL_CLOSE_OK_METHOD_ID* = 0x00140029.uint32


type
  AMQPChannelKind = enum
    AMQP_CHANNEL_NONE = 0
    AMQP_CHANNEL_OPEN_SUBMETHOD = (CHANNEL_OPEN_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CHANNEL_OPEN_OK_SUBMETHOD = (CHANNEL_OPEN_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CHANNEL_FLOW_SUBMETHOD = (CHANNEL_FLOW_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CHANNEL_FLOW_OK_SUBMETHOD = (CHANNEL_FLOW_OK_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CHANNEL_CLOSE_SUBMETHOD = (CHANNEL_CLOSE_METHOD_ID and 0x0000FFFF).uint16
    AMQP_CHANNEL_CLOSE_OK_SUBMETHOD = (CHANNEL_CLOSE_OK_METHOD_ID and 0x0000FFFF).uint16

  AMQPChannelFlowBits* = object
    active* {.bitsize: 1.}: bool
    unused {.bitsize: 7.}: uint8

  AMQPChannel* = ref AMQPChannelObj
  AMQPChannelObj* = object of RootObj
    case kind*: AMQPChannelKind
    of AMQP_CHANNEL_OPEN_SUBMETHOD:
      outOfBand*: string
    of AMQP_CHANNEL_OPEN_OK_SUBMETHOD:
      channelId*: string
    of AMQP_CHANNEL_FLOW_SUBMETHOD, AMQP_CHANNEL_FLOW_OK_SUBMETHOD:
      flowFlags*: AMQPChannelFlowBits
    of AMQP_CHANNEL_CLOSE_SUBMETHOD:
      replyCode*: uint16
      replyText*: string
      classId*: uint16
      methodId*: uint16
    of AMQP_CHANNEL_CLOSE_OK_SUBMETHOD:
      discard
    else:
      discard

proc len*(meth: AMQPChannel): int =
  result = 0
  case meth.kind:
  of AMQP_CHANNEL_OPEN_SUBMETHOD:
    result.inc(meth.outOfBand.len + sizeInt8Uint8)
  of AMQP_CHANNEL_OPEN_OK_SUBMETHOD:
    result.inc(meth.channelId.len + sizeInt32Uint32)
  of AMQP_CHANNEL_FLOW_SUBMETHOD, AMQP_CHANNEL_FLOW_OK_SUBMETHOD:
    result.inc(sizeInt8Uint8)
  of AMQP_CHANNEL_CLOSE_SUBMETHOD:
    result.inc(sizeInt16Uint16)
    result.inc(meth.replyText.len + sizeInt8Uint8)
    result.inc(sizeInt16Uint16)
    result.inc(sizeInt16Uint16)
  of AMQP_CHANNEL_CLOSE_OK_SUBMETHOD:
    result.inc(0)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc decode*(_: typedesc[AMQPChannel], s: AsyncBufferedSocket, t: uint32): Future[AMQPChannel] {.async.} =
  case t:
  of CHANNEL_OPEN_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_OPEN_SUBMETHOD)
    result.outOfBand = await s.decodeShortString()
  of CHANNEL_OPEN_OK_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_OPEN_OK_SUBMETHOD)
    result.channelId = await s.decodeString()
  of CHANNEL_FLOW_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_FLOW_SUBMETHOD)
    result.flowFlags = cast[AMQPChannelFlowBits](await s.readU8())
  of CHANNEL_FLOW_OK_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_FLOW_OK_SUBMETHOD)
    result.flowFlags = cast[AMQPChannelFlowBits](await s.readU8())
  of CHANNEL_CLOSE_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_CLOSE_SUBMETHOD)
    result.replyCode = await s.readBEU16()
    result.replyText = await s.decodeShortString()
    result.classId = await s.readBEU16()
    result.methodId = await s.readBEU16()
  of CHANNEL_CLOSE_OK_METHOD_ID:
    result = AMQPChannel(kind: AMQP_CHANNEL_CLOSE_OK_SUBMETHOD)
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc encode*(meth: AMQPChannel, dst: AsyncBufferedSocket) {.async.} =
  echo $meth.kind
  case meth.kind:
  of AMQP_CHANNEL_OPEN_SUBMETHOD:
    await dst.encodeShortString(meth.outOfBand)
  of AMQP_CHANNEL_OPEN_OK_SUBMETHOD:
    await dst.encodeString(meth.channelId)
  of AMQP_CHANNEL_FLOW_SUBMETHOD, AMQP_CHANNEL_FLOW_OK_SUBMETHOD:
    await dst.write(cast[uint8](meth.flowFlags))
  of AMQP_CHANNEL_CLOSE_SUBMETHOD:
    await dst.writeBE(meth.replyCode)
    await dst.encodeShortString(meth.replyText)
    await dst.writeBE(meth.classId)
    await dst.writeBE(meth.methodId)
  of AMQP_CHANNEL_CLOSE_OK_SUBMETHOD:
    discard
  else:
    raise newException(InvalidFrameMethodException, "Wrong MethodID")

proc newChannelOpen*(outOfBand: string): AMQPChannel =
  result = AMQPChannel(
    kind: AMQP_CHANNEL_OPEN_SUBMETHOD, 
    outOfBand: outOfBand
  )

proc newChannelOpenOk*(channelId: string): AMQPChannel =
  result = AMQPChannel(
    kind: AMQP_CHANNEL_OPEN_OK_SUBMETHOD, 
    channelId: channelId
  )

proc newChannelFlow*(active: bool): AMQPChannel =
  result = AMQPChannel(
    kind: AMQP_CHANNEL_FLOW_SUBMETHOD
  )
  result.flowFlags.active = active

proc newChannelFlowOk*(active: bool): AMQPChannel =
  result = AMQPChannel(
    kind: AMQP_CHANNEL_FLOW_OK_SUBMETHOD
  )
  result.flowFlags.active = active

proc newChannelClose*(replyCode: uint16, replyText: string, classId: uint16, methodId: uint16): AMQPChannel =
  result = AMQPChannel(
    kind: AMQP_CHANNEL_CLOSE_SUBMETHOD,
    replyCode: replyCode,
    replyText: replyText,
    classId: classId,
    methodId: methodId
  )

proc newChannelCloseOk*(): AMQPChannel =
  result = AMQPChannel(kind: AMQP_CHANNEL_CLOSE_OK_SUBMETHOD)
