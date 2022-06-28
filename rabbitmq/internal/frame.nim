import std/[asyncdispatch]
import pkg/networkutils/buffered_socket
import ./spec
import ./exceptions
import ./methods/all
import ./properties

const
  FRAME_METHOD = 1.uint8
  FRAME_HEADER = 2.uint8
  FRAME_BODY = 3.uint8
  FRAME_HEARTBEAT = 8.uint8
  FRAME_END = 206.uint8

type
  FrameType* = enum
    ftProtocolHeader = 0
    ftMethod = FRAME_METHOD
    ftHeader = FRAME_HEADER
    ftBody = FRAME_BODY
    ftHeartBeat = FRAME_HEARTBEAT

  Frame* = ref FrameObj
  FrameObj* = object
    channelNum*: uint16
    size*: uint32
    case frameType*: FrameType
    of ftMethod:
      meth*: AMQPMethod
    of ftHeader:
      bodySize: uint64
      props: Properties
    of ftBody:
      fragment: string
    of ftHeartBeat:
      discard
    of ftProtocolHeader:
      major: uint8
      minor: uint8
      revision: uint8

proc newMethodFrame*(channelNum: uint16, meth: AMQPMethod): Frame =
  result = Frame(frameType: ftMethod, channelNum: channelNum, meth: meth)
  result.size = meth.len.uint32

proc newHeaderFrame*(channelNum: uint16, bodySize: uint64, props: Properties): Frame =
  result.new
  result.frameType = ftHeader
  result.channelNum = channelNum
  result.bodySize = bodySize
  result.props = props

proc newBodyFrame*(channelNum: uint16, fragment: string): Frame =
  result.new
  result.frameType = ftBody
  result.channelNum = channelNum
  result.fragment = fragment

proc newHeartBeatFrame*(channelNum: uint16): Frame =
  result.new
  result.frameType = ftHeartBeat
  result.channelNum = channelNum

proc newProtocolHeaderFrame*(major = PROTOCOL_VERSION[0], minor = PROTOCOL_VERSION[1], revision = PROTOCOL_VERSION[2] ): Frame =
  result = Frame(frameType: ftProtocolHeader, channelNum: 0, major: major, minor: minor, revision: revision)

proc decodeFrame*(src: AsyncBufferedSocket, startFrame: bool = false): Future[Frame] {.async.} =
  if startFrame:
    let header {.used.} = await src.readString(5)
    let major = await src.readU8()
    let minor = await src.readU8()
    let revision = await src.readU8()
    return newProtocolHeaderFrame(major, minor, revision)
  let fType = await src.readU8()
  let chNum = await src.readBEU16()
  let fSize = await src.readBEU32()
  echo "Got Frame: ", fType, " ", chNum," ", fSize
  case fType
  of FRAME_METHOD:
    let meth: AMQPMethod = await src.decodeMethod()
    result = newMethodFrame(chNum, meth)
  of FRAME_HEADER:
    let clsId = await src.readBEU16()
    let skp {.used.} = await src.readBEU16()
    let bodySize = await src.readBEU64()
    let props = await src.decodeProperties(clsId)
    result = newHeaderFrame(chNum, bodySize, props)
  of FRAME_BODY:
    let str = await src.readString(fSize.int)
    result = newBodyFrame(chNum, str)
  of FRAME_HEARTBEAT:
    result = newHeartBeatFrame(chNum)
  else:
    raise newException(InvalidFieldTypeException, "No such field type")
  let fEnd = await src.readU8()
  if fEnd != FRAME_END:
    raise newException(FrameUnmarshalingException, "Last byte error")

proc encodeFrame*(dest: AsyncBufferedSocket, f: Frame) {.async.} =
  if f.frameType == ftProtocolHeader:
    await dest.writeString("AMQP")
    await dest.write(0.uint8)
    await dest.write(f.major)
    await dest.write(f.minor)
    await dest.write(f.revision)
  else:
    await dest.write(f.frameType.uint8)
    await dest.writeBE(f.channelNum)
    await dest.writeBE(f.size)
    case f.frameType
    of ftHeader:
      await dest.writeBE(f.props.kind.uint16)
      await dest.writeBE(0.uint16)
      await dest.writeBE(f.bodySize)
    of ftMethod:
      await dest.encodeMethod(f.meth)
    of ftBody:
      await dest.writeString(f.fragment)
    else:
      discard
    await dest.write(FRAME_END)
  #dest.showBuffer
  await dest.flush()