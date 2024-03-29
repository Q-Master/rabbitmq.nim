import std/[asyncdispatch, strutils]
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
      bodySize*: uint64
      props*: Properties
    of ftBody:
      fragment*: seq[byte]
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
  result = Frame(
    frameType: ftHeader,
    channelNum: channelNum,
    bodySize: bodySize,
    props: props,
    size: (sizeInt16Uint16+sizeInt16Uint16+sizeInt64Uint64+props.len()).uint32
  )

proc newBodyFrame*(channelNum: uint16, fragment: seq[byte]): Frame =
  result = Frame(
    frameType: ftBody,
    channelNum: channelNum,
    fragment: fragment,
    size: fragment.len().uint32
  )

proc newBodyFrame*(channelNum: uint16, size: int): Frame =
  result = Frame(
    frameType: ftBody,
    channelNum: channelNum,
    size: size.uint32
  )
  result.fragment.setLen(size)

proc newHeartBeatFrame*(channelNum: uint16): Frame =
  result = Frame(
    frameType: ftHeartBeat,
    channelNum: channelNum
  )

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
  #echo "Got Frame: ", fType, " ", chNum," ", fSize
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
    result = newBodyFrame(chNum, fSize.int)
    await src.recvInto(result.fragment[0].addr, fSize.int)
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
      await dest.encodeProperties(f.props)
    of ftMethod:
      await dest.encodeMethod(f.meth)
      #echo "METHOD: ", f.meth.kind, ", ", f.meth.methodId.toHex
      #echo "SIZE: ", f.meth.len()
    of ftBody:
      let size {.used.} = await dest.send(f.fragment)
    else:
      discard
    await dest.write(FRAME_END)
  #dest.showBuffer
  await dest.flush()