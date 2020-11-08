import asyncdispatch
import faststreams/[inputs, outputs]
import ./spec
import ./data
import ./exceptions
import ./methods
import ./properties

type
  FrameType* = enum
    ftMethod
    ftHeader
    ftBody
    ftHeartBeat
    ftProtocolHeader

  Frame* = ref FrameObj
  FrameObj* = object
    channelNum: uint16
    case frameType: FrameType
    of ftMethod:
      meth*: Method
    of ftHeader:
      bodySize: uint64
      props: Properties
    of ftBody:
      fragment: seq[byte]
    of ftHeartBeat:
      discard
    of ftProtocolHeader:
      major: uint8
      minor: uint8
      revision: uint8

proc newMethod*(channelNum: uint16, meth: Method): Frame =
  result.new
  result.frameType = ftMethod
  result.channelNum = channelNum
  result.meth = meth

proc newHeader*(channelNum: uint16, bodySize: uint64, props: Properties): Frame =
  result.new
  result.frameType = ftHeader
  result.channelNum = channelNum
  result.bodySize = bodySize
  result.props = props

proc newBody*(channelNum: uint16, fragment: seq[byte]): Frame =
  result.new
  result.frameType = ftBody
  result.channelNum = channelNum
  result.fragment = fragment

proc newHeartBeat*(channelNum: uint16): Frame =
  result.new
  result.frameType = ftHeartBeat
  result.channelNum = channelNum

proc newProtocolHeader*(major = PROTOCOL_VERSION[0], minor = PROTOCOL_VERSION[1], revision = PROTOCOL_VERSION[2] ): Frame =
  result.new
  result.frameType = ftProtocolHeader
  result.channelNum = 0
  result.major = major
  result.minor = minor
  result.revision = revision

proc getFrameHeaderInfo*(encoded: InputStream): (uint8, uint16, uint32) =
  let s = encoded
  let (_, fType) = s.readBigEndianU8()
  let (_, chNum) = s.readBigEndianU16()
  let (_, fSize) = s.readBigEndianU32()
  return (fType, chNum, fSize)

proc decodeFrame*(encoded: InputStream, startFrame: bool = false): Frame =
  let s = encoded
  if startFrame:
    encoded.advance(5)
    let (_, major) = s.readBigEndianU8()
    let (_, minor) = s.readBigEndianU8()
    let (_, revision) = s.readBigEndianU8()
    return newProtocolHeader(major, minor, revision)
  let(fType, chNum, fSize) = getFrameHeaderInfo(encoded)
  if encoded.len().isSome() and encoded.len().get().uint32 < fSize+1:
    raise newException(FrameUnmarshalingException, "Not all data received")
  case fType
  of FRAME_METHOD:
    let meth: Method = s.decodeMethod()
    result = newMethod(chNum, meth)
  of FRAME_HEADER:
    let (_, clsId) = s.readBigEndianU16()
    discard s.readBigEndianU16()
    let (_, bodySize) = s.readBigEndianU64()
    let props: BasicProperties = decodeProperties(clsId, s)
    result = newHeader(chNum, bodySize, props)
  of FRAME_BODY:
    let body = s.read(fSize.int)
    result = newBody(chNum, body)
  of FRAME_HEARTBEAT:
    result = newHeartBeat(chNum)
  else:
    raise newException(InvalidFieldTypeException, "No such field type")
  let fEnd = s.readBigEndianU8()
  if fEnd != FRAME_END:
    raise newException(FrameUnmarshallingException, "Last byte error")

proc encodeFrame*(f: Frame, to: AsyncOutputStream) {.async.} =
  case f.frameType
  of ftProtocolHeader:
    discard await to.write("AMQP")
    discard await to.writeBigEndian8(0.uint8)
    discard await to.writeBigEndian8(f.major)
    discard await to.writeBigEndian8(f.minor)
    discard await to.writeBigEndian8(f.revision)
  of ftHeader:
    discard await to.writeBigEndian16(BASIC_FRAME_ID)
    discard await to.writeBigEndian16(0.uint16)
    discard await to.writeBigEndian64(f.bodySize)
  of ftMethod:
    discard await f.meth.encodeMethod(to)
  of ftBody:
    discard await to.write(f.fragment)
    