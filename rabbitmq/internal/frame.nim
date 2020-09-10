import streams
import strutils
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
      fragment: string
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

proc newBody*(channelNum: uint16, fragment: string): Frame =
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

proc decodeFrame*(encoded: var string): Frame =
  let s = newStringStream(encoded)
  if encoded[0..4] == "AMQP":
    if encoded.len < 7:
      s.close()
      return nil
    s.setPosition(5)
    let major = s.readBigEndianU8()
    let minor = s.readBigEndianU8()
    let revision = s.readBigEndianU8()
    s.close()
    encoded.delete(0, 7)
    return newProtocolHeader(major, minor, revision)
  var fType: uint8
  var chNum: uint16
  var fSize: uint32
  try:
    fType = s.readBigEndianU8()
    chNum = s.readBigEndianU16()
    fSize = s.readBigEndianU32()
  except IOError:
    s.close()
    return nil
  let fEnd = FRAME_HEADER_SIZE + fSize.int + FRAME_END_SIZE
  if fEnd > encoded.len():
    s.close()
    return nil
  case fType
  of FRAME_METHOD:
    let meth = dispatchMethods(s)
    s.close()
    encoded.delete(0, fEnd)
    return newMethod(chNum, meth)
  of FRAME_HEADER:
    let clsId = s.readBigEndianU16()
    discard s.readBigEndianU16()
    let bodySize = s.readBigEndianU64()
    let props = dispatchProperties(clsId, s)
    s.close()
    encoded.delete(0, fEnd)
    return newHeader(chNum, bodySize, props)
  of FRAME_BODY:
    let body = s.readStr(fSize.int)
    s.close()
    encoded.delete(0, fEnd)
    return newBody(chNum, body)
  of FRAME_HEARTBEAT:
    s.close()
    encoded.delete(0, fEnd)
    return newHeartBeat(chNum)
  else:
    s.close()
    encoded.delete(0, fEnd)
    raise newException(InvalidFieldTypeException, "No such field type")
