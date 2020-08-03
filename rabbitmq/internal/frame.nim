import ./spec

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
      bodySize: uint32
      props: Properties
    of ftBody:
      fragment: string
    of ftHeartBeat:
      discard
    of ftProtocolHeader:
      major: char
      minor: char
      revision: char

proc newMethod*(channelNum: uint16, meth: Method): Frame =
  result.new
  result.frameType = ftMethod
  result.channelNum = channelNum
  result.meth = meth

proc newHeader*(channelNum: uint16, bodySize: uint32, props: Properties): Frame =
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

proc newProtocolHeader*(major: char = PROTOCOL_VERSION[0], minor: char = PROTOCOL_VERSION[1], revision: char = PROTOCOL_VERSION[2] ): Frame =
  result.new
  result.frameType = ftProtocolHeader
  result.channelNum = 0
  result.major = major
  result.minor = minor
  result.revision = revision

