import std/[times, tables, asyncdispatch]
import pkg/networkutils/buffered_socket
import ./field
import ./exceptions

const CONNECTION_PROPERTIES_ID* = 0x000A.uint16
const CHANNEL_PROPERTIES_ID* = 0x0014.uint16
const ACCESS_PROPERTIES_ID* = 0x001E.uint16
const EXCHANGE_PROPERTIES_ID* = 0x0028.uint16
const QUEUE_PROPERTIES_ID* = 0x0032.uint16
const BASIC_PROPERTIES_ID* = 0x003C.uint16
const CONFIRM_PROPERTIES_ID* = 0x0055.uint16
const TX_PROPERTIES_ID* = 0x005A.uint16
const ALL_PROPERTIES_IDS* = {
  CONNECTION_PROPERTIES_ID, CHANNEL_PROPERTIES_ID, ACCESS_PROPERTIES_ID, 
  EXCHANGE_PROPERTIES_ID, QUEUE_PROPERTIES_ID, BASIC_PROPERTIES_ID,
  CONFIRM_PROPERTIES_ID, TX_PROPERTIES_ID
  }

type
  PropertiesKind* = enum
    PROPERTIES_NONE = 0x0000.uint16
    CONNECTION_PROPERTIES = CONNECTION_PROPERTIES_ID
    CHANNEL_PROPERTIES = CHANNEL_PROPERTIES_ID
    ACCESS_PROPERTIES = ACCESS_PROPERTIES_ID
    EXCHANGE_PROPERTIES = EXCHANGE_PROPERTIES_ID
    QUEUE_PROPERTIES = QUEUE_PROPERTIES_ID
    BASIC_PROPERTIES = BASIC_PROPERTIES_ID
    CONFIRM_PROPERTIES = CONFIRM_PROPERTIES_ID
    TX_PROPERTIES = TX_PROPERTIES_ID

  BasicPropertiesFlags* = object
    unused_0_1 {.bitsize: 2.}: uint8
    clusterId* {.bitsize: 1.}: bool
    appId* {.bitsize: 1.}: bool
    userId* {.bitsize: 1.}: bool
    pType* {.bitsize: 1.}: bool
    timestamp* {.bitsize: 1.}: bool
    messageId* {.bitsize: 1.}: bool
    expiration* {.bitsize: 1.}: bool
    replyTo* {.bitsize: 1.}: bool
    correlationId* {.bitsize: 1.}: bool
    priority* {.bitsize: 1.}: bool
    deliveryMode* {.bitsize: 1.}: bool
    headers* {.bitsize: 1.}: bool
    contentEncoding* {.bitsize: 1.}: bool
    contentType* {.bitsize: 1.}: bool
    unused_16_31 {.bitsize: 16.}: uint16


  Properties* = ref PropertiesObj
  PropertiesObj* = object of RootObj
    flags*: uint32
    case kind*: PropertiesKind
    of BASIC_PROPERTIES:
      basicFlags*: BasicPropertiesFlags
      contentType*: ref string
      contentEncoding*: ref string
      headers*: FieldTable
      deliveryMode*: uint8
      priority*: uint8
      correlationId*: ref string
      replyTo*: ref string
      expiration*: ref string
      messageId*: ref string
      timestamp*: ref Time
      pType*: ref string
      userId*: ref string
      appId*: ref string
      clusterId*: ref string
    else:
      discard

proc decodeProperties*(src: AsyncBufferedSocket, id: uint16): Future[Properties] {.async.} =
  if id notin ALL_PROPERTIES_IDS:
    raise newException(AMQPFrameError, "Properties id(" & $id & ") is not correct")
  var flags: uint32 = 0
  var fidx = 0
  while true:
    let pflags = await src.readBEU16()
    flags = flags or (pflags.uint32 shl (fidx*16))
    if (pflags and 0x0001) == 0:
      break
    fidx += 1
  result = Properties(kind: PropertiesKind(id))
  result.flags = flags
  case id:
  of BASIC_PROPERTIES_ID:
    result.basicFlags = cast[BasicPropertiesFlags](flags)
    if result.basicFlags.contentType:
      result.contentType[] = await src.decodeShortString()
    if result.basicFlags.contentEncoding:
      result.contentEncoding[] = await src.decodeShortString()
    if result.basicFlags.headers:
      result.headers = await src.decodeTable()
    if result.basicFlags.deliveryMode:
      result.deliveryMode = await src.readU8()
    if result.basicFlags.priority:
      result.priority = await src.readU8()
    if result.basicFlags.correlationId:
      result.correlationId[] = await src.decodeShortString()
    if result.basicFlags.replyTo:
      result.replyTo[] = await src.decodeShortString()
    if result.basicFlags.expiration:
      result.expiration[] = await src.decodeShortString()
    if result.basicFlags.messageId:
      result.messageId[] = await src.decodeShortString()
    if result.basicFlags.timestamp:
      let ts = await src.readBE64()
      result.timestamp[] = ts.fromUnix()
    if result.basicFlags.pType:
      result.pType[] = await src.decodeShortString()
    if result.basicFlags.userId:
      result.userId[] = await src.decodeShortString()
    if result.basicFlags.appId:
      result.appId[] = await src.decodeShortString()
    if result.basicFlags.clusterId:
      result.clusterId[] = await src.decodeShortString()
  else:
    discard

proc encodeProperties*(p: Properties, dst: AsyncBufferedSocket): Future[Properties] {.async.} =
  var flags: uint32 = (if p.kind == BASIC_PROPERTIES: cast[uint32](p.basicFlags) else: p.flags)
  while true:
    let remainder:uint32 = flags shr 16
    var partialFlags = (flags and 0xfffe).uint16
    if remainder != 0:
      partialFlags = partialFlags or 0x0001
    await dst.writeBE(partialFlags)
    flags = remainder
    if flags == 0:
      break
  case p.kind:
  of BASIC_PROPERTIES:
    if p.basicFlags.contentType:
      await dst.encodeShortString(p.contentType[])
    if p.basicFlags.contentEncoding:
      await dst.encodeShortString(p.contentEncoding[])
    if p.basicFlags.headers:
      await dst.encodeTable(p.headers)
    if p.basicFlags.deliveryMode:
      await dst.write(p.deliveryMode)
    if p.basicFlags.priority:
      await dst.write(p.priority)
    if p.basicFlags.correlationId:
      await dst.encodeShortString(p.correlationId[])
    if p.basicFlags.replyTo:
      await dst.encodeShortString(p.replyTo[])
    if p.basicFlags.expiration:
      await dst.encodeShortString(p.expiration[])
    if p.basicFlags.messageId:
      await dst.encodeShortString(p.messageId[])
    if p.basicFlags.timestamp:
      let ts = p.timestamp[].toUnix()
      await dst.writeBE(ts)
    if p.basicFlags.pType:
      await dst.encodeShortString(p.pType[])
    if p.basicFlags.userId:
      await dst.encodeShortString(p.userId[])
    if p.basicFlags.appId:
      await dst.encodeShortString(p.appId[])
    if p.basicFlags.clusterId:
      await dst.encodeShortString(p.clusterId[])
  else:
    discard
