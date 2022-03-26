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

  BasicPropertiesFlag* {.size: sizeof(uint32).} = enum
    BASIC_PROPERTIES_FLAG_UNUSED_BIT_0     #0
    BASIC_PROPERTIES_FLAG_UNUSED_BIT_1     #1
    BASIC_PROPERTIES_FLAG_CLUSTER_ID       #2
    BASIC_PROPERTIES_FLAG_APP_ID           #3
    BASIC_PROPERTIES_FLAG_USER_ID          #4
    BASIC_PROPERTIES_FLAG_TYPE             #5
    BASIC_PROPERTIES_FLAG_TIMESTAMP        #6
    BASIC_PROPERTIES_FLAG_MESSAGE_ID       #7
    BASIC_PROPERTIES_FLAG_EXPIRATION       #8
    BASIC_PROPERTIES_FLAG_REPLY_TO         #9
    BASIC_PROPERTIES_FLAG_CORRELATION_ID   #10
    BASIC_PROPERTIES_FLAG_PRIORITY         #11
    BASIC_PROPERTIES_FLAG_DELIVERY_MODE    #12
    BASIC_PROPERTIES_FLAG_HEADERS          #13
    BASIC_PROPERTIES_FLAG_CONTENT_ENCODING #14
    BASIC_PROPERTIES_FLAG_CONTENT_TYPE     #15
  
  BasicPropertiesFlags = set[BasicPropertiesFlag]
  
  Properties* = ref PropertiesObj
  PropertiesObj* = object of RootObj
    flags*: uint32
    case kind: PropertiesKind
    of BASIC_PROPERTIES:
      basicFlags*: BasicPropertiesFlags
      contentType*: ref string
      contentEncoding*: ref string
      headers*: TableRef[string, Field]
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
    if BASIC_PROPERTIES_FLAG_CONTENT_TYPE in result.basicFlags:
      result.contentType[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_CONTENT_ENCODING in result.basicFlags:
      result.contentEncoding[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_HEADERS in result.basicFlags:
      result.headers = await src.decodeTable()
    if BASIC_PROPERTIES_FLAG_DELIVERY_MODE in result.basicFlags:
      result.deliveryMode = await src.readU8()
    if BASIC_PROPERTIES_FLAG_PRIORITY in result.basicFlags:
      result.priority = await src.readU8()
    if BASIC_PROPERTIES_FLAG_CORRELATION_ID in result.basicFlags:
      result.correlationId[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_REPLY_TO in result.basicFlags:
      result.replyTo[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_EXPIRATION in result.basicFlags:
      result.expiration[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_MESSAGE_ID in result.basicFlags:
      result.messageId[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_TIMESTAMP in result.basicFlags:
      let ts = await src.readBE64()
      result.timestamp[] = ts.fromUnix()
    if BASIC_PROPERTIES_FLAG_TYPE in result.basicFlags:
      result.pType[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_USER_ID in result.basicFlags:
      result.userId[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_APP_ID in result.basicFlags:
      result.appId[] = await src.decodeShortString()
    if BASIC_PROPERTIES_FLAG_CLUSTER_ID in result.basicFlags:
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
    if BASIC_PROPERTIES_FLAG_CONTENT_TYPE in p.basicFlags:
      await dst.encodeShortString(p.contentType[])
    if BASIC_PROPERTIES_FLAG_CONTENT_ENCODING in p.basicFlags:
      await dst.encodeShortString(p.contentEncoding[])
    if BASIC_PROPERTIES_FLAG_HEADERS in p.basicFlags:
      await dst.encodeTable(p.headers)
    if BASIC_PROPERTIES_FLAG_DELIVERY_MODE in p.basicFlags:
      await dst.write(p.deliveryMode)
    if BASIC_PROPERTIES_FLAG_PRIORITY in p.basicFlags:
      await dst.write(p.priority)
    if BASIC_PROPERTIES_FLAG_CORRELATION_ID in p.basicFlags:
      await dst.encodeShortString(p.correlationId[])
    if BASIC_PROPERTIES_FLAG_REPLY_TO in p.basicFlags:
      await dst.encodeShortString(p.replyTo[])
    if BASIC_PROPERTIES_FLAG_EXPIRATION in p.basicFlags:
      await dst.encodeShortString(p.expiration[])
    if BASIC_PROPERTIES_FLAG_MESSAGE_ID in p.basicFlags:
      await dst.encodeShortString(p.messageId[])
    if BASIC_PROPERTIES_FLAG_TIMESTAMP in p.basicFlags:
      let ts = p.timestamp[].toUnix()
      await dst.writeBE(ts)
    if BASIC_PROPERTIES_FLAG_TYPE in p.basicFlags:
      await dst.encodeShortString(p.pType[])
    if BASIC_PROPERTIES_FLAG_USER_ID in p.basicFlags:
      await dst.encodeShortString(p.userId[])
    if BASIC_PROPERTIES_FLAG_APP_ID in p.basicFlags:
      await dst.encodeShortString(p.appId[])
    if BASIC_PROPERTIES_FLAG_CLUSTER_ID in p.basicFlags:
      await dst.encodeShortString(p.clusterId[])
  else:
    discard
