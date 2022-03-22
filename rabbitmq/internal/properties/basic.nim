import options
import tables
import times
import ./props
import ../data
import ../streams

const FLAG_CONTENT_TYPE: uint16 = 0x0001 shl 15
const FLAG_CONTENT_ENCODING: uint16 = 0x0001 shl 14
const FLAG_HEADERS: uint16 = 0x0001 shl 13
const FLAG_DELIVERY_MODE: uint16 = 0x0001 shl 12
const FLAG_PRIORITY: uint16 = 0x0001 shl 11
const FLAG_CORRELATION_ID: uint16 = 0x0001 shl 10
const FLAG_REPLY_TO: uint16 = 0x0001 shl 9
const FLAG_EXPIRATION: uint16 = 0x0001 shl 8
const FLAG_MESSAGE_ID: uint16 = 0x0001 shl 7
const FLAG_TIMESTAMP: uint16 = 0x0001 shl 6
const FLAG_TYPE: uint16 = 0x0001 shl 5
const FLAG_USER_ID: uint16 = 0x0001 shl 4
const FLAG_APP_ID: uint16 = 0x0001 shl 3
const FLAG_CLUSTER_ID: uint16 = 0x0001 shl 2


type 
  BasicProperties* = ref object of Properties
    contentType: Option[string]
    contentEncoding: Option[string]
    headers: TableRef[string, Field]
    deliveryMode: Option[uint8]
    priority: Option[uint8]
    correlationId: Option[string]
    replyTo: Option[string]
    expiration: Option[string]
    messageId: Option[string]
    timestamp: Option[Time]
    pType: Option[string]
    userId: Option[string]
    appId: Option[string]
    clusterId: Option[string]

proc newBasicProperties*(
    contentType=none(string), 
    contentEncoding=none(string), 
    headers:TableRef[string, Field]=nil, 
    deliveryMode=none(uint8), 
    priority=none(uint8), 
    correlationId=none(string), 
    replyTo=none(string), 
    expiration=none(string), 
    messageId=none(string), 
    timestamp=none(Time), 
    pType=none(string), 
    userId=none(string), 
    appId=none(string), 
    clusterId=none(string)
  ): BasicProperties =
  result.new
  result.contentType = contentType
  result.contentEncoding = contentEncoding
  result.headers = headers
  result.deliveryMode = deliveryMode
  result.priority = priority
  result.correlationId = correlationId
  result.replyTo = replyTo
  result.expiration = expiration
  result.messageId = messageId
  result.timestamp = timestamp
  result.pType = pType
  result.userId = userId
  result.appId = appId
  result.clusterId = clusterId

proc decode*(_: type[BasicProperties], encoded: InputStream): BasicProperties =
  var flags: uint64 = 0
  var fidx = 0
  while true:
    let (_, pflags) = encoded.readBigEndianU16()
    flags = flags or (pflags.uint64 shl (fidx*16))
    if (pflags and 0x0001) == 0:
      break
    fidx += 1
  let contentType = block:
    if (flags and FLAG_CONTENT_TYPE) != 0: 
      let (_, ct) = encoded.readShortString()
      ct.option()
    else: 
      none(string)
  let contentEncoding = block:
    if (flags and FLAG_CONTENT_ENCODING) != 0: 
      let (_, ce) = encoded.readShortString()
      ce.option()
    else:
      none(string)
  let (_, headers) = (if (flags and FLAG_HEADERS) != 0: encoded.decodeTable() else: (1, nil))
  let deliveryMode = block:
    if (flags and FLAG_DELIVERY_MODE) != 0: 
      let (_, dm) = encoded.readBigEndianU8()
      dm.option()
    else: 
      none(uint8)
  let priority = block:
    if (flags and FLAG_PRIORITY) != 0: 
      let (_, pri) = encoded.readBigEndianU8()
      pri.option()
    else: 
      none(uint8)
  let correlationId = block:
    if (flags and FLAG_CORRELATION_ID) != 0:
      let (_, cid) = encoded.readShortString()
      cid.option()
    else:
      none(string)
  let replyTo = block:
    if (flags and FLAG_REPLY_TO) != 0:
      let (_, rto) = encoded.readShortString()
      rto.option()
    else:
      none(string)
  let expiration = block:
    if (flags and FLAG_EXPIRATION) != 0:
      let (_, exp) = encoded.readShortString()
      exp.option()
    else:
      none(string)
  let messageId = block:
    if (flags and FLAG_MESSAGE_ID) != 0:
      let (_, mid) = encoded.readShortString()
      mid.option()
    else:
      none(string)
  let timestamp = block:
    if (flags and FLAG_TIMESTAMP) != 0: 
      let(_, tm) = encoded.readBigEndian64() 
      fromUnix(tm).option
    else:
      none(Time)
  let pType = block:
    if (flags and FLAG_TYPE) != 0:
      let (_, pt) = encoded.readShortString()
      pt.option()
    else:
      none(string)
  let userId = block:
    if (flags and FLAG_USER_ID) != 0:
      let (_, uid) = encoded.readShortString()
      uid.option()
    else:
      none(string)
  let appId = block:
    if (flags and FLAG_APP_ID) != 0:
      let (_, aid) = encoded.readShortString()
      aid.option()
    else:
      none(string)
  let clusterId = block:
    if (flags and FLAG_CLUSTER_ID) != 0:
      let (_, cid) = encoded.readShortString()
      cid.option()
    else:
      none(string)
  result = newBasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, expiration, messageId, timestamp, pType, userId, appId, clusterId)

proc encode*(self: BasicProperties, to: OutputStream) =
  let tmpStream = newOutputStream()
  var flags: uint64 = 0
  if self.contentType.isSome():
    tmpStream.writeShortString(self.contentType.get())
    flags = flags or FLAG_CONTENT_TYPE
  if self.contentEncoding.isSome():
    tmpStream.writeShortString(self.contentEncoding.get())
    flags = flags or FLAG_CONTENT_ENCODING
  if not self.headers.isNil():
    tmpStream.encodeTable(self.headers)
    flags = flags or FLAG_HEADERS
  if self.deliveryMode.isSome():
    tmpStream.writeBigEndian8(self.deliveryMode.get())
    flags = flags or FLAG_DELIVERY_MODE
  if self.priority.isSome():
    tmpStream.writeBigEndian8(self.priority.get())
    flags = flags or FLAG_PRIORITY
  if self.correlationId.isSome():
    tmpStream.writeShortString(self.correlationId.get())
    flags = flags or FLAG_CORRELATION_ID
  if self.replyTo.isSome():
    tmpStream.writeShortString(self.replyTo.get())
    flags = flags or FLAG_REPLY_TO
  if self.expiration.isSome():
    tmpStream.writeShortString(self.expiration.get())
    flags = flags or FLAG_EXPIRATION
  if self.messageId.isSome():
    tmpStream.writeShortString(self.messageId.get())
    flags = flags or FLAG_MESSAGE_ID
  if self.timestamp.isSome():
    tmpStream.writeBigEndian64(self.timestamp.get().toUnix())
    flags = flags or FLAG_TIMESTAMP
  if self.pType.isSome():
    tmpStream.writeShortString(self.pType.get())
    flags = flags or FLAG_TYPE
  if self.userId.isSome():
    tmpStream.writeShortString(self.userId.get())
    flags = flags or FLAG_USER_ID
  if self.appId.isSome():
    tmpStream.writeShortString(self.appId.get())
    flags = flags or FLAG_APP_ID
  if self.clusterId.isSome():
    tmpStream.writeShortString(self.clusterId.get())
    flags = flags or FLAG_CLUSTER_ID
  
  while true:
    let remainder = flags shr 16
    var pflags = (flags and 0xfffe).uint16
    if remainder != 0:
      pflags = pflags or 0x0001
    to.writeBigEndian16(pflags)
    flags = remainder
    if flags == 0:
      break
  let output: string = tmpStream.readAll()
  to.write(output)
  
#[
class BasicProperties(amqp_object.Properties):
    CLASS = Basic
    INDEX = 0x003C  # 60
    NAME = 'BasicProperties'
]#