#import ./mthd
import ./submethods
import ../data
import ../streams

const ACCESS_METHODS* = 0x001E.uint16
const ACCESS_REQUEST_METHOD_ID = 0x001E000A.uint32
const ACCESS_REQUEST_OK_METHOD_ID = 0x001E000B.uint32

type AccessVariants* = enum
  NONE = 0x0000.uint16
  ACCESS_REQUEST_METHOD = (ACCESS_REQUEST_METHOD_ID and 0x0000ffff).uint16
  ACCESS_REQUEST_OK_METHOD = (ACCESS_REQUEST_OK_METHOD_ID and 0x0000ffff).uint16

type
  AccessMethod* = ref object of SubMethod
    case indexLo*: AccessVariants
    of ACCESS_REQUEST_METHOD:
      realm*: string
      exclusive*: bool
      passive*: bool
      active*: bool
      write*: bool
      read*: bool
    of ACCESS_REQUEST_OK_METHOD:
      ticket*: uint16
    else:
      discard

proc decodeAccessRequest(encoded: InputStream): (bool, seq[uint16], AccessMethod)
proc encodeAccessRequest(to: OutputStream, data: AccessMethod)
proc decodeAccessRequestOk(encoded: InputStream): (bool, seq[uint16], AccessMethod)
proc encodeAccessRequestOk(to: OutputStream, data: AccessMethod)

proc decode*(_: type[AccessMethod], submethodId: AccessVariants, encoded: InputStream): (bool, seq[uint16], AccessMethod) =
  case submethodId
  of ACCESS_REQUEST_METHOD:
    result = decodeAccessRequest(encoded)
  of ACCESS_REQUEST_OK_METHOD:
    result = decodeAccessRequestOk(encoded)
  else:
    discard

proc encode*(to: OutputStream, data: AccessMethod) =
  case data.indexLo
  of ACCESS_REQUEST_METHOD:
    to.encodeAccessRequest(data)
  of ACCESS_REQUEST_OK_METHOD:
    to.encodeAccessRequestOk(data)
  else:
    discard

#--------------- Access.Request ---------------#

proc newAccessRequest*(realm="/data", exclusive=false, passive=true, active=true, write=true, read=true): (bool, seq[uint16], AccessMethod) =
  var res = AccessMethod(indexLo: ACCESS_REQUEST_METHOD)
  res.realm = realm
  res.exclusive = exclusive
  res.passive = passive
  res.active = active
  res.write = write
  res.read = read
  result = (true, @[ord(ACCESS_REQUEST_METHOD).uint16], res)

proc decodeAccessRequest(encoded: InputStream): (bool, seq[uint16], AccessMethod) =
  let (_, realm) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let exclusive = (bbuf and 0x01) != 0
  let passive = (bbuf and 0x02) != 0
  let active = (bbuf and 0x04) != 0
  let write = (bbuf and 0x08) != 0
  let read = (bbuf and 0x10) != 0
  result = newAccessRequest(realm, exclusive, passive, active, write, read)

proc encodeAccessRequest(to: OutputStream, data: AccessMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if data.exclusive: 0x01 else: 0x00) or 
    (if data.passive: 0x02 else: 0x00) or 
    (if data.active: 0x04 else: 0x00) or 
    (if data.write: 0x08 else: 0x00) or 
    (if data.read: 0x10 else: 0x00)
  to.writeShortString(data.realm)
  to.writeBigEndian8(bbuf)

#--------------- Access.RequestOk ---------------#

proc newAccessRequestOk*(ticket = 1.uint16): (bool, seq[uint16], AccessMethod) =
  let res = AccessMethod(indexLo: ACCESS_REQUEST_OK_METHOD)
  res.ticket = ticket
  result = (false, @[], res)

proc decodeAccessRequestOk(encoded: InputStream): (bool, seq[uint16], AccessMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  result = newAccessRequestOk(ticket)

proc encodeAccessRequestOk(to: OutputStream, data: AccessMethod) =
  to.writeBigEndian16(data.ticket)
