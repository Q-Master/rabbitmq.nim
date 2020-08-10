import streams
import ./mthd
import ../data

type 
  AccessRequest* = ref object of Method
    realm: string
    exclusive: bool
    passive: bool
    active: bool
    write: bool
    read: bool
  AccessRequestOk* = ref object of Method
    ticket: uint16

#--------------- Access.Request ---------------#

proc newAccessRequest*(realm="/data", exclusive=false, passive=true, active=true, write=true, read=true): AccessRequest =
  result.new
  result.initMethod(true, 0x001E000A)
  result.realm = realm
  result.exclusive = exclusive
  result.passive = passive
  result.active = active
  result.write = write
  result.read = read

proc decode*(_: type[AccessRequest], encoded: Stream): AccessRequest =
  let realm = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  let exclusive = (bbuf and 0x01) != 0
  let passive = (bbuf and 0x02) != 0
  let active = (bbuf and 0x04) != 0
  let write = (bbuf and 0x08) != 0
  let read = (bbuf and 0x10) != 0
  result = newAccessRequest(realm, exclusive, passive, active, write, read)

proc encode*(self: AccessRequest, to: Stream) =
  to.writeShortString(self.realm)
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.exclusive: 0x01 else: 0x00) or 
    (if self.passive: 0x02 else: 0x00) or 
    (if self.active: 0x04 else: 0x00) or 
    (if self.write: 0x08 else: 0x00) or 
    (if self.read: 0x10 else: 0x00)
  to.writeBigEndian8(bbuf)

#--------------- Access.RequestOk ---------------#

proc newAccessRequestOk*(ticket = 1.uint16): AccessRequestOk =
  result.new
  result.initMethod(false, 0x001E000B)
  result.ticket = ticket

proc decode*(_: type[AccessRequestOk], encoded: Stream): AccessRequestOk =
  let ticket = encoded.readBigEndianU16()
  result = newAccessRequestOk(ticket)

proc encode*(self: AccessRequestOk, to: Stream) =
  to.writeBigEndian16(self.ticket)
