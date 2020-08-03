import streams
import ./mthd
import ../data

type AccessRequest* = ref object of Method
  realm: string
  exclusive: bool
  passive: bool
  active: bool
  write: bool
  read: bool

proc newAccessRequest*(realm="/data", exclusive=false, passive=true, active=true, write=true, read=true): AccessRequest =
  result.new
  result.initMethod(true, 0x001E000A)
  result.realm = realm
  result.exclusive = exclusive
  result.passive = passive
  result.active = active
  result.write = write
  result.read = read

method decode*(self: AccessRequest, encoded: Stream): AccessRequest =
  self.realm = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.exclusive = (bbuf and 0x01) != 0
  self.passive = (bbuf and 0x02) != 0
  self.active = (bbuf and 0x04) != 0
  self.write = (bbuf and 0x08) != 0
  self.read = (bbuf and 0x10) != 0
  return self

method encode*(self: AccessRequest): string =
  var s = newStringStream("")
  s.writeShortString(self.realm)
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.exclusive: 0x01 else: 0x00) or 
    (if self.passive: 0x02 else: 0x00) or 
    (if self.active: 0x04 else: 0x00) or 
    (if self.write: 0x08 else: 0x00) or 
    (if self.read: 0x10 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type AccessRequestOk* = ref object of Method
  ticket: uint16

proc newAccessRequestOk*(ticket = 1.uint16): AccessRequestOk =
  result.new
  result.initMethod(false, 0x001E000B)
  result.ticket = ticket

method decode*(self: AccessRequestOk, encoded: Stream): AccessRequestOk =
  self.ticket = encoded.readBigEndianU16()
  return self

method encode*(self: AccessRequestOk): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  result = s.readAll()
  s.close()
