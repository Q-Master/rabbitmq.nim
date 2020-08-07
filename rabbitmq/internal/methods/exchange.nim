import streams
import tables
import ./mthd
import ../data

type ExchangeDeclare* = ref object of Method
  ticket: uint16
  exchange: string
  etype: string
  passive: bool
  durable: bool
  autoDelete: bool
  internal: bool
  noWait: bool
  arguments: TableRef[string, DataTable]

proc newExchangeDeclare*(
  ticket = 0.uint16, 
  exchange="", 
  etype="direct", 
  passive=false, 
  durable=false, 
  autoDelete=false, 
  internal=false, 
  noWait=false, 
  arguments: TableRef[string, DataTable]=nil): ExchangeDeclare =
  result.new
  result.initMethod(true, 0x0028000A)
  result.ticket = ticket
  result.exchange = exchange
  result.etype = etype
  result.passive = passive
  result.durable = durable
  result.autoDelete = autoDelete
  result.internal = internal
  result.noWait = noWait
  result.arguments = arguments

method decode*(self: ExchangeDeclare, encoded: Stream): ExchangeDeclare =
  self.ticket = encoded.readBigEndianU16()
  self.exchange = encoded.readShortString()
  self.etype = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.passive = (bbuf and 0x01) != 0
  self.durable = (bbuf and 0x02) != 0
  self.autoDelete = (bbuf and 0x04) != 0
  self.internal = (bbuf and 0x08) != 0
  self.noWait = (bbuf and 0x10) != 0
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: ExchangeDeclare): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.exchange)
  s.writeShortString(self.etype)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.passive: 0x01 else: 0x00) or 
  (if self.durable: 0x02 else: 0x00) or 
  (if self.autoDelete: 0x04 else: 0x00) or 
  (if self.internal: 0x08 else: 0x00) or 
  (if self.noWait: 0x10 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type ExchangeDeclareOk* = ref object of Method

proc newExchangeDeclareOk*(): ExchangeDeclare =
  result.new
  result.initMethod(false, 0x0028000B)

type ExchangeDelete* = ref object of Method
  ticket: uint16
  exchange: string
  ifUnused: bool
  noWait: bool

proc newExchangeDelete*(ticket = 0.uint16, exchange = "", ifUnused = false, noWait = false): ExchangeDelete =
  result.new
  result.initMethod(true, 0x00280014)
  result.ticket = ticket
  result.exchange = exchange
  result.ifUnused = ifUnused
  result.noWait = noWait

method decode*(self: ExchangeDelete, encoded: Stream): ExchangeDelete =
  self.ticket = encoded.readBigEndianU16()
  self.exchange = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.ifUnused = (bbuf and 0x01) != 0
  self.noWait = (bbuf and 0x02) != 0
  return self

method encode*(self: ExchangeDelete): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.exchange)
  let bbuf: uint8 = 0x00.uint8 or 
  (if self.ifUnused: 0x01 else: 0x00) or 
  (if self.noWait: 0x02 else: 0x00)
  s.writeBigEndian8(bbuf)
  result = s.readAll()
  s.close()

type ExchangeDeleteOk* = ref object of Method

proc newExchangeDeleteOk*(): ExchangeDeleteOk =
  result.new
  result.initMethod(false, 0x00280015)

type ExchangeBind* = ref object of Method
  ticket: uint16
  destination: string
  source: string
  routingKey: string
  noWait: bool
  arguments: TableRef[string, DataTable]

proc newExchangeBind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): ExchangeBind =
  result.new
  result.initMethod(true, 0x0028001E)
  result.destination = destination
  result.source = source
  result.routingKey = routingKey
  result.noWait = noWait
  result.arguments = arguments

method decode*(self: ExchangeBind, encoded: Stream): ExchangeBind =
  self.ticket = encoded.readBigEndianU16()
  self.destination = encoded.readShortString()
  self.source = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noWait = (bbuf and 0x01) != 0
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: ExchangeBind): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.destination)
  s.writeShortString(self.source)
  s.writeShortString(self.routingKey)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type ExchangeBindOk* = ref object of Method

proc newExchangeBindOk*(): ExchangeBindOk =
  result.new
  result.initMethod(false, 0x0028001F)

type ExchangeUnbind* = ref object of Method
  ticket: uint16
  destination: string
  source: string
  routingKey: string
  noWait: bool
  arguments: TableRef[string, DataTable]

proc newExchangeUnbind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, DataTable] = nil): ExchangeUnbind =
  result.new
  result.initMethod(true, 0x00280028)
  result.destination = destination
  result.source = source
  result.routingKey = routingKey
  result.noWait = noWait
  result.arguments = arguments

method decode*(self: ExchangeUnbind, encoded: Stream): ExchangeUnbind =
  self.ticket = encoded.readBigEndianU16()
  self.destination = encoded.readShortString()
  self.source = encoded.readShortString()
  self.routingKey = encoded.readShortString()
  let bbuf = encoded.readBigEndianU8()
  self.noWait = (bbuf and 0x01) != 0
  self.arguments = encoded.decodeTable()
  return self

method encode*(self: ExchangeUnbind): string =
  var s = newStringStream("")
  s.writeBigEndian16(self.ticket)
  s.writeShortString(self.destination)
  s.writeShortString(self.source)
  s.writeShortString(self.routingKey)
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  s.writeBigEndian8(bbuf)
  s.encodeTable(self.arguments)
  result = s.readAll()
  s.close()

type ExchangeUnbindOk* = ref object of Method

proc newExchangeUnbindOk*(): ExchangeUnbindOk =
  result.new
  result.initMethod(false, 0x00280033)
