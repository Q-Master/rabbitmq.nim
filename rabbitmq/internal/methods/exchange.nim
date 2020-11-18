import tables
import ./mthd
import ../data
import ../streams

type 
  ExchangeDeclare* = ref object of Method
    ticket: uint16
    exchange: string
    etype: string
    passive: bool
    durable: bool
    autoDelete: bool
    internal: bool
    noWait: bool
    arguments: TableRef[string, DataTable]
  ExchangeDeclareOk* = ref object of Method
  ExchangeDelete* = ref object of Method
    ticket: uint16
    exchange: string
    ifUnused: bool
    noWait: bool
  ExchangeDeleteOk* = ref object of Method
  ExchangeBind* = ref object of Method
    ticket: uint16
    destination: string
    source: string
    routingKey: string
    noWait: bool
    arguments: TableRef[string, DataTable]
  ExchangeBindOk* = ref object of Method
  ExchangeUnbind* = ref object of Method
    ticket: uint16
    destination: string
    source: string
    routingKey: string
    noWait: bool
    arguments: TableRef[string, DataTable]
  ExchangeUnbindOk* = ref object of Method

#--------------- Exchange.Declare ---------------#

proc newExchangeDeclare*(
  ticket = 0.uint16, 
  exchange="", 
  eType="direct", 
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
  result.eType = eType
  result.passive = passive
  result.durable = durable
  result.autoDelete = autoDelete
  result.internal = internal
  result.noWait = noWait
  result.arguments = arguments

proc decode*(_: type[ExchangeDeclare], encoded: InputStream): ExchangeDeclare =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, exchange) = encoded.readShortString()
  let (_, eType) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let passive = (bbuf and 0x01) != 0
  let durable = (bbuf and 0x02) != 0
  let autoDelete = (bbuf and 0x04) != 0
  let internal = (bbuf and 0x08) != 0
  let noWait = (bbuf and 0x10) != 0
  result = newExchangeDeclare(ticket, exchange, eType, passive, durable, autodelete, internal, noWait, arguments)

proc encode*(self: ExchangeDeclare, to: OutputStream) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.passive: 0x01 else: 0x00) or 
    (if self.durable: 0x02 else: 0x00) or 
    (if self.autoDelete: 0x04 else: 0x00) or 
    (if self.internal: 0x08 else: 0x00) or 
    (if self.noWait: 0x10 else: 0x00)
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.exchange)
  to.writeShortString(self.etype)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Exchange.DeclareOk ---------------#

proc newExchangeDeclareOk*(): ExchangeDeclareOk =
  result.new
  result.initMethod(false, 0x0028000B)

proc decode*(_: type[ExchangeDeclareOk], encoded: InputStream): ExchangeDeclareOk = newExchangeDeclareOk()

proc encode*(self: ExchangeDeclareOk, to: OutputStream) = discard

#--------------- Exchange.Delete ---------------#

proc newExchangeDelete*(ticket = 0.uint16, exchange = "", ifUnused = false, noWait = false): ExchangeDelete =
  result.new
  result.initMethod(true, 0x00280014)
  result.ticket = ticket
  result.exchange = exchange
  result.ifUnused = ifUnused
  result.noWait = noWait

proc decode*(_: type[ExchangeDelete], encoded: InputStream): ExchangeDelete =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, exchange) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let ifUnused = (bbuf and 0x01) != 0
  let noWait = (bbuf and 0x02) != 0
  result = newExchangeDelete(ticket, exchange, ifUnused, noWait)

proc encode*(self: ExchangeDelete, to: OutputStream) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if self.ifUnused: 0x01 else: 0x00) or 
    (if self.noWait: 0x02 else: 0x00)
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.exchange)
  to.writeBigEndian8(bbuf)

#--------------- Exchange.DeleteOk ---------------#

proc newExchangeDeleteOk*(): ExchangeDeleteOk =
  result.new
  result.initMethod(false, 0x00280015)

proc decode*(_: type[ExchangeDeleteOk], encoded: InputStream): ExchangeDeleteOk = newExchangeDeleteOk()

proc encode*(self: ExchangeDeleteOk, to: OutputStream) = discard

#--------------- Exchange.Bind ---------------#

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

proc decode*(_: type[ExchangeBind], encoded: InputStream): ExchangeBind =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, destination) = encoded.readShortString()
  let (_, source) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newExchangeBind(ticket, destination, source, routingKey, noWait, arguments)

proc encode*(self: ExchangeBind, to: OutputStream) =
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.destination)
  to.writeShortString(self.source)
  to.writeShortString(self.routingKey)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Exchange.BindOk ---------------#

proc newExchangeBindOk*(): ExchangeBindOk =
  result.new
  result.initMethod(false, 0x0028001F)

proc decode*(_: type[ExchangeBindOk], encoded: InputStream): ExchangeBindOk = newExchangeBindOk()

proc encode*(self: ExchangeBindOk, to: OutputStream) = discard

#--------------- Exchange.Unbind ---------------#

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

proc decode*(_: type[ExchangeUnbind], encoded: InputStream): ExchangeUnbind =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, destination) = encoded.readShortString()
  let (_, source) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newExchangeUnbind(ticket, destination, source, routingKey, noWait, arguments)

proc encode*(self: ExchangeUnbind, to: OutputStream) =
  let bbuf: uint8 = (if self.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(self.ticket)
  to.writeShortString(self.destination)
  to.writeShortString(self.source)
  to.writeShortString(self.routingKey)
  to.writeBigEndian8(bbuf)
  to.encodeTable(self.arguments)

#--------------- Exchange.UnbindOk ---------------#

proc newExchangeUnbindOk*(): ExchangeUnbindOk =
  result.new
  result.initMethod(false, 0x00280033)

proc decode*(_: type[ExchangeUnbindOk], encoded: InputStream): ExchangeUnbindOk = newExchangeUnbindOk()

proc encode*(self: ExchangeUnbindOk, to: OutputStream) = discard
