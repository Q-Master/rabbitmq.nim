import tables
import ./submethods
import ../data
import ../streams

const EXCHANGE_METHODS* = 0x0028.uint16
const EXCHANGE_DECLARE_METHOD_ID = 0x0028000A.uint32
const EXCHANGE_DECLARE_OK_METHOD_ID = 0x0028000B.uint32
const EXCHANGE_DELETE_METHOD_ID = 0x00280014.uint32
const EXCHANGE_DELETE_OK_METHOD_ID = 0x00280015.uint32
const EXCHANGE_BIND_METHOD_ID = 0x0028001E.uint32
const EXCHANGE_BIND_OK_METHOD_ID = 0x0028001F.uint32
const EXCHANGE_UNBIND_METHOD_ID = 0x00280028.uint32
const EXCHANGE_UNBIND_OK_METHOD_ID = 0x00280033.uint32

type
  ExchangeVariants* = enum
    NONE = 0
    EXCHANGE_DECLARE_METHOD = (EXCHANGE_DECLARE_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_DECLARE_OK_METHOD = (EXCHANGE_DECLARE_OK_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_DELETE_METHOD = (EXCHANGE_DELETE_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_DELETE_OK_METHOD = (EXCHANGE_DELETE_OK_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_BIND_METHOD = (EXCHANGE_BIND_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_BIND_OK_METHOD = (EXCHANGE_BIND_OK_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_UNBIND_METHOD = (EXCHANGE_UNBIND_METHOD_ID and 0x0000FFFF).uint16
    EXCHANGE_UNBIND_OK_METHOD = (EXCHANGE_UNBIND_OK_METHOD_ID and 0x0000FFFF).uint16

type 
  ExchangeMethod* = ref object of SubMethod
    noWait*: bool
    ticket*: uint16
    exchange*: string
    arguments*: TableRef[string, Field]
    case indexLo*: ExchangeVariants
    of EXCHANGE_DECLARE_METHOD:
      etype*: string
      passive*: bool
      durable*: bool
      autoDelete*: bool
      internal*: bool
    of EXCHANGE_DECLARE_OK_METHOD:
      discard
    of EXCHANGE_DELETE_METHOD:
      ifUnused*: bool
    of EXCHANGE_DELETE_OK_METHOD:
      discard
    of EXCHANGE_BIND_METHOD, EXCHANGE_UNBIND_METHOD:
      destination*: string
      source*: string
      routingKey*: string
    of EXCHANGE_BIND_OK_METHOD, EXCHANGE_UNBIND_OK_METHOD:
      discard
    else:
      discard

proc decodeExchangeDeclare(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeDeclare(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeDeclareOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeDeclareOk(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeDelete(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeDelete(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeDeleteOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeDeleteOk(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeBind(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeBind(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeBindOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeBindOk(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeUnbind(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeUnbind(to: OutputStream, data: ExchangeMethod)
proc decodeExchangeUnbindOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod)
proc encodeExchangeUnbindOk(to: OutputStream, data: ExchangeMethod)

proc decode*(_: type[ExchangeMethod], submethodId: ExchangeVariants, encoded: InputStream): (bool, seq[uint16], ExchangeMethod) =
  case submethodId
  of EXCHANGE_DECLARE_METHOD:
    result = decodeExchangeDeclare(encoded)
  of EXCHANGE_DECLARE_OK_METHOD:
    result = decodeExchangeDeclareOk(encoded)
  of EXCHANGE_DELETE_METHOD:
    result = decodeExchangeDelete(encoded)
  of EXCHANGE_DELETE_OK_METHOD:
    result = decodeExchangeDeleteOk(encoded)
  of EXCHANGE_BIND_METHOD:
    result = decodeExchangeBind(encoded)
  of EXCHANGE_BIND_OK_METHOD:
    result = decodeExchangeBindOk(encoded)
  of EXCHANGE_UNBIND_METHOD:
    result = decodeExchangeUnbind(encoded)
  of EXCHANGE_UNBIND_OK_METHOD:
    result = decodeExchangeUnbindOk(encoded)
  else:
      discard

proc encode*(to: OutputStream, data: ExchangeMethod) =
  case data.indexLo
  of EXCHANGE_DECLARE_METHOD:
    to.encodeExchangeDeclare(data)
  of EXCHANGE_DECLARE_OK_METHOD:
    to.encodeExchangeDeclareOk(data)
  of EXCHANGE_DELETE_METHOD:
    to.encodeExchangeDelete(data)
  of EXCHANGE_DELETE_OK_METHOD:
    to.encodeExchangeDeleteOk(data)
  of EXCHANGE_BIND_METHOD:
    to.encodeExchangeBind(data)
  of EXCHANGE_BIND_OK_METHOD:
    to.encodeExchangeBindOk(data)
  of EXCHANGE_UNBIND_METHOD:
    to.encodeExchangeUnbind(data)
  of EXCHANGE_UNBIND_OK_METHOD:
    to.encodeExchangeUnbindOk(data)
  else:
    discard

#--------------- Exchange.Declare ---------------#

proc newExchangeDeclare*(
  ticket = 0.uint16, 
  exchange="", 
  etype="direct", 
  passive=false, 
  durable=false, 
  autoDelete=false, 
  internal=false, 
  noWait=false, 
  arguments: TableRef[string, Field]=nil): (bool, seq[uint16], ExchangeMethod) =
  var res = ExchangeMethod(indexLo: EXCHANGE_DECLARE_METHOD)
  res.ticket = ticket
  res.exchange = exchange
  res.etype = etype
  res.passive = passive
  res.durable = durable
  res.autoDelete = autoDelete
  res.internal = internal
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(EXCHANGE_DECLARE_OK_METHOD).uint16], res)

proc decodeExchangeDeclare(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) =
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
  result = newExchangeDeclare(ticket, exchange, eType, passive, durable, autoDelete, internal, noWait, arguments)

proc encodeExchangeDeclare(to: OutputStream, data: ExchangeMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if data.passive: 0x01 else: 0x00) or 
    (if data.durable: 0x02 else: 0x00) or 
    (if data.autoDelete: 0x04 else: 0x00) or 
    (if data.internal: 0x08 else: 0x00) or 
    (if data.noWait: 0x10 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.exchange)
  to.writeShortString(data.etype)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Exchange.DeclareOk ---------------#

proc newExchangeDeclareOk*(): (bool, seq[uint16], ExchangeMethod) =
  result = (false, @[], ExchangeMethod(indexLo: EXCHANGE_DECLARE_OK_METHOD))

proc decodeExchangeDeclareOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) = newExchangeDeclareOk()

proc encodeExchangeDeclareOk(to: OutputStream, data: ExchangeMethod) = discard

#--------------- Exchange.Delete ---------------#

proc newExchangeDelete*(ticket = 0.uint16, exchange = "", ifUnused = false, noWait = false): (bool, seq[uint16], ExchangeMethod) =
  var res = ExchangeMethod(indexLo: EXCHANGE_DELETE_METHOD)
  res.ticket = ticket
  res.exchange = exchange
  res.ifUnused = ifUnused
  res.noWait = noWait
  result = (true, @[ord(EXCHANGE_DELETE_OK_METHOD).uint16], res)

proc decodeExchangeDelete(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, exchange) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let ifUnused = (bbuf and 0x01) != 0
  let noWait = (bbuf and 0x02) != 0
  result = newExchangeDelete(ticket, exchange, ifUnused, noWait)

proc encodeExchangeDelete(to: OutputStream, data: ExchangeMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if data.ifUnused: 0x01 else: 0x00) or 
    (if data.noWait: 0x02 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.exchange)
  to.writeBigEndian8(bbuf)

#--------------- Exchange.DeleteOk ---------------#

proc newExchangeDeleteOk*(): (bool, seq[uint16], ExchangeMethod) =
  result = (false, @[], ExchangeMethod(indexLo: EXCHANGE_DELETE_OK_METHOD))

proc decodeExchangeDeleteOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) = newExchangeDeleteOk()

proc encodeExchangeDeleteOk(to: OutputStream, data: ExchangeMethod) = discard

#--------------- Exchange.Bind ---------------#

proc newExchangeBind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, Field] = nil): (bool, seq[uint16], ExchangeMethod) =
  var res = ExchangeMethod(indexLo: EXCHANGE_BIND_METHOD)
  res.destination = destination
  res.source = source
  res.routingKey = routingKey
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(EXCHANGE_BIND_OK_METHOD).uint16], res)

proc decodeExchangeBind(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, destination) = encoded.readShortString()
  let (_, source) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newExchangeBind(ticket, destination, source, routingKey, noWait, arguments)

proc encodeExchangeBind(to: OutputStream, data: ExchangeMethod) =
  let bbuf: uint8 = (if data.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.destination)
  to.writeShortString(data.source)
  to.writeShortString(data.routingKey)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Exchange.BindOk ---------------#

proc newExchangeBindOk*(): (bool, seq[uint16], ExchangeMethod) =
  result = (false, @[], ExchangeMethod(indexLo: EXCHANGE_BIND_OK_METHOD))

proc decodeExchangeBindOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) = newExchangeBindOk()

proc encodeExchangeBindOk(to: OutputStream, data: ExchangeMethod) = discard

#--------------- Exchange.Unbind ---------------#

proc newExchangeUnbind*(
  ticket = 0.uint16, 
  destination = "", 
  source = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, Field] = nil): (bool, seq[uint16], ExchangeMethod) =
  var res = ExchangeMethod(indexLo: EXCHANGE_UNBIND_METHOD)
  res.destination = destination
  res.source = source
  res.routingKey = routingKey
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(EXCHANGE_UNBIND_OK_METHOD).uint16], res)

proc decodeExchangeUnbind(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, destination) = encoded.readShortString()
  let (_, source) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newExchangeUnbind(ticket, destination, source, routingKey, noWait, arguments)

proc encodeExchangeUnbind(to: OutputStream, data: ExchangeMethod) =
  let bbuf: uint8 = (if data.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.destination)
  to.writeShortString(data.source)
  to.writeShortString(data.routingKey)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Exchange.UnbindOk ---------------#

proc newExchangeUnbindOk*(): (bool, seq[uint16], ExchangeMethod) =
  result = (false, @[], ExchangeMethod(indexLo: EXCHANGE_UNBIND_OK_METHOD))

proc decodeExchangeUnbindOk(encoded: InputStream): (bool, seq[uint16], ExchangeMethod) = newExchangeUnbindOk()

proc encodeExchangeUnbindOk(to: OutputStream, data: ExchangeMethod) = discard
