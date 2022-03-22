import tables
import ./submethods
import ../data
import ../streams

const QUEUE_METHODS* = 0x0032.uint16
const QUEUE_DECLARE_METHOD_ID = 0x0032000A.uint32
const QUEUE_DECLARE_OK_METHOD_ID = 0x0032000B.uint32
const QUEUE_BIND_METHOD_ID = 0x00320014.uint32
const QUEUE_BIND_OK_METHOD_ID = 0x00320015.uint32
const QUEUE_PURGE_METHOD_ID = 0x0032001E.uint32
const QUEUE_PURGE_OK_METHOD_ID = 0x0032001F.uint32
const QUEUE_DELETE_METHOD_ID = 0x00320028.uint32
const QUEUE_DELETE_OK_METHOD_ID = 0x00320029.uint32
const QUEUE_UNBIND_METHOD_ID = 0x00320032.uint32
const QUEUE_UNBIND_OK_METHOD_ID = 0x00320033.uint32

type
  QueueVariants* = enum
    NONE = 0
    QUEUE_DECLARE_METHOD = (QUEUE_DECLARE_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_DECLARE_OK_METHOD = (QUEUE_DECLARE_OK_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_BIND_METHOD = (QUEUE_BIND_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_BIND_OK_METHOD = (QUEUE_BIND_OK_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_PURGE_METHOD = (QUEUE_PURGE_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_PURGE_OK_METHOD = (QUEUE_PURGE_OK_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_DELETE_METHOD = (QUEUE_DELETE_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_DELETE_OK_METHOD = (QUEUE_DELETE_OK_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_UNBIND_METHOD = (QUEUE_UNBIND_METHOD_ID and 0x0000FFFF).uint16
    QUEUE_UNBIND_OK_METHOD = (QUEUE_UNBIND_OK_METHOD_ID and 0x0000FFFF).uint16

type 
  QueueMethod* = ref object of SubMethod
    ticket*: uint16
    queue*: string
    noWait*: bool
    arguments*: TableRef[string, Field]
    case indexLo*: QueueVariants
    of QUEUE_DECLARE_METHOD:
      passive*: bool
      durable*: bool
      exclusive*: bool
      autoDelete*: bool
    of QUEUE_DECLARE_OK_METHOD, QUEUE_PURGE_OK_METHOD, QUEUE_DELETE_OK_METHOD:
      messageCount*: uint32
      consumerCount*: uint32
    of QUEUE_BIND_METHOD, QUEUE_UNBIND_METHOD:
      bQueue*: string
      routingKey*: string
    of QUEUE_BIND_OK_METHOD:
      discard
    of QUEUE_PURGE_METHOD:
      discard
    of QUEUE_DELETE_METHOD:
      ifUnused*: bool
      ifEmpty*: bool
    of QUEUE_UNBIND_OK_METHOD:
      discard
    else:
      discard

proc decodeQueueDeclare(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueDeclare(to: OutputStream, data: QueueMethod)
proc decodeQueueDeclareOk(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueDeclareOk(to: OutputStream, data: QueueMethod)
proc decodeQueueBind(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueBind(to: OutputStream, data: QueueMethod)
proc decodeQueueBindOk(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueBindOk(to: OutputStream, data: QueueMethod)
proc decodeQueuePurge(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueuePurge(to: OutputStream, data: QueueMethod)
proc decodeQueuePurgeOk(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueuePurgeOk(to: OutputStream, data: QueueMethod)
proc decodeQueueDelete(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueDelete(to: OutputStream, data: QueueMethod)
proc decodeQueueDeleteOk(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueDeleteOk(to: OutputStream, data: QueueMethod)
proc decodeQueueUnbind(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueUnbind(to: OutputStream, data: QueueMethod)
proc decodeQueueUnbindOk(encoded: InputStream): (bool, seq[uint16], QueueMethod)
proc encodeQueueUnbindOk(to: OutputStream, data: QueueMethod)

proc decode*(_: type[QueueMethod], submethodId: QueueVariants, encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  case submethodId
  of QUEUE_DECLARE_METHOD:
    result = decodeQueueDeclare(encoded)
  of QUEUE_DECLARE_OK_METHOD:
    result = decodeQueueDeclareOk(encoded)
  of QUEUE_BIND_METHOD:
    result = decodeQueueBind(encoded)
  of QUEUE_BIND_OK_METHOD:
    result = decodeQueueBindOk(encoded)
  of QUEUE_PURGE_METHOD:
    result = decodeQueuePurge(encoded)
  of QUEUE_PURGE_OK_METHOD:
    result = decodeQueuePurgeOk(encoded)
  of QUEUE_DELETE_METHOD:
    result = decodeQueueDelete(encoded)
  of QUEUE_DELETE_OK_METHOD:
    result = decodeQueueDeleteOk(encoded)
  of QUEUE_UNBIND_METHOD:
    result = decodeQueueUnbind(encoded)
  of QUEUE_UNBIND_OK_METHOD:
    result = decodeQueueUnbindOk(encoded)
  else:
      discard

proc encode*(to: OutputStream, data: QueueMethod) =
  case data.indexLo
  of QUEUE_DECLARE_METHOD:
    to.encodeQueueDeclare(data)
  of QUEUE_DECLARE_OK_METHOD:
    to.encodeQueueDeclareOk(data)
  of QUEUE_BIND_METHOD:
    to.encodeQueueBind(data)
  of QUEUE_BIND_OK_METHOD:
    to.encodeQueueBindOk(data)
  of QUEUE_PURGE_METHOD:
    to.encodeQueuePurge(data)
  of QUEUE_PURGE_OK_METHOD:
    to.encodeQueuePurgeOk(data)
  of QUEUE_DELETE_METHOD:
    to.encodeQueueDelete(data)
  of QUEUE_DELETE_OK_METHOD:
    to.encodeQueueDeleteOk(data)
  of QUEUE_UNBIND_METHOD:
    to.encodeQueueUnbind(data)
  of QUEUE_UNBIND_OK_METHOD:
    to.encodeQueueUnbindOk(data)
  else:
    discard

#--------------- Queue.Declare ---------------#

proc newQueueDeclare*(
  ticket = 0.uint16, 
  queue="", 
  passive=false, 
  durable=false, 
  exclusive=false,
  autoDelete=false, 
  noWait=false, 
  arguments: TableRef[string, Field]=nil): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_DECLARE_METHOD)
  res.ticket = ticket
  res.queue = queue
  res.passive = passive
  res.durable = durable
  res.exclusive = exclusive
  res.autoDelete = autoDelete
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(QUEUE_DECLARE_OK_METHOD).uint16], res)

proc decodeQueueDeclare(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let passive = (bbuf and 0x01) != 0
  let durable = (bbuf and 0x02) != 0
  let exclusive = (bbuf and 0x04) != 0
  let autoDelete = (bbuf and 0x08) != 0
  let noWait = (bbuf and 0x10) != 0
  result = newQueueDeclare(ticket, queue, passive, durable, exclusive, autoDelete, noWait, arguments)

proc encodeQueueDeclare(to: OutputStream, data: QueueMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if data.passive: 0x01 else: 0x00) or 
    (if data.durable: 0x02 else: 0x00) or 
    (if data.exclusive: 0x04 else: 0x00) or 
    (if data.autoDelete: 0x08 else: 0x00) or 
    (if data.noWait: 0x10 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Queue.DeclareOk ---------------#

proc newQueueDeclareOk*(queue="", messageCount=0.uint32, consumerCount=0.uint32): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_DECLARE_OK_METHOD)
  res.queue = queue
  res.messageCount = messageCount
  res.consumerCount = consumerCount
  result = (false, @[], res)

proc decodeQueueDeclareOk(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, queue) = encoded.readShortString()
  let (_, messageCount) = encoded.readBigEndianU32()
  let (_, consumerCount) = encoded.readBigEndianU32()
  result = newQueueDeclareOk(queue, messageCount, consumerCount)

proc encodeQueueDeclareOk(to: OutputStream, data: QueueMethod) =
  to.writeShortString(data.queue)
  to.writeBigEndian32(data.messageCount)
  to.writeBigEndian32(data.consumerCount)

#--------------- Queue.Bind ---------------#

proc newQueueBind*(
  ticket = 0.uint16, 
  queue = "", 
  bQueue = "", 
  routingKey = "", 
  noWait=false, 
  arguments: TableRef[string, Field] = nil): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_BIND_METHOD)
  res.queue = queue
  res.bQueue = bQueue
  res.routingKey = routingKey
  res.noWait = noWait
  res.arguments = arguments
  result = (true, @[ord(QUEUE_BIND_OK_METHOD).uint16], res)

proc decodeQueueBind(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bQueue) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let (_, arguments) = encoded.decodeTable()
  let noWait = (bbuf and 0x01) != 0
  result = newQueueBind(ticket, queue, bQueue, routingKey, noWait, arguments)

proc encodeQueueBind(to: OutputStream, data: QueueMethod) =
  let bbuf: uint8 = (if data.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeShortString(data.bQueue)
  to.writeShortString(data.routingKey)
  to.writeBigEndian8(bbuf)
  to.encodeTable(data.arguments)

#--------------- Queue.BindOk ---------------#

proc newQueueBindOk*(): (bool, seq[uint16], QueueMethod) =
  result = (false, @[], QueueMethod(indexLo: QUEUE_BIND_OK_METHOD))

proc decodeQueueBindOk(encoded: InputStream): (bool, seq[uint16], QueueMethod) = newQueueBindOk()

proc encodeQueueBindOk(to: OutputStream, data: QueueMethod) = discard

#--------------- Queue.Purge ---------------#

proc newQueuePurge*(ticket = 0.uint16, queue="", noWait=false): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_PURGE_METHOD)
  res.ticket = ticket
  res.queue = queue
  res.noWait = noWait
  result = (true, @[ord(QUEUE_PURGE_OK_METHOD).uint16], res)

proc decodeQueuePurge(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newQueuePurge(ticket, queue, noWait)

proc encodeQueuePurge(to: OutputStream, data: QueueMethod) =
  let bbuf: uint8 = (if data.noWait: 0x01 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeBigEndian8(bbuf)

#--------------- Queue.PurgeOk ---------------#

proc newQueuePurgeOk*(messageCount = 0.uint32): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_PURGE_OK_METHOD)
  res.messageCount = messageCount
  result = (false, @[], res)

proc decodeQueuePurgeOk(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, messageCount) = encoded.readBigEndianU32()
  result = newQueuePurgeOk(messageCount)

proc encodeQueuePurgeOk(to: OutputStream, data: QueueMethod) =
  to.writeBigEndian32(data.messageCount)

#--------------- Queue.Delete ---------------#

proc newQueueDelete*(ticket = 0.uint16, queue="", ifUnused=false, ifEmpty=false, noWait=false): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_DELETE_METHOD)
  res.ticket = ticket
  res.queue = queue
  res.ifUnused = ifUnused
  res.ifEmpty = ifEmpty
  res.noWait = noWait
  result = (true, @[ord(QUEUE_DELETE_OK_METHOD).uint16], res)

proc decodeQueueDelete(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bbuf) = encoded.readBigEndianU8()
  let ifUnused = (bbuf and 0x01) != 0
  let ifEmpty = (bbuf and 0x02) != 0
  let noWait = (bbuf and 0x04) != 0
  result = newQueueDelete(ticket, queue, ifUnused, ifEmpty, noWait)

proc encodeQueueDelete(to: OutputStream, data: QueueMethod) =
  let bbuf: uint8 = 0x00.uint8 or 
    (if data.ifUnused: 0x01 else: 0x00) or 
    (if data.ifEmpty: 0x02 else: 0x00) or 
    (if data.noWait: 0x04 else: 0x00)
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeBigEndian8(bbuf)

#--------------- Queue.DeleteOk ---------------#

proc newQueueDeleteOk*(messageCount = 0.uint32): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_DELETE_OK_METHOD)
  res.messageCount = messageCount
  result = (false, @[], res)

proc decodeQueueDeleteOk(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, messageCount) = encoded.readBigEndianU32()
  result = newQueueDeleteOk(messageCount)

proc encodeQueueDeleteOk(to: OutputStream, data: QueueMethod) =
  to.writeBigEndian32(data.messageCount)

#--------------- Queue.Unbind ---------------#

proc newQueueUnbind*(
  ticket = 0.uint16, 
  queue = "", 
  bQueue = "", 
  routingKey = "", 
  arguments: TableRef[string, Field] = nil): (bool, seq[uint16], QueueMethod) =
  var res = QueueMethod(indexLo: QUEUE_UNBIND_METHOD)
  res.queue = queue
  res.bQueue = bQueue
  res.routingKey = routingKey
  res.arguments = arguments
  result = (true, @[ord(QUEUE_UNBIND_OK_METHOD).uint16], res)

proc decodeQueueUnbind(encoded: InputStream): (bool, seq[uint16], QueueMethod) =
  let (_, ticket) = encoded.readBigEndianU16()
  let (_, queue) = encoded.readShortString()
  let (_, bQueue) = encoded.readShortString()
  let (_, routingKey) = encoded.readShortString()
  let (_, arguments) = encoded.decodeTable()
  result = newQueueUnbind(ticket, queue, bQueue, routingKey, arguments)

proc encodeQueueUnbind(to: OutputStream, data: QueueMethod) =
  to.writeBigEndian16(data.ticket)
  to.writeShortString(data.queue)
  to.writeShortString(data.bQueue)
  to.writeShortString(data.routingKey)
  to.encodeTable(data.arguments)

#--------------- Queue.UnbindOk ---------------#

proc newQueueUnbindOk*(): (bool, seq[uint16], QueueMethod) =
  result = (false, @[], QueueMethod(indexLo: QUEUE_UNBIND_OK_METHOD))

proc decodeQueueUnbindOk(encoded: InputStream): (bool, seq[uint16], QueueMethod) = newQueueUnbindOk()

proc encodeQueueUnbindOk(to: OutputStream, data: QueueMethod) = discard
