import asyncdispatch
import faststreams/[inputs, outputs]
from ./methods/mthd import Method
import ./methods/connection
import ./methods/channel
import ./methods/exchange
import ./methods/queue
import ./methods/basic
import ./methods/tx
import ./methods/confirm
import ./methods/access
import ./exceptions
import ./data

export connection, channel, exchange, queue, basic, tx, confirm, access
export Method

const NO_SUCH_METHOD_STR = "No such method"

proc decodeMethod*(encoded: InputStream): Method =
  let (_, methodId) = encoded.readBigEndianU32()
  case methodId
  of 0x000A000A:
    result = ConnectionStart.decode(encoded)
  of 0x000A000B:
    result = ConnectionStartOk.decode(encoded)
  of 0x000A0014:
    result = ConnectionSecure.decode(encoded)
  of 0x000A0015:
    result = ConnectionSecureOk.decode(encoded)
  of 0x000A001E:
    result = ConnectionTune.decode(encoded)
  of 0x000A001F:
    result = ConnectionTuneOk.decode(encoded)
  of 0x000A0028:
    result = ConnectionOpen.decode(encoded)
  of 0x000A0029:
    result = ConnectionOpenOk.decode(encoded)
  of 0x000A0032:
    result = ConnectionClose.decode(encoded)
  of 0x000A0033:
    result = ConnectionCloseOk.decode(encoded)
  of 0x000A003C:
    result = ConnectionBlocked.decode(encoded)
  of 0x000A003D:
    result = ConnectionUnblocked.decode(encoded)
  of 0x0014000A:
    result = ChannelOpen.decode(encoded)
  of 0x0014000B:
    result = ChannelOpenOk.decode(encoded)
  of 0x00140014:
    result = ChannelFlow.decode(encoded)
  of 0x00140015:
    result = ChannelFlowOk.decode(encoded)
  of 0x00140028:
    result = ChannelClose.decode(encoded)
  of 0x00140029:
    result = ChannelCloseOk.decode(encoded)
  of 0x001E000A:
    result = AccessRequest.decode(encoded)
  of 0x001E000B:
    result = AccessRequestOk.decode(encoded)
  of 0x0028000A:
    result = ExchangeDeclare.decode(encoded)
  of 0x0028000B:
    result = ExchangeDeclareOk.decode(encoded)
  of 0x00280014:
    result = ExchangeDelete.decode(encoded)
  of 0x00280015:
    result = ExchangeDeleteOk.decode(encoded)
  of 0x0028001E:
    result = ExchangeBind.decode(encoded)
  of 0x0028001F:
    result = ExchangeBindOk.decode(encoded)
  of 0x00280028:
    result = ExchangeUnbind.decode(encoded)
  of 0x00280033:
    result = ExchangeUnbindOk.decode(encoded)
  of 0x0032000A:
    result = QueueDeclare.decode(encoded)
  of 0x0032000B:
    result = QueueDeclareOk.decode(encoded)
  of 0x00320014:
    result = QueueBind.decode(encoded)
  of 0x00320015:
    result = QueueBindOk.decode(encoded)
  of 0x0032001E:
    result = QueuePurge.decode(encoded)
  of 0x0032001F:
    result = QueuePurgeOk.decode(encoded)
  of 0x00320028:
    result = QueueDelete.decode(encoded)
  of 0x00320029:
    result = QueueDeleteOk.decode(encoded)
  of 0x00320032:
    result = QueueUnbind.decode(encoded)
  of 0x00320033:
    result = QueueUnbindOk.decode(encoded)
  of 0x003C000A:
    result = BasicQos.decode(encoded)
  of 0x003C000B:
    result = BasicQosOk.decode(encoded)
  of 0x003C0014:
    result = BasicConsume.decode(encoded)
  of 0x003C0015:
    result = BasicConsumeOk.decode(encoded)
  of 0x003C001E:
    result = BasicCancel.decode(encoded)
  of 0x003C001F:
    result = BasicCancelOk.decode(encoded)
  of 0x003C0028:
    result = BasicPublish.decode(encoded)
  of 0x003C0032:
    result = BasicReturn.decode(encoded)
  of 0x003C003C:
    result = BasicDeliver.decode(encoded)
  of 0x003C0046:
    result = BasicGet.decode(encoded)
  of 0x003C0047:
    result = BasicGetOk.decode(encoded)
  of 0x003C0048:
    result = BasicGetEmpty.decode(encoded)
  of 0x003C0050:
    result = BasicAck.decode(encoded)
  of 0x003C005A:
    result = BasicReject.decode(encoded)
  of 0x003C0064:
    result = BasicRecoverAsync.decode(encoded)
  of 0x003C006E:
    result = BasicRecover.decode(encoded)
  of 0x003C006F:
    result = BasicRecoverOk.decode(encoded)
  of 0x003C0078:
    result = BasicNack.decode(encoded)
  of 0x005A000A:
    result = TxSelect.decode(encoded)
  of 0x005A000B:
    result = TxSelectOk.decode(encoded)
  of 0x005A0014:
    result = TxCommit.decode(encoded)
  of 0x005A0015:
    result = TxCommitOk.decode(encoded)
  of 0x005A001E:
    result = TxRollback.decode(encoded)
  of 0x005A001F:
    result = TxRollbackOk.decode(encoded)
  of 0x0055000A:
    result = ConfirmSelect.decode(encoded)
  of 0x0055000B:
    result = ConfirmSelectOk.decode(encoded)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

proc encodeMethod*[T: Method](m: T, to: AsyncOutputStream) {.async.}

proc encodeMethod*(m: Method, to: AsyncOutputStream) {.async.} =
  case m.index
  of 0x000A000A:
    await cast[ConnectionStart](m).encodeMethod(to)
  of 0x000A000B:
    await cast[ConnectionStartOk](m).encodeMethod(to)
  of 0x000A0014:
    await cast[ConnectionSecure](m).encodeMethod(to)
  of 0x000A0015:
    await cast[ConnectionSecureOk](m).encodeMethod(to)
  of 0x000A001E:
    await cast[ConnectionTune](m).encodeMethod(to)
  of 0x000A001F:
    await cast[ConnectionTuneOk](m).encodeMethod(to)
  of 0x000A0028:
    await cast[ConnectionOpen](m).encodeMethod(to)
  of 0x000A0029:
    await cast[ConnectionOpenOk](m).encodeMethod(to)
  of 0x000A0032:
    await cast[ConnectionClose](m).encodeMethod(to)
  of 0x000A0033:
    await cast[ConnectionCloseOk](m).encodeMethod(to)
  of 0x000A003C:
    await cast[ConnectionBlocked](m).encodeMethod(to)
  of 0x000A003D:
    await cast[ConnectionUnblocked](m).encodeMethod(to)
  of 0x0014000A:
    await cast[ChannelOpen](m).encodeMethod(to)
  of 0x0014000B:
    await cast[ChannelOpenOk](m).encodeMethod(to)
  of 0x00140014:
    await cast[ChannelFlow](m).encodeMethod(to)
  of 0x00140015:
    await cast[ChannelFlowOk](m).encodeMethod(to)
  of 0x00140028:
    await cast[ChannelClose](m).encodeMethod(to)
  of 0x00140029:
    await cast[ChannelCloseOk](m).encodeMethod(to)
  of 0x001E000A:
    await cast[AccessRequest](m).encodeMethod(to)
  of 0x001E000B:
    await cast[AccessRequestOk](m).encodeMethod(to)
  of 0x0028000A:
    await cast[ExchangeDeclare](m).encodeMethod(to)
  of 0x0028000B:
    await cast[ExchangeDeclareOk](m).encodeMethod(to)
  of 0x00280014:
    await cast[ExchangeDelete](m).encodeMethod(to)
  of 0x00280015:
    await cast[ExchangeDeleteOk](m).encodeMethod(to)
  of 0x0028001E:
    await cast[ExchangeBind](m).encodeMethod(to)
  of 0x0028001F:
    await cast[ExchangeBindOk](m).encodeMethod(to)
  of 0x00280028:
    await cast[ExchangeUnbind](m).encodeMethod(to)
  of 0x00280033:
    await cast[ExchangeUnbindOk](m).encodeMethod(to)
  of 0x0032000A:
    await cast[QueueDeclare](m).encodeMethod(to)
  of 0x0032000B:
    await cast[QueueDeclareOk](m).encodeMethod(to)
  of 0x00320014:
    await cast[QueueBind](m).encodeMethod(to)
  of 0x00320015:
    await cast[QueueBindOk](m).encodeMethod(to)
  of 0x0032001E:
    await cast[QueuePurge](m).encodeMethod(to)
  of 0x0032001F:
    await cast[QueuePurgeOk](m).encodeMethod(to)
  of 0x00320028:
    await cast[QueueDelete](m).encodeMethod(to)
  of 0x00320029:
    await cast[QueueDeleteOk](m).encodeMethod(to)
  of 0x00320032:
    await cast[QueueUnbind](m).encodeMethod(to)
  of 0x00320033:
    await cast[QueueUnbindOk](m).encodeMethod(to)
  of 0x003C000A:
    await cast[BasicQos](m).encodeMethod(to)
  of 0x003C000B:
    await cast[BasicQosOk](m).encodeMethod(to)
  of 0x003C0014:
    await cast[BasicConsume](m).encodeMethod(to)
  of 0x003C0015:
    await cast[BasicConsumeOk](m).encodeMethod(to)
  of 0x003C001E:
    await cast[BasicCancel](m).encodeMethod(to)
  of 0x003C001F:
    await cast[BasicCancelOk](m).encodeMethod(to)
  of 0x003C0028:
    await cast[BasicPublish](m).encodeMethod(to)
  of 0x003C0032:
    await cast[BasicReturn](m).encodeMethod(to)
  of 0x003C003C:
    await cast[BasicDeliver](m).encodeMethod(to)
  of 0x003C0046:
    await cast[BasicGet](m).encodeMethod(to)
  of 0x003C0047:
    await cast[BasicGetOk](m).encodeMethod(to)
  of 0x003C0048:
    await cast[BasicGetEmpty](m).encodeMethod(to)
  of 0x003C0050:
    await cast[BasicAck](m).encodeMethod(to)
  of 0x003C005A:
    await cast[BasicReject](m).encodeMethod(to)
  of 0x003C0064:
    await cast[BasicRecoverAsync](m).encodeMethod(to)
  of 0x003C006E:
    await cast[BasicRecover](m).encodeMethod(to)
  of 0x003C006F:
    await cast[BasicRecoverOk](m).encodeMethod(to)
  of 0x003C0078:
    await cast[BasicNack](m).encodeMethod(to)
  of 0x005A000A:
    await cast[TxSelect](m).encodeMethod(to)
  of 0x005A000B:
    await cast[TxSelectOk](m).encodeMethod(to)
  of 0x005A0014:
    await cast[TxCommit](m).encodeMethod(to)
  of 0x005A0015:
    await cast[TxCommitOk](m).encodeMethod(to)
  of 0x005A001E:
    await cast[TxRollback](m).encodeMethod(to)
  of 0x005A001F:
    await cast[TxRollbackOk](m).encodeMethod(to)
  of 0x0055000A:
    await cast[ConfirmSelect](m).encodeMethod(to)
  of 0x0055000B:
    await cast[ConfirmSelectOk](m).encodeMethod(to)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)

const WRONG_METHOD_STR = "Wrong method"

proc encodeMethod*[T: Method](m: T, to: AsyncOutputStream) {.async.} =
  discard await to.writeBigEndian32(m.index)
  case m.index
  of 0x000A000A:
    if m is not ConnectionStart:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A000B:
    if m is not ConnectionStartOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0014:
    if m is not ConnectionSecure:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0015:
    if m is not ConnectionSecureOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A001E:
    if m is not ConnectionTune:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A001F:
    if m is not ConnectionTuneOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0028:
    if m is not ConnectionOpen:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0029:
    if m is not ConnectionOpenOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0032:
    if m is not ConnectionClose:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A0033:
    if m is not ConnectionCloseOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A003C:
    if m is not ConnectionBlocked:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x000A003D:
    if m is not ConnectionUnblocked:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0014000A:
    if m is not ChannelOpen:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0014000B:
    if m is not ChannelOpenOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00140014:
    if m is not ChannelFlow:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00140015:
    if m is not ChannelFlowOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00140028:
    if m is not ChannelClose:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00140029:
    if m is not ChannelCloseOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x001E000A:
    if m is not AccessRequest:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x001E000B:
    if m is not AccessRequestOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0028000A:
    if m is not ExchangeDeclare:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0028000B:
    if m is not ExchangeDeclareOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00280014:
    if m is not ExchangeDelete:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00280015:
    if m is not ExchangeDeleteOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0028001E:
    if m is not ExchangeBind:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0028001F:
    if m is not ExchangeBindOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00280028:
    if m is not ExchangeUnbind:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00280033:
    if m is not ExchangeUnbindOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0032000A:
    if m is not QueueDeclare:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0032000B:
    if m is not QueueDeclareOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320014:
    if m is not QueueBind:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320015:
    if m is not QueueBindOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0032001E:
    if m is not QueuePurge:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0032001F:
    if m is not QueuePurgeOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320028:
    if m is not QueueDelete:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320029:
    if m is not QueueDeleteOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320032:
    if m is not QueueUnbind:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x00320033:
    if m is not QueueUnbindOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C000A:
    if m is not BasicQos:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C000B:
    if m is not BasicQosOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0014:
    if m is not BasicConsume:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0015:
    if m is not BasicConsumeOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C001E:
    if m is not BasicCancel:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C001F:
    if m is not BasicCancelOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0028:
    if m is not BasicPublish:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0032:
    if m is not BasicReturn:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C003C:
    if m is not BasicDeliver:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0046:
    if m is not BasicGet:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0047:
    if m is not BasicGetOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0048:
    if m is not BasicGetEmpty:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0050:
    if m is not BasicAck:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C005A:
    if m is not BasicReject:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0064:
    if m is not BasicRecoverAsync:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C006E:
    if m is not BasicRecover:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C006F:
    if m is not BasicRecoverOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x003C0078:
    if m is not BasicNack:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A000A:
    if m is not TxSelect:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A000B:
    if m is not TxSelectOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A0014:
    if m is not TxCommit:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A0015:
    if m is not TxCommitOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A001E:
    if m is not TxRollback:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x005A001F:
    if m is not TxRollbackOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0055000A:
    if m is not ConfirmSelect:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  of 0x0055000B:
    if m is not ConfirmSelectOk:
      raise newException(InvalidFrameMethodException, WRONG_METHOD_STR)
  else:
    raise newException(InvalidFrameMethodException, NO_SUCH_METHOD_STR)
  await m.encode(to)