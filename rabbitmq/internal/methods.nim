import streams
from ./methods/mthd import Method
import ./methods/connection
import ./methods/channel
import ./methods/exchange
import ./methods/queue
import ./methods/basic
import ./methods/tx
import ./methods/confirm
import ./methods/access
import ./methods/props
import ./exceptions
import ./data

export connection, channel, exchange, queue, basic, tx, confirm, props, access
export Method

proc dispatchMethods*(encoded: Stream): Method =
  let methodId = encoded.readBigEndianU32()
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
    raise newException(InvalidFrameMethodException, "No such method")
