import std/[asyncdispatch]
import ./internal/methods/all
import ./connection
import ./message

type
  Transaction = ref TransactionObj
  TransactionObj = object of RootObj
    channel: Channel

proc rpc*(tr: Transaction, meth: AMQPMethod, expectedMethods: sink seq[AMQPMethods], payload: Message = nil, noWait: bool = false): Future[Payload] =
  tr.channel.rpc(meth, expectedMethods, payload, noWait)

proc select*(channel: Channel): Future[Transaction] {.async.} =
  let res {.used.} = await channel.rpc(
    newTxSelectMethod(), @[AMQP_TX_SELECT_OK_METHOD]
  )
  result = Transaction(channel: channel)

proc commit*(tr: Transaction): Future[Transaction] {.async.} =
  let res {.used.} = await tr.rpc(
    newTxCommitMethod(), @[AMQP_TX_COMMIT_OK_METHOD]
  )
  result = Transaction(channel: tr.channel)

proc rollback*(tr: Transaction): Future[Transaction] {.async.} =
  let res {.used.} = await tr.rpc(
    newTxRollbackMethod(), @[AMQP_TX_ROLLBACK_OK_METHOD]
  )
  result = Transaction(channel: tr.channel)
