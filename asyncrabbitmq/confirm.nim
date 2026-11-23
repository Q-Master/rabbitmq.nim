import std/[asyncdispatch]
import ./internal/methods/all
import ./internal/exceptions
import ./connection


proc select*(channel: AMQPChannel, noWait: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newConfirmSelectMethod(noWait), @[AMQP_CONFIRM_SELECT_OK_METHOD], noWait=noWait
  )
  channel.enableConfirms()


proc waitForConfirms*(channel: AMQPChannel, timeout: int = 0): Future[void] {.async.} =
  if not channel.confirms:
    raise newException(ValueError, "Channel is not in confirm mode")
  if channel.pendingConfirms > 0:
    let waiter = newFuture[void]("waitForConfirms")
    channel.addConfirmWaiter(waiter)
    if timeout > 0:
      let ok = await waiter.withTimeout(timeout)
      if not ok:
        raise newException(RMQConnectionFailed, "waitForConfirms timed out")
    else:
      await waiter
