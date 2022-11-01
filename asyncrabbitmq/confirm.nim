import std/[asyncdispatch]
import ./internal/methods/all
import ./connection

proc select*(channel: Channel, noWait: bool = false) {.async.} =
  let res {.used.} = await channel.rpc(
    newConfirmSelectMethod(noWait), @[AMQP_CONFIRM_SELECT_OK_METHOD], noWait=noWait
  )
