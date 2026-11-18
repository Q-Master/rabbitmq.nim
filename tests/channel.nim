import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection]


const RMQ_URL {.strdefine.} = "amqp://guest:guest@localhost/"


suite "RabbitMQ channel":
  setup:
    discard

  test "Channel create/close":
    proc testChannelCreation() {.async.} =
      var address = RMQ_URL.fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        let ch = await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        check ch.channelId == 1
        check chan.channelId == 2
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())
