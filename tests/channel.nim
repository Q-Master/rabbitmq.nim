import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection]

suite "RabbitMQ channel":
  setup:
    discard

  test "Channel create/close":
    proc testChannelCreation() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        check chan.channelId == 1
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())
