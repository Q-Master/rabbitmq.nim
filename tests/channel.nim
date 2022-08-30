import std/[unittest, asyncdispatch]
import rabbitmq/[rabbitmq, connection]

suite "RabbitMQ channel":
  setup:
    discard

  test "Simple channel create/close":
    proc testChannelCreation() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Channel allocated"
        check chan.channelId == 1
        checkpoint "Closing the channel"
        await chan.close()
        checkpoint "Channel closed ok"
      except RMQConnectionFailed:
        checkpoint "Can't connect to Redis instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
        checkpoint "Connection closed"
    waitFor(testChannelCreation())
