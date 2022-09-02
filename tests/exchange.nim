import std/[unittest, asyncdispatch, asyncfutures]
import rabbitmq/[rabbitmq, connection, exchange]

suite "RabbitMQ exchange":
  setup:
    discard

  test "Exchange declare/delete":
    proc testChannelCreation() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Creating exchange"
        let exchange = await chan.exchangeDeclare("testExchange", EXCHANGE_DIRECT)
        check exchange.id == "testExchange"
        checkpoint "Deleting the exchange"
        await exchange.delete()
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())
