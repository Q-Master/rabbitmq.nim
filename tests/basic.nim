import std/[unittest, asyncdispatch]
import rabbitmq/[rabbitmq, connection, exchange, basic, message]

suite "RabbitMQ basic":
  setup:
    discard

  test "Basic publish":
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
        let props = newBasicProperties()
        let headers = {"rpc": "OK"}.asFieldTable()
        props.headers=headers
        props.correlationId="The best correlation ID I've ever met"
        let msg = newMessage("Message", props)
        await exchange.publish("", msg, mandatory = true)
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