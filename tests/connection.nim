import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection]

suite "RabbitMQ connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
      except RMQConnectionFailed:
        checkpoint "Can't connect to Redis instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
        checkpoint "Connection closed"
    waitFor(testConnection())
