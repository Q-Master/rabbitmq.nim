import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection]


const RMQ_URL {.strdefine.} = "amqp://guest:guest@localhost/"


suite "RabbitMQ connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async.} =
      var address = RMQ_URL.fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        let chn {.used.} = await connection.connect()
      except RMQConnectionFailed:
        checkpoint "Can't connect to Redis instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
        checkpoint "Connection closed"
    waitFor(testConnection())
