import std/[unittest, asyncdispatch]
import rabbitmq/rabbitmq

suite "RabbitMQ connection":
  setup:
    discard

  test "Simple connect/disconnect":
    proc testConnection() {.async} =
      var address = "amqp://test:test@localhost/test".fromURL()
      var connection = newRabbitMQ(address, 1)
      var rabbit: RabbitMQConn
      try:
        await connection.connect()
        rabbit = await connection.acquire()
      except RMQConnectionFailed:
        echo "Can't connect to Redis instance"
        fail()
      finally:
        if not rabbit.isNil:
          rabbit.release()
        await connection.close()
    waitFor(testConnection())
#[
  test "Simple connect/disconnect using with statement":
    proc testConnection() {.async} =
      var address = "amqp://test:test@localhost/test".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        connection.withRabbit:
          echo "RMQ connected"
          #await rabbit.encodeString(@["PING"])
          #let replStr = await redis.readLine()
          #check(replStr == "+PONG")
      except RMQConnectionFailed:
        echo "Can't connect to Redis instance"
        fail()
      await connection.close()
    waitFor(testConnection())
]#  