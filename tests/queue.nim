import std/[unittest, asyncdispatch]
import rabbitmq/[rabbitmq, connection, queue]

suite "RabbitMQ queue":
  setup:
    discard

  test "Queue declare/delete":
    proc testChannelCreation() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue")
        check q.id == "testQueue"
        checkpoint "Deleting the queue"
        let msgCount = await q.delete()
        check msgCount == 0
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())

  test "Queue declare/purge":
    proc testChannelCreation() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue")
        check q.id == "testQueue"
        checkpoint "Purging the queue"
        let msgCount = await q.purge()
        check msgCount == 0
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())
