import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection, queue, exchange]


const RMQ_URL {.strdefine.} = "amqp://guest:guest@localhost/"


suite "RabbitMQ queue":
  setup:
    discard

  test "Queue declare/delete":
    proc testChannelCreation() {.async.} =
      var address = RMQ_URL.fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        checkpoint "Allocating channel"
        let chan = await connection.connect()
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue", autoDelete = true)
        check q.id == "testQueue"
        checkpoint "Deleting the queue"
        let msgCount = await q.delete()
        check msgCount == 0
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())

  test "Queue declare/purge":
    proc testChannelCreation() {.async.} =
      var address = RMQ_URL.fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        checkpoint "Allocating channel"
        let chan = await connection.connect()
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue", autoDelete = true)
        check q.id == "testQueue"
        checkpoint "Purging the queue"
        let msgCount = await q.purge()
        check msgCount == 0
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())

  test "Queue bind/unbind":
    proc testChannelCreation() {.async.} =
      var address = RMQ_URL.fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        checkpoint "Allocating channel"
        let chan = await connection.connect()
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue", autoDelete = true)
        check q.id == "testQueue"
        checkpoint "Binding the queue"
        let exch = newExchange("amq.direct", nil)
        var success = await q.queueBind(exch, "test")
        check success == true
        checkpoint "Unbinding the queue"
        success = await q.unbind(exch, "testQueue")
        check success == true
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    waitFor(testChannelCreation())
