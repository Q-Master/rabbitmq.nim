import std/[unittest, asyncdispatch]
import asyncrabbitmq/[asyncrabbitmq, connection, exchange, queue, basic, message, consumertag]

suite "RabbitMQ basic":
  const message = "Message"
  setup:
    discard

  test "Basic publish":
    proc testBasicPublish() {.async} =
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
        let msg = newMessage(message, props)
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
    waitFor(testBasicPublish())
  
  test "Basic consume":
    proc testBasicPublish() {.async} =
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Creating exchange"
        let exchange = await chan.exchangeDeclare("testExchange", EXCHANGE_DIRECT, autoDelete = true)
        check exchange.id == "testExchange"
        let props = newBasicProperties()
        let headers = {"rpc": "OK"}.asFieldTable()
        props.headers=headers
        props.correlationId="The best correlation ID I've ever met"
        let msg = newMessage(message, props)
        await exchange.publish("testKey", msg, mandatory = true)
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()
    
    proc testBasicConsume() {.async.} =
      var ct: string
      var resfut = newFuture[void]("Awaiting for result to check")
      proc onDeliverCB(env: Envelope) {.async.} =
        checkpoint "Verifying the message"
        check env.consumerTag == ct
        check env.exchange == "testExchange"
        check env.routingKey == "testKey"
        check env.msg.data == @(message.toOpenArrayByte(0, message.len-1))
        check env.msg.props.headers.hasKey("rpc")
        check env.msg.props.headers["rpc"].stringVal == "OK"
        resfut.complete()
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Creating exchange"
        let exchange = await chan.exchangeDeclare("testExchange", EXCHANGE_DIRECT, autoDelete = true)
        check exchange.id == "testExchange"
        checkpoint "Creating queue"
        let q = await chan.queueDeclare("testQueue", autoDelete = true)
        check q.id == "testQueue"
        checkpoint "Binding the queue"
        let success = await q.queueBind(exchange, routingKey="testKey")
        check success == true
        var consumerTag = await q.consume()
        ct = consumerTag.id
        consumerTag.onDeliver = onDeliverCB
        await resfut
        checkpoint "Cancelling the consumer"
        let ct1 = await chan.cancel(consumerTag)
        check ct1.id == consumerTag.id
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()

    proc testBasicPublishConsume() {.async.} =
      let fut = testBasicConsume()
      await sleepAsync(1000)
      let fut1 = testBasicPublish()
      await fut1
      await fut

    waitFor(testBasicPublishConsume())

  test "Basic return":
    proc testBasicReturn() {.async} =
      var resfut = newFuture[void]("Awaiting for result to check")
      proc onReturnCB(msg: Message) {.async.} =
        checkpoint "Verifying the message"
        check msg.data == @(message.toOpenArrayByte(0, message.len-1))
        check msg.props.headers.hasKey("rpc")
        check msg.props.headers["rpc"].stringVal == "OK"
        resfut.complete()
      
      var address = "amqp://guest:guest@localhost/".fromURL()
      var connection = newRabbitMQ(address, 1)
      try:
        await connection.connect()
        checkpoint "Allocating channel"
        let chan = await connection.openChannel()
        checkpoint "Setting onReturn callback"
        chan.onReturn = onReturnCB
        checkpoint "Creating exchange"
        let exchange = await chan.exchangeDeclare("testExchange", EXCHANGE_DIRECT, autoDelete = true)
        check exchange.id == "testExchange"
        let props = newBasicProperties()
        let headers = {"rpc": "OK"}.asFieldTable()
        props.headers=headers
        props.correlationId="The best correlation ID I've ever met"
        let msg = newMessage(message, props)
        await exchange.publish("testKey", msg, mandatory = true)
        await resfut
        checkpoint "Closing the channel"
        await chan.close()
      except RMQConnectionFailed:
        checkpoint "Can't connect to RabbitMQ instance"
        fail()
      finally:
        checkpoint "Closing the connection"
        await connection.close()

    waitFor(testBasicReturn())
