import std/[asyncdispatch, tables]
import pkg/networkutils/buffered_socket
import rabbitmq/internal/[field, frame]
import rabbitmq/internal/methods/all

const RABBITMQ_MAX_BUF: int = 1024

proc testConnection() {.async} =
  let caps: FieldTable = {
    "publisher_confirms": true.asField,
    "exchange_exchange_bindings": true.asField,
    "basic.nack": true.asField,
    "consumer_cancel_notify": true.asField,
    "connection.blocked": true.asField,
    "consumer_priorities": true.asField,
    "authentication_failure_close": true.asField,
    "per_consumer_qos": true.asField,
    "direct_reply_to": true.asField
  }.newTable

  let clientProps: FieldTable = {
      "capabilities":  caps.asField,
      "cluster_name": "rabbit@d7bb69834e3f".asField,
      "copyright": "Copyright (c) 2007-2022 VMware, Inc. or its affiliates.".asField,
      "information": "Licensed under the MPL 2.0. Website: https://rabbitmq.com".asField,
      "platform": "Erlang/OTP 24.3.2".asField,
      "product": "RabbitMQ".asField,
      "version": "3.9.14".asField
    }.newTable

  var sock = newAsyncBufferedSocket(inBufSize = RABBITMQ_MAX_BUF, outBufSize = RABBITMQ_MAX_BUF)
  let meth = newMethod(CONNECTION_START_METHOD_ID)
  meth.connObj = newConnectionStart(0, 9, clientProps, "PLAIN AMQPLAIN", "en_US")
  let frame = newMethodFrame(0, meth)
  echo frame.size
  await sock.encodeFrame(frame)
  sock.showBuffer

waitFor(testConnection())

#[
table size 469
capabilities
Bytes left 456 dec'd 13
table size 199
    "publisher_confirms": true.asField,
    "exchange_exchange_bindings": true.asField,
    "basic.nack": true.asField,
    "consumer_cancel_notify": true.asField,
    "connection.blocked": true.asField,
    "consumer_priorities": true.asField,
    "authentication_failure_close": true.asField,
    "per_consumer_qos": true.asField,
    "direct_reply_to": true.asField
END TABLE
"cluster_name": "rabbit@d7bb69834e3f",
"copyright": "Copyright (c) 2007-2022 VMware, Inc. or its affiliates.",
"information": "Licensed under the MPL 2.0. Website: https://rabbitmq.com",
"platform": "Erlang/OTP 24.3.2",
"product": "RabbitMQ",
"version": "3.9.14"
]#