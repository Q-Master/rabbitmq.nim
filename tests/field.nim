import std/[asyncdispatch, tables, strutils, macros]
import pkg/networkutils/buffered_socket
import rabbitmq/internal/[field, frame]
import rabbitmq/internal/methods/all

const RABBITMQ_MAX_BUF: int = 1024

#[
let caps = {
    "publisher_confirms": true,
    "exchange_exchange_bindings": true,
    "basic.nack": true,
    "consumer_cancel_notify": true,
    "connection.blocked": true,
    "consumer_priorities": true,
    "authentication_failure_close": true,
    "per_consumer_qos": true,
    "direct_reply_to": true
  }.asFieldTable
echo caps
]#
let clientProps = asFieldTable({
  "caps": {
    "publisher_confirms": true,
    "exchange_exchange_bindings": true,
    "basic.nack": true,
    "consumer_cancel_notify": true,
    "connection.blocked": true,
    "consumer_priorities": true,
    "authentication_failure_close": true,
    "per_consumer_qos": true,
    "direct_reply_to": true
  },
  "cluster_name": "rabbit@d7bb69834e3f",
  "copyright": "Copyright (c) 2007-2022 VMware, Inc. or its affiliates.",
  "information": "Licensed under the MPL 2.0. Website: https://rabbitmq.com",
  "platform": "Erlang/OTP 24.3.2",
  "product": "RabbitMQ",
  "version": "3.9.14"
  })
echo clientProps

proc testConnection() {.async} =
  var sock = newAsyncBufferedSocket(inBufSize = RABBITMQ_MAX_BUF, outBufSize = RABBITMQ_MAX_BUF)
  var frame: Frame
  #[
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

  let meth = newMethod(CONNECTION_START_METHOD_ID)
  meth.connObj = newConnectionStart(0, 9, clientProps, "PLAIN AMQPLAIN", "en_US")
  var frame = newMethodFrame(0, meth)
  echo frame.size
  #await sock.encodeFrame(frame)
  #sock.showBuffer
  ]#
  sock.setInBuffer("\x01\x00\x00\x00\x00\x01\x45\x00\x0a\x00\x0b\x00\x00\x01\x21\x07\x70\x72\x6f\x64\x75\x63\x74\x53\x00\x00\x00\x0a\x72\x61\x62\x62\x69\x74\x6d\x71\x2d\x63\x07\x76\x65\x72\x73\x69\x6f\x6e\x53\x00\x00\x00\x0a\x30\x2e\x31\x32\x2e\x30\x2d\x70\x72\x65\x08\x70\x6c\x61\x74\x66\x6f\x72\x6d\x53\x00\x00\x00\x06\x44\x61\x72\x77\x69\x6e\x09\x63\x6f\x70\x79\x72\x69\x67\x68\x74\x53\x00\x00\x00\x49\x43\x6f\x70\x79\x72\x69\x67\x68\x74\x20\x28\x63\x29\x20\x32\x30\x30\x37\x2d\x32\x30\x31\x34\x20\x56\x4d\x57\x61\x72\x65\x20\x49\x6e\x63\x2c\x20\x54\x6f\x6e\x79\x20\x47\x61\x72\x6e\x6f\x63\x6b\x2d\x4a\x6f\x6e\x65\x73\x2c\x20\x61\x6e\x64\x20\x41\x6c\x61\x6e\x20\x41\x6e\x74\x6f\x6e\x75\x6b\x2e\x0b\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69\x6f\x6e\x53\x00\x00\x00\x28\x53\x65\x65\x20\x68\x74\x74\x70\x73\x3a\x2f\x2f\x67\x69\x74\x68\x75\x62\x2e\x63\x6f\x6d\x2f\x61\x6c\x61\x6e\x78\x7a\x2f\x72\x61\x62\x62\x69\x74\x6d\x71\x2d\x63\x0c\x63\x61\x70\x61\x62\x69\x6c\x69\x74\x69\x65\x73\x46\x00\x00\x00\x3c\x1c\x61\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x69\x6f\x6e\x5f\x66\x61\x69\x6c\x75\x72\x65\x5f\x63\x6c\x6f\x73\x65\x74\x01\x1a\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x62\x69\x6e\x64\x69\x6e\x67\x73\x74\x01\x05\x50\x4c\x41\x49\x4e\x00\x00\x00\x0c\x00\x67\x75\x65\x73\x74\x00\x67\x75\x65\x73\x74\x05\x65\x6e\x5f\x55\x53\xce")
  sock.showBuffer
  frame = await sock.decodeFrame()
  if frame.frameType == ftMethod:
    echo "Frame method id: ", $frame.meth.methodId.toHex
    echo $frame.meth.methodId," ", $frame.meth.kind, " ", $frame.meth.connObj.kind
    echo frame.meth.connObj.clientProps
    #for k,v in frame.meth.connObj.clientProps:
    #  echo k, ": ", v
    echo frame.meth.connObj.mechanism
    echo frame.meth.connObj.response
    echo frame.meth.connObj.locale

  let startOKprops: FieldTable = asFieldTable({
    "product": "rabbitmq-c", 
    "version": "0.12.0-pre", 
    "platform": "Darwin", 
    "copyright": "Copyright (c) 2007-2014 VMWare Inc, Tony Garnock-Jones, and Alan Antonuk.", 
    "information": "See https://github.com/alanxz/rabbitmq-c", 
    "capabilities": {
      "authentication_failure_close": true, 
      "exchange_exchange_bindings": true
    }
  })

  
  let startOk = newConnectionStartOkMethod(startOKprops, "PLAIN", "\x00guest\x00guest")
  frame = newMethodFrame(0, startOk)
  echo frame.size
  await sock.encodeFrame(frame)
  sock.showBuffer
  #[
{"version": "0.12.0-pre", "copyright": "Copyright (c) 2007-2014 VMWare Inc, Tony Garnock-Jones, and Alan Antonuk.", "information": "See https://github.com/alanxz/rabbitmq-c", "platform": "Darwin", "product": "rabbitmq-c", "capabilities": {"exchange_exchange_bindings": true, "authentication_failure_close": true}}
PLAIN
guestguest
en_US
  ]#

waitFor(testConnection())

#[

@[1, 0, 0, 0, 0, 1, 118, 0, 10, 0, 11, 0, 0, 1, 84, 7, 118, 101, 114, 115, 105, 111, 110, 83, 48, 46, 49, 11, 105, 110, 102, 111, 114, 109, 97, 116, 105, 111, 110, 83, 83, 101, 101, 32, 104, 116, 116, 112, 115, 58, 47, 47, 103, 105, 116, 104, 117, 98, 46, 99, 111, 109, 47, 81, 45, 77, 97, 115, 116, 101, 114, 47, 114, 97, 98, 98, 105, 116, 109, 113, 46, 110, 105, 109, 8, 112, 108, 97, 116, 102, 111, 114, 109, 83, 78, 105, 109, 32, 49, 46, 54, 46, 52, 12, 99, 97, 112, 97, 98, 105, 108, 105, 116, 105, 101, 115, 70, 0, 0, 0, 140, 26, 101, 120, 99, 104, 97, 110, 103, 101, 95, 101, 120, 99, 104, 97, 110, 103, 101, 95, 98, 105, 110, 100, 105, 110, 103, 115, 116, 1, 18, 112, 117, 98, 108, 105, 115, 104, 101, 114, 95, 99, 111, 110, 102, 105, 114, 109, 115, 116, 1, 28, 97, 117, 116, 104, 101, 110, 116, 105, 99, 97, 116, 105, 111, 110, 95, 102, 97, 105, 108, 117, 114, 101, 95, 99, 108, 111, 115, 101, 116, 1, 18, 99, 111, 110, 110, 101, 99, 116, 105, 111, 110, 46, 98, 108, 111, 99, 107, 101, 100, 116, 0, 10, 98, 97, 115, 105, 99, 46, 110, 97, 99, 107, 116, 1, 22, 99, 111, 110, 115, 117, 109, 101, 114, 95, 99, 97, 110, 99, 101, 108, 95, 110, 111, 116, 105, 102, 121, 116, 1, 7, 112, 114, 111, 100, 117, 99, 116, 83, 114, 97, 98, 98, 105, 116, 109, 113, 46, 110, 105, 109, 9, 99, 111, 112, 121, 114, 105, 103, 104, 116, 83, 86, 108, 97, 100, 105, 109, 105, 114, 32, 66, 101, 114, 101, 122, 101, 110, 107, 111, 32, 60, 113, 109, 97, 115, 116, 101, 114, 50, 48, 48, 48, 64, 103, 109, 97, 105, 108, 46, 99, 111, 109, 62, 5, 80, 76, 65, 73, 78, 10, 0, 0, 0, 0, 116, 101, 115, 116, 0, 116, 101, 115, 116, 5, 101, 110, 95, 85, 83, 206, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


"\x01\x00\x00\x00" \
"\x00\x01\x45\x00\x0a\x00\x0b\x00\x00\x01\x21\x07\x70\x72\x6f\x64" \
"\x75\x63\x74\x53\x00\x00\x00\x0a\x72\x61\x62\x62\x69\x74\x6d\x71" \
"\x2d\x63\x07\x76\x65\x72\x73\x69\x6f\x6e\x53\x00\x00\x00\x0a\x30" \
"\x2e\x31\x32\x2e\x30\x2d\x70\x72\x65\x08\x70\x6c\x61\x74\x66\x6f" \
"\x72\x6d\x53\x00\x00\x00\x06\x44\x61\x72\x77\x69\x6e\x09\x63\x6f" \
"\x70\x79\x72\x69\x67\x68\x74\x53\x00\x00\x00\x49\x43\x6f\x70\x79" \
"\x72\x69\x67\x68\x74\x20\x28\x63\x29\x20\x32\x30\x30\x37\x2d\x32" \
"\x30\x31\x34\x20\x56\x4d\x57\x61\x72\x65\x20\x49\x6e\x63\x2c\x20" \
"\x54\x6f\x6e\x79\x20\x47\x61\x72\x6e\x6f\x63\x6b\x2d\x4a\x6f\x6e" \
"\x65\x73\x2c\x20\x61\x6e\x64\x20\x41\x6c\x61\x6e\x20\x41\x6e\x74" \
"\x6f\x6e\x75\x6b\x2e\x0b\x69\x6e\x66\x6f\x72\x6d\x61\x74\x69\x6f" \
"\x6e\x53\x00\x00\x00\x28\x53\x65\x65\x20\x68\x74\x74\x70\x73\x3a" \
"\x2f\x2f\x67\x69\x74\x68\x75\x62\x2e\x63\x6f\x6d\x2f\x61\x6c\x61" \
"\x6e\x78\x7a\x2f\x72\x61\x62\x62\x69\x74\x6d\x71\x2d\x63\x0c\x63" \
"\x61\x70\x61\x62\x69\x6c\x69\x74\x69\x65\x73\x46\x00\x00\x00\x3c" \
"\x1c\x61\x75\x74\x68\x65\x6e\x74\x69\x63\x61\x74\x69\x6f\x6e\x5f" \
"\x66\x61\x69\x6c\x75\x72\x65\x5f\x63\x6c\x6f\x73\x65\x74\x01\x1a" \
"\x65\x78\x63\x68\x61\x6e\x67\x65\x5f\x65\x78\x63\x68\x61\x6e\x67" \
"\x65\x5f\x62\x69\x6e\x64\x69\x6e\x67\x73\x74\x01\x05\x50\x4c\x41" \
"\x49\x4e\x00\x00\x00\x0c\x00\x67\x75\x65\x73\x74\x00\x67\x75\x65" \
"\x73\x74\x05\x65\x6e\x5f\x55\x53\xce"



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