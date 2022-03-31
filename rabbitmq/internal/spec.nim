import tables

const
  PROTOCOL_VERSION* = (0.uint8, 9.uint8, 1.uint8)
  DEFAULT_PORT* = 5672
  FRAME_HEADER_SIZE* = 7
  DECIMAL_VAL_LENGTH* = 5
  AMQP_FRAME_MAX* = 1024*131
  #AMQP_FRAME_MAX* = 1024
  DEFAULT_POOLSIZE* = 5

let DEFAULT_PORTS* = {
  "amqp": 5672,
  "amqps": 5671,
}.toTable()

const PRODUCT* = "rabbitmq.nim"
const PLATFORM* = "Nim " & NimVersion
const RMQVERSION* = "0.1"
const AUTHOR* = "Vladimir Berezenko <qmaster2000@gmail.com>"
const INFORMATION* = "See https://github.com/Q-Master/rabbitmq.nim"