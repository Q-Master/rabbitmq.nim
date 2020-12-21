import tables

const PROTOCOL_VERSION* = (0.uint8, 9.uint8, 1.uint8)
const DEFAULT_PORT* = 5672
const FRAME_HEADER_SIZE* = 7
const FRAME_END_SIZE* = 1
const BASIC_FRAME_ID* = 60.uint16
const DECIMAL_VAL_LENGTH* = 5

const FRAME_METHOD* = 1.uint8
const FRAME_HEADER* = 2.uint8
const FRAME_BODY* = 3.uint8
const FRAME_HEARTBEAT* = 8.uint8
const FRAME_END* = 206.uint8

let DEFAULT_PORTS* = {
  "amqp": 5672,
  "amqps": 5671,
}.toTable()

const PRODUCT* = "rabbitmq.nim"
const PLATFORM* = "Nim " & NimVersion
