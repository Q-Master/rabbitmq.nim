import std/[asyncdispatch, hashes]
import ./message

type
  ConsumerTag* = ref ConsumerTagObj
  ConsumerTagObj* = object of RootObj
    id: string
    onDeliver*: proc (msg: Envelope): Future[void]

  Envelope* = ref EnvelopeObj
  EnvelopeObj* = object of RootObj
    consumerTag*: string
    deliveryTag*: uint64
    exchange*: string
    routingKey*: string
    msg*: Message
    redelivered*: bool

proc newEnvelope*(ct: ConsumerTag, dt: uint64, ex: string, rk: string, msg: Message, redelivered: bool = false): Envelope =
  result = Envelope(
    consumerTag: ct.id,
    deliveryTag: dt,
    exchange: ex,
    routingKey: rk,
    msg: msg,
    redelivered: redelivered
  )

proc newConsumerTag*(id: string): ConsumerTag =
  result.new
  result.id = id

proc id*(tag: ConsumerTag): string =
  if tag.isNil:
    result = ""
  else:
    result = tag.id

proc hash*(c: ConsumerTag): Hash = c.id.hash()
proc `==`*(a, b: ConsumerTag): bool = a.id == b.id
