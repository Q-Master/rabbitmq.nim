import std/[asyncdispatch, hashes]
import ./message

type
  ConsumerTagCB* = proc (env: Envelope, noAck: bool): Future[void] {.closure, gcsafe.}
  ConsumerTag* = ref object
    id: string
    noAck: bool
    onDeliver*: ConsumerTagCB

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


proc newConsumerTag*(id: string, cb: ConsumerTagCB, noAck: bool): ConsumerTag =
  result.new
  result.id = id
  result.onDeliver = cb
  result.noAck = noAck


proc id*(tag: ConsumerTag): string =
  if tag.isNil:
    result = ""
  else:
    result = tag.id


proc hash*(c: ConsumerTag): Hash = c.id.hash()
proc `==`*(a, b: ConsumerTag): bool = a.id == b.id


proc call*(ct: ConsumerTag, dt: uint64, ex: string, rk: string, msg: Message, redelivered: bool = false): Future[void] =
  let env = ct.newEnvelope(dt, ex, rk, msg, redelivered)
  result = ct.onDeliver(env, ct.noAck)
