#[
Class Grammar:
    basic = C:QOS S:QOS-OK
          / C:CONSUME S:CONSUME-OK
          / C:CANCEL S:CANCEL-OK
          / C:PUBLISH content
          / S:RETURN content
          / S:DELIVER content
          / C:GET ( S:GET-OK content / S:GET-EMPTY )
          / C:ACK
          / S:ACK
          / C:REJECT
          / C:NACK
          / S:NACK
          / C:RECOVER-ASYNC
          / C:RECOVER S:RECOVER-OK
]#

import std/[asyncdispatch]
import ../internal/methods/all
import ../internal/[exceptions, field, properties, frame]
import ../connection
import ./queue
import ./exchange

type
  ConsumerTag* = ref ConsumerTagObj
  ConsumerTagObj* = object of RootObj
    consumerTagId*: string
    channel: Channel
    onDeliver: proc (msg: Message): Future[void]
    onReturn: proc (msg: Message): Future[void]
    stop: bool
    stopFuture: Future[void]

  Message* = ref MessageObj
  MessageObj* = object of RootObj
    props*: Properties
    data*: seq[byte]
  
  Envelope* = ref EnvelopeObj
  EnvelopeObj* = object of RootObj
    consumerTag*: ConsumerTag
    deliveryTag*: uint64
    exchange*: Exchange
    routingKey*: string
    msg*: Message
    redelivered*: bool

#[
/**
 * Envelope object
 *
 * \since v0.4.0
 */
typedef struct amqp_envelope_t_ {
  amqp_channel_t channel;     /**< channel message was delivered on */
  amqp_bytes_t consumer_tag;  /**< the consumer tag the message was delivered to
                               */
  uint64_t delivery_tag;      /**< the messages delivery tag */
  amqp_boolean_t redelivered; /**< flag indicating whether this message is being
                                 redelivered */
  amqp_bytes_t exchange;      /**< exchange this message was published to */
  amqp_bytes_t routing_key; /**< the routing key this message was published with
                             */
  amqp_message_t message;   /**< the message */
} amqp_envelope_t;
]#

proc getConsumerTagId(tag: ConsumerTag): string =
  if tag.isNil:
    result = ""
  else:
    result = tag.consumerTagId

proc basicQos*(channel: Channel, prefetchSize=0.uint32, prefetchCount=0.uint16, globalQos=false) {.async.} =
  let res {.used.} = await channel.rpc(
    newBasicQosMethod(
      prefetchSize, prefetchCount, globalQos
    ), 
    @[BASIC_QOS_OK_METHOD_ID]
  )

proc basicConsume*(
  queue: Queue, consumerTag: ConsumerTag = nil, 
  noLocal=false, noAck=false, exclusive=false, noWait=false, 
  args: FieldTable=nil
  ): Future[ConsumerTag] {.async.} =
  let res = await queue.channel.rpc(
    newBasicConsumeMethod(
      queue.queueId, consumerTag.getConsumerTagId(),
      noLocal, noAck, exclusive, noWait,
      args
    ), 
    @[BASIC_CONSUME_OK_METHOD_ID]
  )
  result = ConsumerTag(consumerTagId: res.basicObj.consumerTag, channel: queue.channel, stop: false, stopFuture: newFuture[void]("Consumer stop"))
  #TODO start DELIVER and RETURN callbacks here

proc basicCancel*(consumerTag: ConsumerTag, noWait=false): Future[ConsumerTag] {.async.} =
  let res = await consumerTag.channel.rpc(
    newBasicCancelMethod(
      consumerTag.getConsumerTagId(), noWait
    ), 
    @[BASIC_CANCEL_OK_METHOD_ID]
  )
  if res.basicObj.consumerTag != consumerTag.consumerTagId:
    raise newException(AMQPCommandInvalid, "Consumer tag differs")
  result = consumerTag
  consumerTag.stop = true
  await consumerTag.stopFuture
  #TODO stop DELIVER and RETURN callbacks here

proc basicPublish*(exchange: Exchange, routingKey: string, data: openArray[byte], mandatory=false, immediate=false) {.async.} =
  discard

proc consumer(consumerTag: ConsumerTag) {.async.} =
  while consumerTag.stop == false:
    let frame = await consumerTag.channel.waitOrGetFrame()
    if frame.frameType == ftMethod and frame.meth.methodId in [BASIC_DELIVER_METHOD_ID, BASIC_RETURN_METHOD_ID]:
      var envelope = Envelope(consumerTag: consumerTag)
      case frame.meth.methodId
      of BASIC_DELIVER_METHOD_ID:
        envelope.deliveryTag = frame.meth.basicObj.deliveryTag
        envelope.exchange = newExchange(frame.meth.basicObj.exchange, consumerTag.channel)
        envelope.routingKey = frame.meth.basicObj.routingKey
        envelope.redelivered = frame.meth.basicObj.redelivered
      of BASIC_RETURN_METHOD_ID:

      else:
        discard



proc queue*(consumerTag: ConsumerTag): Queue = consumerTag.queue

#[


amqp_rpc_reply_t amqp_consume_message(amqp_connection_state_t state,
                                      amqp_envelope_t *envelope,
                                      const struct timeval *timeout,
                                      AMQP_UNUSED int flags) {
  int res;
  amqp_frame_t frame;
  amqp_basic_deliver_t *delivery_method;
  amqp_rpc_reply_t ret;

  memset(&ret, 0, sizeof(ret));
  memset(envelope, 0, sizeof(*envelope));

  res = amqp_simple_wait_frame_noblock(state, &frame, timeout);
  if (AMQP_STATUS_OK != res) {
    ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
    ret.library_error = res;
    goto error_out1;
  }
  
  if (AMQP_FRAME_METHOD != frame.frame_type ||
      AMQP_BASIC_DELIVER_METHOD != frame.payload.method.id) {
    amqp_put_back_frame(state, &frame);
    ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
    ret.library_error = AMQP_STATUS_UNEXPECTED_STATE;
    goto error_out1;
  }

  delivery_method = frame.payload.method.decoded;

  envelope->channel = frame.channel;
  envelope->consumer_tag = amqp_bytes_malloc_dup(delivery_method->consumer_tag);
  envelope->delivery_tag = delivery_method->delivery_tag;
  envelope->redelivered = delivery_method->redelivered;
  envelope->exchange = amqp_bytes_malloc_dup(delivery_method->exchange);
  envelope->routing_key = amqp_bytes_malloc_dup(delivery_method->routing_key);

  if (amqp_bytes_malloc_dup_failed(envelope->consumer_tag) ||
      amqp_bytes_malloc_dup_failed(envelope->exchange) ||
      amqp_bytes_malloc_dup_failed(envelope->routing_key)) {
    ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
    ret.library_error = AMQP_STATUS_NO_MEMORY;
    goto error_out2;
  }

  ret = amqp_read_message(state, envelope->channel, &envelope->message, 0);
  if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
    goto error_out2;
  }

  ret.reply_type = AMQP_RESPONSE_NORMAL;
  return ret;

error_out2:
  amqp_bytes_free(envelope->routing_key);
  amqp_bytes_free(envelope->exchange);
  amqp_bytes_free(envelope->consumer_tag);
error_out1:
  return ret;
}

amqp_rpc_reply_t amqp_read_message(amqp_connection_state_t state,
                                   amqp_channel_t channel,
                                   amqp_message_t *message,
                                   AMQP_UNUSED int flags) {
  amqp_frame_t frame;
  amqp_rpc_reply_t ret;

  size_t body_read;
  char *body_read_ptr;
  int res;

  memset(&ret, 0, sizeof(ret));
  memset(message, 0, sizeof(*message));

  res = amqp_simple_wait_frame_on_channel(state, channel, &frame);
  if (AMQP_STATUS_OK != res) {
    ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
    ret.library_error = res;

    goto error_out1;
  }

  if (AMQP_FRAME_HEADER != frame.frame_type) {
    if (AMQP_FRAME_METHOD == frame.frame_type &&
        (AMQP_CHANNEL_CLOSE_METHOD == frame.payload.method.id ||
         AMQP_CONNECTION_CLOSE_METHOD == frame.payload.method.id)) {

      ret.reply_type = AMQP_RESPONSE_SERVER_EXCEPTION;
      ret.reply = frame.payload.method;

    } else {
      ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
      ret.library_error = AMQP_STATUS_UNEXPECTED_STATE;

      amqp_put_back_frame(state, &frame);
    }
    goto error_out1;
  }

  init_amqp_pool(&message->pool, 4096);
  res = amqp_basic_properties_clone(frame.payload.properties.decoded,
                                    &message->properties, &message->pool);

  if (AMQP_STATUS_OK != res) {
    ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
    ret.library_error = res;
    goto error_out3;
  }

  if (0 == frame.payload.properties.body_size) {
    message->body = amqp_empty_bytes;
  } else {
    if (SIZE_MAX < frame.payload.properties.body_size) {
      ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
      ret.library_error = AMQP_STATUS_NO_MEMORY;
      goto error_out1;
    }
    message->body =
        amqp_bytes_malloc((size_t)frame.payload.properties.body_size);
    if (NULL == message->body.bytes) {
      ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
      ret.library_error = AMQP_STATUS_NO_MEMORY;
      goto error_out1;
    }
  }

  body_read = 0;
  body_read_ptr = message->body.bytes;

  while (body_read < message->body.len) {
    res = amqp_simple_wait_frame_on_channel(state, channel, &frame);
    if (AMQP_STATUS_OK != res) {
      ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
      ret.library_error = res;
      goto error_out2;
    }
    if (AMQP_FRAME_BODY != frame.frame_type) {
      if (AMQP_FRAME_METHOD == frame.frame_type &&
          (AMQP_CHANNEL_CLOSE_METHOD == frame.payload.method.id ||
           AMQP_CONNECTION_CLOSE_METHOD == frame.payload.method.id)) {

        ret.reply_type = AMQP_RESPONSE_SERVER_EXCEPTION;
        ret.reply = frame.payload.method;
      } else {
        ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
        ret.library_error = AMQP_STATUS_BAD_AMQP_DATA;
      }
      goto error_out2;
    }

    if (body_read + frame.payload.body_fragment.len > message->body.len) {
      ret.reply_type = AMQP_RESPONSE_LIBRARY_EXCEPTION;
      ret.library_error = AMQP_STATUS_BAD_AMQP_DATA;
      goto error_out2;
    }

    memcpy(body_read_ptr, frame.payload.body_fragment.bytes,
           frame.payload.body_fragment.len);

    body_read += frame.payload.body_fragment.len;
    body_read_ptr += frame.payload.body_fragment.len;
  }

  ret.reply_type = AMQP_RESPONSE_NORMAL;
  return ret;

error_out2:
  amqp_bytes_free(message->body);
error_out3:
  empty_amqp_pool(&message->pool);
error_out1:
  return ret;
}
]#