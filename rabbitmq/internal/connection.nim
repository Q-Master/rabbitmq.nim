import std/[asyncdispatch, asyncnet, times, tables]
import pkg/networkutils/buffered_socket
import ./methods/all
import ./frame
import ./address
import ./exceptions
import ./spec
import ./field
import ./auth

type
  RabbitMQ = ref RabbitMQObj
  RabbitMQObj = object of RootObj
    pool: seq[RabbitMQConn]
    current: int
    timeout: int
    hosts: RMQAddresses
    closing: bool

  RabbitMQConn* = ref RabbitMQConnObj
  RabbitMQConnObj* = object of RootObj
    inuse: bool
    authenticated: bool
    connected: bool
    sock: AsyncBufferedSocket
    serverProps: FieldTable
    channelMax: uint16
    frameMax: uint32
    heartbeat: uint16

#[
uint16_t ConnectionImpl::add(const std::shared_ptr<ChannelImpl> &channel)
{
    // check if we have exceeded the limit already
    if (_maxChannels > 0 && _channels.size() >= _maxChannels) return 0;

    // keep looping to find an id that is not in use
    while (true)
    {
        // is this id in use?
        if (_nextFreeChannel > 0 && _channels.find(_nextFreeChannel) == _channels.end()) break;

        // id is in use, move on
        _nextFreeChannel++;
    }

    // we have a new channel
    _channels[_nextFreeChannel] = channel;

    // done
    return _nextFreeChannel++;
}
void ConnectionImpl::remove(const ChannelImpl *channel)
{
    // skip zero channel
    if (channel->id() == 0) return;

    // remove it
    _channels.erase(channel->id());
}
]#

const
  DEFAULT_POOLSIZE = 5

proc newRabbitMQConn(): RabbitMQConn
proc disconnect[T: RabbitMQConn | RabbitMQConnObj](rabbit: var T): Future[void]
proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.}

proc connect*(pool: RabbitMQ, callback: proc (conn: RabbitMQConn): Future[void] {.closure, gcsafe.} = nil) {.async.} =
  for i in 0 ..< pool.pool.len:
    template conn: untyped = pool.pool[i]
    if conn.isNil:
      conn = newRabbitMQConn()
    if not conn.connected:
      let adr = pool.hosts.next()
      let connFut = conn.sock.connect(adr.host, adr.port)
      var success = false
      if pool.timeout > 0:
        success = await connFut.withTimeout(pool.timeout)
      else:
        await connFut
        success = true
      if success:
        await needLogin(conn, adr)
        conn.connected = true
        if not callback.isNil:
          asyncCheck callback(conn)
      else:
        conn.connected = false
        raise newException(RMQConnectionFailed, "Timeout connecting to RabbitMQ")

proc acquire*(pool: RabbitMQ): Future[RabbitMQConn] {.async.} =
  ## Retrieves next non-in-use async socket for request
  if pool.closing:
    raise newException(RMQConnectionClosed, "Connection is safely closing now")
  let stime = getTime()
  while true:
    template s: untyped = pool.pool[pool.current]
    if s.connected:
      if not s.inuse:
        s.inuse = true
        return s
    pool.current.inc()
    pool.current = pool.current.mod(pool.pool.len)
    if pool.timeout > 0:
      let diff = (getTime() - stime).inMilliseconds()
      if diff > pool.timeout:
        raise newException(RMQConnectionFailed, "Failed to acquire connection")
    if pool.current == 0:
      await sleepAsync(100)

proc release*(rabbit: RabbitMQConn) =
  rabbit.inuse = false

proc newRabbitMQ*(addresses: RMQAddresses, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): RabbitMQ =
  result.new
  result.pool.setLen(poolsize)
  result.current = 0
  result.timeout = timeout
  result.closing = false
  result.hosts = addresses

proc newRabbitMQ*(address: RMQAddress, poolsize: int = DEFAULT_POOLSIZE, timeout: int = 0): RabbitMQ =
  let addrs = RMQAddresses()
  addrs.addresses.add(address)
  addrs.current = 0
  addrs.order = RMQ_CONNECTION_DIRECT
  result = newRabbitMQ(addrs, poolsize, timeout)

proc close*(pool: RabbitMQ) {.async.}=
  pool.closing = true
  var closed: bool = false
  while not closed:
    closed = true
    for i in 0 ..< pool.pool.len:
      template s: untyped = pool.pool[i]
      if not s.isNil:
        if s.inuse:
          closed = false
        else:
          await s.disconnect()
    await sleepAsync(1)

template withRabbit*(t: RabbitMQ, x: untyped) =
  var rabbit {.inject.} = await t.acquire()
  x
  rabbit.release()

proc waitMethods*(rmq: RabbitMQConn, methods: sink seq[uint32]): Future[AMQPMethod] {.async.} =
  let frame = await rmq.sock.decodeFrame()
  if frame.frameType == ftMethod:
    if frame.meth.methodId notin methods:
      raise newException(AMQPUnexpectedMethod, "Method " & $frame.meth.methodId & " is unexpected")
    result = frame.meth
  else:
    raise newException(AMQPUnexpectedFrame, "Frame " & $frame.frameType & " is unexpected")

proc waitMethod*(rmq: RabbitMQConn, meth: uint32): Future[AMQPMethod] =
  result = rmq.waitMethods(@[meth])

proc disconnect[T: RabbitMQConn | RabbitMQConnObj](rabbit: var T): Future[void] =
  if rabbit.connected:
    result = rabbit.sock.close()
    #TODO Add calling all waiting callbacks
  rabbit.connected = false
  rabbit.inuse = false

proc `=destroy`(rabbit: var RabbitMQConnObj) =
  if rabbit.connected:
    waitFor(rabbit.disconnect())

proc newRabbitMQConn(): RabbitMQConn =
  result.new()
  result.inuse = false
  result.connected = false
  result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX, outBufSize = AMQP_FRAME_MAX)
  #result.sock = newAsyncBufferedSocket(inBufSize = AMQP_FRAME_MAX)
  result.channelMax = 0
  result.frameMax = AMQP_FRAME_MAX
  result.heartbeat = 0

proc needLogin(conn: RabbitMQConn, info: RMQAddress) {.async.} =
  let header = newProtocolHeaderFrame(PROTOCOL_VERSION[0], PROTOCOL_VERSION[1], PROTOCOL_VERSION[2])
  await conn.sock.encodeFrame(header)
  let start = await conn.waitMethod(CONNECTION_START_METHOD_ID)
  if start.connObj.versionMajor != PROTOCOL_VERSION[0] or start.connObj.versionMinor != PROTOCOL_VERSION[1]:
    raise newException(AMQPIncompatibleProtocol, "Incompatible protocol version")
  conn.serverProps = start.connObj.serverProperties
  let authMethod = getCheckAuthSupported(start.connObj.mechanisms)
  let authBytes = authMethod.encodeAuth(user = info.login.username, password = info.login.password)
  let clientProps: FieldTable = asFieldTable({
    "platform": PLATFORM,
    "product": PRODUCT,
    "version": RMQVERSION,
    "copyright": AUTHOR,
    "information": INFORMATION,
    "capabilities": {
      "authentication_failure_close": true,
      "exchange_exchange_bindings": true,
      "basic.nack": true,
      "connection.blocked": false,
      "consumer_cancel_notify": true,
      "publisher_confirms": true
    }
  })
  var meth = newConnectionStartOkMethod(clientProps, $authMethod, authBytes)
  var frame = newMethodFrame(0, meth)
  await conn.sock.encodeFrame(frame)
  let tuneOrClose = await conn.waitMethods(@[CONNECTION_TUNE_METHOD_ID, CONNECTION_CLOSE_METHOD_ID])
  if tuneOrClose.methodId == CONNECTION_CLOSE_METHOD_ID:
    raise newException(RMQConnectionClosed, "Connection closed unexpectedly")

  template serverChannelMax: untyped = tuneOrClose.connObj.channelMax
  template serverFrameMax: untyped = tuneOrClose.connObj.frameMax
  template serverHeatbeat: untyped = tuneOrClose.connObj.heartbeat
  template clientChannelMax: untyped = conn.channelMax
  template clientFrameMax: untyped = conn.frameMax
  template clientHeatbeat: untyped = conn.heartbeat

  if serverChannelMax != 0 and (serverChannelMax < clientChannelMax or clientChannelMax == 0):
    clientChannelMax = serverChannelMax
  elif serverChannelMax == 0 and clientChannelMax == 0:
    clientChannelMax = uint16.high
  if serverFrameMax != 0 and serverFrameMax < clientFrameMax:
    clientFrameMax = serverFrameMax
  if serverHeatbeat != 0 and serverHeatbeat < clientHeatbeat:
    clientHeatbeat = serverHeatbeat
  meth = newConnectionTuneOkMethod(clientChannelMax, clientFrameMax, clientHeatbeat)
  frame = newMethodFrame(0, meth)
  await conn.sock.encodeFrame(frame)
  meth = newConnectionOpenMethod(info.vhost, "", true)
  frame = newMethodFrame(0, meth)
  await conn.sock.encodeFrame(frame)
  
#[

static inline int amqp_heartbeat_send(amqp_connection_state_t state) {
  return state->heartbeat;
}

static inline int amqp_heartbeat_recv(amqp_connection_state_t state) {
  return 2 * state->heartbeat;
}

]#

#[

  {
    amqp_connection_tune_ok_t s;
    s.frame_max = client_frame_max;
    s.channel_max = client_channel_max;
    s.heartbeat = client_heartbeat;

    res = amqp_send_method_inner(state, 0, AMQP_CONNECTION_TUNE_OK_METHOD, &s, AMQP_SF_NONE, deadline);
    if (res < 0) {
      goto error_res;
    }
  }

  amqp_release_buffers(state);

  {
    amqp_method_number_t replies[] = {AMQP_CONNECTION_OPEN_OK_METHOD, 0};
    amqp_connection_open_t s;
    s.virtual_host = amqp_cstring_bytes(vhost);
    s.capabilities = amqp_empty_bytes;
    s.insist = 1;

    result = simple_rpc_inner(state, 0, AMQP_CONNECTION_OPEN_METHOD, replies,
                              &s, deadline);
    if (result.reply_type != AMQP_RESPONSE_NORMAL) {
      goto out;
    }
  }

  result.reply_type = AMQP_RESPONSE_NORMAL;
  result.reply.id = 0;
  result.reply.decoded = NULL;
  result.library_error = 0;
  amqp_maybe_release_buffers(state);

out:
  return result;


static amqp_rpc_reply_t simple_rpc_inner(
    amqp_connection_state_t state, amqp_channel_t channel,
    amqp_method_number_t request_id, amqp_method_number_t *expected_reply_ids,
    void *decoded_request_method, amqp_time_t deadline) {
  int status;
  amqp_rpc_reply_t result;

  memset(&result, 0, sizeof(result));

  status = amqp_send_method(state, channel, request_id, decoded_request_method);
  if (status < 0) {
    return amqp_rpc_reply_error(status);
  }

  {
    amqp_frame_t frame;

  retry:
    status = wait_frame_inner(state, &frame, deadline);
    if (status != AMQP_STATUS_OK) {
      if (status == AMQP_STATUS_TIMEOUT) {
        amqp_socket_close(state->socket, AMQP_SC_FORCE);
      }
      return amqp_rpc_reply_error(status);
    }

    /*
     * We store the frame for later processing unless it's something
     * that directly affects us here, namely a method frame that is
     * either
     *  - on the channel we want, and of the expected type, or
     *  - on the channel we want, and a channel.close frame, or
     *  - on channel zero, and a connection.close frame.
     */
    if (!((frame.frame_type == AMQP_FRAME_METHOD) && (((frame.channel == channel) && (amqp_id_in_reply_list(frame.payload.method.id, expected_reply_ids) || (frame.payload.method.id == AMQP_CHANNEL_CLOSE_METHOD))) || ((frame.channel == 0) && (frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD))))) {
      amqp_pool_t *channel_pool;
      amqp_frame_t *frame_copy;
      amqp_link_t *link;

      channel_pool = amqp_get_or_create_channel_pool(state, frame.channel);
      frame_copy = amqp_pool_alloc(channel_pool, sizeof(amqp_frame_t));
      link = amqp_pool_alloc(channel_pool, sizeof(amqp_link_t));
      *frame_copy = frame;

      link->next = NULL;
      link->data = frame_copy;

      if (state->last_queued_frame == NULL) {
        state->first_queued_frame = link;
      } else {
        state->last_queued_frame->next = link;
      }
      state->last_queued_frame = link;

      goto retry;
    }

    result.reply_type =
        (amqp_id_in_reply_list(frame.payload.method.id, expected_reply_ids))
            ? AMQP_RESPONSE_NORMAL
            : AMQP_RESPONSE_SERVER_EXCEPTION;

    result.reply = frame.payload.method;
    return result;
  }
}
]#