const 
  MAX_CHANNELS = 65535 # per AMQP 0.9.1 spec.
  CLOSED = 0.uint8
  OPENING = 1.uint8
  OPEN = 2.uint8
  CLOSING = 3.uint8  # client-initiated close in progress


type
  Channel* = ref ChannelObj
  ChannelObj* = object
    state: uint8
    chNum: uint32

proc newChannel*(): Channel = discard

#[ 
if not isinstance(channel_number, int):
    raise exceptions.InvalidChannelNumber(channel_number)

validators.rpc_completion_callback(on_open_callback)

self.channel_number = channel_number
self.callbacks = connection.callbacks
self.connection = connection

# Initially, flow is assumed to be active
self.flow_active = True

self._content_assembler = ContentFrameAssembler()

self._blocked = collections.deque(list())
self._blocking = None
self._has_on_flow_callback = False
self._cancelled = set()
self._consumers = dict()
self._consumers_with_noack = set()
self._on_flowok_callback = None
self._on_getok_callback = None
self._on_openok_callback = on_open_callback
self._state = self.CLOSED

# We save the closing reason exception to be passed to on-channel-close
# callback at closing of the channel. Exception representing the closing
# reason; ChannelClosedByClient or ChannelClosedByBroker on controlled
# close; otherwise another exception describing the reason for failure
# (most likely connection failure).
self._closing_reason = None  # type: None | Exception

# opaque cookie value set by wrapper layer (e.g., BlockingConnection)
# via _set_cookie
self._cookie = None
]#