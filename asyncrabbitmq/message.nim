import ./internal/[properties, frame, exceptions]
export properties


type
  Message* = ref MessageObj
  MessageObj* = object of RootObj
    props*: Properties
    data*: seq[byte]
    pos: uint64


proc newMessage*(): Message =
  result.new()
  result.pos = 0


proc newMessage*(data: openArray[byte], props: Properties): Message =
  let props = if props.isNil: newBasicProperties() else: props
  result = Message(props: props, data: @data)
  result.pos = data.len.uint64


proc newMessage*(data: string, props: Properties): Message = newMessage(data.toOpenArrayByte(0, data.len()-1), props)


proc build*(msg: Message, frame: Frame): bool =
  case frame.frameType
  of ftHeader:
    if not msg.props.isNil:
      raise newException(AMQPUnexpectedFrame, "Duplicate content header")
    msg.props = frame.props
    msg.data.setLenUninit(frame.bodySize.int)
    msg.pos = 0
  of ftBody:
    if msg.props.isNil:
      raise newException(AMQPUnexpectedFrame, "Body frame before content header")
    if frame.fragment.len > 0:
      if msg.pos + frame.fragment.len.uint64 > msg.data.len.uint64:
        raise newException(AMQPUnexpectedFrame, "Body frame exceeds declared body size")
      copyMem(msg.data[msg.pos].addr, frame.fragment[0].addr, frame.fragment.len)
      msg.pos += frame.fragment.len.uint64
  else:
    raise newException(AMQPUnexpectedFrame, "Unexpected frame")
  result = msg.pos == msg.data.len.uint64
