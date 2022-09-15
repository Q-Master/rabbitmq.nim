import ./internal/[properties, frame, exceptions]

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
  if frame.frameType == ftHeader and msg.props.isNil:
    #TODO fill the header
    msg.props = frame.props
    msg.data.setLen(frame.bodySize)
    msg.pos = 0
  elif frame.frameType == ftBody and msg.pos != msg.data.len.uint64:
    copyMem(msg.data[msg.pos].addr, frame.fragment[0].addr, frame.fragment.len)
    msg.pos += frame.fragment.len.uint64
  else:
    raise newException(AMQPUnexpectedFrame, "Unexpected frame")
  result = msg.data.len.uint64 == msg.pos
