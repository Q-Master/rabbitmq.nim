import ./internal/[properties]

type
  Message* = ref MessageObj
  MessageObj* = object of RootObj
    props*: Properties
    data*: seq[byte]

proc newMessage*(data: openArray[byte], props: Properties): Message =
  let props = if props.isNil: newBasicProperties() else: props
  result = Message(props: props, data: @data)
