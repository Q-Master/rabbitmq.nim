from ./properties/props import Properties
import ./properties/basic
import ./streams

export basic, Properties

proc decodeProperties*[T: Properties](clsId: uint16, encoded: InputStream): T =
  case clsId
  of 0x003C:
    result = BasicProperties.decode(encoded)
  else:
    raise newException(InvalidFrameMethodException, "No such property")
