import streams
import ./exceptions
import ./data
from ./properties/props import Properties
import ./properties/basic

export basic, Properties

proc dispatchProperties*(clsId: uint16, encoded: Stream): Properties =
  case clsId
  of 0x003C:
    result = BasicProperties.decode(encoded)
  else:
    raise newException(InvalidFrameMethodException, "No such property")
