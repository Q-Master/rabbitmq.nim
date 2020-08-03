import streams

type
  MethodID = enum
    idmNone

  Method* {.inheritable.} = ref MethodObj
  MethodObj* {.inheritable.} = object
    syncronous: bool
    index: uint32
    #methodID: MethodID

proc initMethod*(m: Method, syncronous: bool, index: uint32) =
  m.syncronous = syncronous
  m.index = index

method decode*(self: Method, encoded: Stream): Method {.base.} =
  return self

method encode*(self: Method): string {.base.} =
  return ""
