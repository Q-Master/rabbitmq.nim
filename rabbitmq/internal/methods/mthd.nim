import asyncdispatch
import faststreams/inputs

type
  Method* {.inheritable.} = ref MethodObj
  MethodObj* {.inheritable.} = object
    syncronous*: bool
    index*: uint32

proc initMethod*(m: Method, syncronous: bool, index: uint32) =
  m.syncronous = syncronous
  m.index = index

method decode*(self: Method, encoded: AsyncInputStream): Future[Method] {.base, async.} =
  return self

method encode*(self: Method): Future[string] {.base, async.} =
  return ""
