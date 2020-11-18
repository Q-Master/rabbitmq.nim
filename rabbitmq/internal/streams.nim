const CHUNK_SIZE = 4096

type
  InputStream* = ref InputStreamObj
  InputStreamObj = object of RootObj
    data: ptr byte
    curr: ptr byte
    overalSize: uint
    size: uint
 
  OutputStream* = ref OutputStreamObj
  OutputStreamObj = object of RootObj
    datas: seq[string]
    curr: int
    chunk_pos: int
    overalSize: uint
  
  NotEnoughSpaceError* = object of CatchableError

proc newInputStream*(source: openArray[byte]): InputStream =
  result.new()
  result.data = unsafeAddr source[0]
  result.curr = result.data
  result.size = source.len().uint
  result.overalSize = result.size

proc readInto*(s: InputStream, dst: ptr byte, length: int | uint | int32 | uint32 | int64 | uint64): uint {.discardable.} =
  let sz = if length.uint < s.size: length.uint else: s.size
  if sz > 0:
    copyMem(dst, s.curr, sz-1)
    s.size -= sz
    s.curr = cast[ptr byte](cast[uint](s.curr) + cast[uint](sz))
  result = sz

proc readInto*(s: InputStream, dst: var string) : uint {.discardable.} =
  s.readInto(cast[ptr byte](addr(dst[0])), dst.len)

proc rewind*(s: InputStream) =
  s.curr = s.data
  s.size = s.overalSize

proc advance*(s: InputStream, amount: int | uint) =
  s.size -= amount.uint
  s.curr = cast[ptr byte](cast[uint](s.curr) + cast[uint](amount))

proc len*(s: InputStream): uint = s.size

proc newChunk(s: OutputStream)
proc reset*(s: OutputStream)

proc newOutputStream*(): OutputStream =
  result.new()
  result.reset()

proc write*(s: OutputStream, src: ptr byte, length: int | uint | int32 | uint32 | int64 | uint64): uint {.discardable.} =
  if length <= CHUNK_SIZE-s.chunk_pos:
    copyMem(src, addr(s.datas[s.curr][s.chunk_pos]), length)
    s.chunk_pos += length
  else:
    var least = length
    while least > 0:
      if least <= CHUNK_SIZE-s.chunk_pos:
        copyMem(src, addr(s.datas[s.curr][s.chunk_pos]), least)
        s.chunk_pos += least
      else:
        let lngth = CHUNK_SIZE-s.chunk_pos
        copyMem(src, addr(s.datas[s.curr][s.chunk_pos]), lngth)
        s.newChunk()
        least -= lngth
  if s.chunk_pos == CHUNK_SIZE:
    s.newChunk()
  s.overalSize += length.uint

proc write*(s: OutputStream, src: string | seq[uint8 | int8] | seq[byte]): uint {.discardable.} =
  s.write(cast[ptr byte](unsafeAddr src[0]), src.len)

proc readAllInto*(s: OutputStream, dst: var string) =
  if dst.len < s.overalSize.int:
    raise newException(NotEnoughSpaceError, "String is too small")
  var pos: uint = 0
  var least = s.overalSize
  for i in 0..s.datas.len-1:
    if least <= CHUNK_SIZE:
      copyMem(addr(s.datas[i][0]), addr(dst[pos]), least)
      pos += least
    else:
      copyMem(addr(s.datas[i][0]), addr(dst[pos]), CHUNK_SIZE)
      pos += CHUNK_SIZE
      least -= CHUNK_SIZE

proc readAll*(s: OutputStream): string =
  result = newString(s.overalSize)
  s.readAllInto(result)

proc reset*(s: OutputStream) =
  let str = newString(CHUNK_SIZE)
  s.datas = @[str]
  s.curr = 0
  s.chunk_pos = 0
  s.overalSize = 0

proc newChunk(s: OutputStream) =
  let str = newString(CHUNK_SIZE)
  s.datas.add(str)
  s.curr += 1
  s.chunk_pos = 0
