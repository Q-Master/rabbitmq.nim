import streams
import endians
import times
import tables
import faststreams/[inputs, outputs]
import ./exceptions
import ./async_socket_adapters
import ./spec

type
  Decimal = string
  DataType = enum
    dtBool = "t"
    dtByte = "b"
    dtUByte = "B"
    dtShort = "U"
    dtUShort = "u"
    dtInt = "I"
    dtUInt = "i"
    dtLong = "L"
    dtULong = "l"
    dtFloat = "f"
    dtDouble = "d"
    dtDecimal = "D"
    dtSignedShort = "s"
    dtString = "S"
    dtBytes = "x"
    dtArray = "A"
    dtTimestamp = "T"
    dtTable = "F"
    dtVoid = "V"

  DataTable* = ref DataTableObj
  DataTableObj {.acyclic.} = object
    case dtype: DataType
    of dtBool:
      boolVal: bool
    of dtByte:
      byteVal: int8
    of dtUByte:
      uByteVal: uint8
    of dtShort, dtSignedShort:
      shortVal: int16
    of dtUShort:
      uShortVal: uint16
    of dtInt:
      intVal: int32
    of dtUInt:
      uIntVal: uint32
    of dtLong:
      longVal: int64
    of dtULong:
      uLongVal: uint64
    of dtFloat:
      floatVal: float32
    of dtDouble:
      doubleVal: float64
    of dtDecimal:
      decimalVal: Decimal
    of dtString:
      stringVal: string
    of dtBytes:
      bytesVal: seq[int8]
    of dtArray:
      arrayVal: seq[DataTable]
    of dtTimestamp:
      tsVal: Time
    of dtTable:
      tableVal: TableRef[string, DataTable]
    of dtVoid:
      discard
    
const sizeInt8Uint8 = sizeof(int8)
proc readBigEndian8*(s: AsyncInputStream): Future[(int, int8)] {.async.} =
  var res: int8 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt8Uint8)
  result = (sizeInt8Uint8, res)

proc readBigEndianU8*(s: AsyncInputStream): Future[(int, uint8)] {.async.} =
  var res: uint8 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt8Uint8)
  result = (sizeInt8Uint8,res)

const sizeInt16Uint16 = sizeof(int16)
proc readBigEndian16*(s: AsyncInputStream): Future[(int, int16)] {.async.} =
  var res: int16 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt16Uint16)
  result = (sizeInt16Uint16, res)
  bigEndian16(addr result, addr result)

proc readBigEndianU16*(s: AsyncInputStream): Future[(int, uint16)] {.async.} =
  var res: uint16 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt16Uint16)
  result = (sizeInt16Uint16, res)
  bigEndian16(addr result, addr result)

const sizeInt32Uint32 = sizeof(int32)
proc readBigEndian32*(s: AsyncInputStream): Future[(int, int32)] {.async.} =
  var res: int32 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt32Uint32)
  result = (sizeInt32Uint32, res)
  bigEndian32(addr result, addr result)

proc readBigEndianU32*(s: AsyncInputStream): Future[(int, uint32)] {.async.} =
  var res: uint32 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt32Uint32)
  result = (sizeInt32Uint32, res)
  bigEndian32(addr result, addr result)

const sizeInt64Uint64 = sizeof(int64)
proc readBigEndian64*(s: AsyncInputStream): Future[(int, int64)] {.async.} =
  var res: int64 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt64Uint64)
  result = (sizeInt64Uint64, res)
  bigEndian64(addr result, addr result)

proc readBigEndianU64*(s: AsyncInputStream): Future[(int, uint64)] {.async.} =
  var res: uint64 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeInt64Uint64)
  result = (sizeInt64Uint64, res)
  bigEndian64(addr result, addr result)

const sizeFloat32 = sizeof(float32)
proc readBigEndianFloat32*(s: AsyncInputStream): Future[(int, float32)] {.async.} =
  var res: float32 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeFloat32)
  result = (sizeFloat32, res)
  #bigEndian32(addr result, addr result)

const sizeFloat64 = sizeof(float64)
proc readBigEndianFloat64*(s: AsyncInputStream): Future[(int, float64)] {.async.} =
  var res: float64 = 0
  discard await s.asyncReadIntoEx(cast[ptr byte](addr res), sizeFloat64)
  result = (sizeFloat64, res)
  #bigEndian64(addr result, addr result)

proc readShortString*(s: AsyncInputStream): Future[(int, string)] {.async.} =
  let length: int = s.read().int
  var str = newStringOfCap(length)
  discard await s.asyncReadIntoEx(cast[ptr byte](addr str), length)
  str.setLen(length)
  result = (length, str)

proc readString*(s: AsyncInputStream): Future[(int, string)] {.async.} =
  let (size, length) = await s.readBigEndianU32()
  var str = newStringOfCap(length)
  discard await s.asyncReadIntoEx(cast[ptr byte](addr str), length)
  str.setLen(length.int)
  result = (length.int+size, str)

proc decodeValue(s: AsyncInputStream): Future[(int, DataTable)] {.async.}
proc readArray*(s: AsyncInputStream): Future[(int, seq[DataTable])] {.async.} =
  var arr: seq[DataTable] = @[]
  let (size, length) = await s.readBigEndianU32()
  var cnt = 0
  while cnt < length.int:
    let (sz, val) = await s.decodeValue()
    cnt += sz
    arr.add(val)
  result = (size+length.int, arr)

proc decodeTable*(s: AsyncInputStream): Future[(int, TableRef[string, DataTable])] {.async.} =
  var res = newTable[string, DataTable]()
  let (size, tableSize) = await s.readBigEndian32()
  var off = 0.int
  while off < tableSize:
    let (ssize, key) = await s.readShortString()
    let (vsize, value) = await s.decodeValue()
    res[key] = value
    off = off + ssize + vsize
  result = (off+size, res)

proc decodeValue(s: AsyncInputStream): Future[(int, DataTable)] {.async.} =
  var (size, kind) = await s.readBigEndian8()
  case kind.char
  of 't':
    let (sz, bval) = await s.readBigEndianU8()
    size += sz
    result = (size, DataTable(dtype: dtBool, boolVal: bval.bool))
  of 'b':
    let (sz, byteVal) = await s.readBigEndian8()
    size += sz
    result = (size, DataTable(dtype: dtByte, byteVal: byteVal))
  of 'B':
    let (sz, uByteVal) = await s.readBigEndianU8()
    size += sz
    result = (size, DataTable(dtype: dtUByte, uByteVal: uByteVal))
  of 'U':
    let (sz, shortVal) = await s.readBigEndian16()
    size += sz
    result = (size, DataTable(dtype: dtShort, shortVal: shortVal))
  of 'u':
    let (sz, uShortVal) = await s.readBigEndianU16()
    size += sz
    result = (size, DataTable(dtype: dtUShort, uShortVal: uShortVal))
  of 'I':
    let (sz, intVal) = await s.readBigEndian32()
    size += sz
    result = (size, DataTable(dtype: dtInt, intVal: intVal))
  of 'i':
    let (sz, uIntVal) = await s.readBigEndianU32()
    size += sz
    result = (size, DataTable(dtype: dtUInt, uIntVal: uIntVal))
  of 'L':
    let (sz, longVal) = await s.readBigEndian64()
    size += sz
    result = (size, DataTable(dtype: dtLong, longVal: longVal))
  of 'l':
    let (sz, uLongVal) = await s.readBigEndianU64()
    size += sz
    result = (size, DataTable(dtype: dtULong, uLongVal: uLongVal))
  of 'f':
    #TODO Need to investigate endiannes
    let (sz, floatVal) = await s.readBigEndianFloat32()
    size += sz
    result = (size, DataTable(dtype: dtFloat, floatVal: floatVal))
  of 'd':
    #TODO Need to investigate endiannes
    let (sz, doubleVal) = await s.readBigEndianFloat64()
    size += sz
    result = (size, DataTable(dtype: dtDouble, doubleVal: doubleVal))
  of 'D':
    var decimalVal = newStringOfCap(DECIMAL_VAL_LENGTH)
    discard await s.asyncReadIntoEx(cast[ptr byte](addr decimalVal), DECIMAL_VAL_LENGTH)
    size += DECIMAL_VAL_LENGTH
    result = (size, DataTable(dtype: dtDecimal, decimalVal: decimalVal))
  of 's':
    let (sz, shortVal) = await s.readBigEndian16()
    size += sz
    result = (size, DataTable(dtype: dtSignedShort, shortVal: shortVal))
  of 'S':
    let (sz, stringVal) = await s.readString()
    size += sz
    result = (size, DataTable(dtype: dtString, stringVal: stringVal))
  of 'x':
    let res = DataTable(dtype: dtBytes)
    let (sz, length) = await s.readBigEndianU32()
    res.bytesVal.setLen(length)
    discard await s.asyncReadIntoEx(cast[ptr byte](addr res.bytesVal[0]), length)
    result = (size+sz+length.int, res)
  of 'A':
    let (sz, arrayVal)= await s.readArray()
    size += sz
    result = (size, DataTable(dtype: dtArray, arrayVal: arrayVal))
  of 'T':
    let (sz, ts) = await s.readBigEndian64()
    size += sz
    result = (size, DataTable(dtype: dtTimestamp, tsVal: fromUnix(ts)))
  of 'F':
    let (sz, tableVal) = await s.decodeTable()
    size += sz
    result = (size, DataTable(dtype: dtTable, tableVal: tableVal))
  of 'V':
    result = (size, DataTable(dtype: dtVoid))
  else:
    raise newException(InvalidFieldTypeException, "Unknown field type: " & $kind)

proc writeBigEndian8*(s: AsyncOutputStream, x: int8 | uint8): Future[AsyncOutputStream] {.discardable, async.} =
  await s.asyncWriteBytes(cast[ptr byte](unsafeAddr x), sizeInt8Uint8)
  result = s

proc writeBigEndian16*(s: AsyncOutputStream, x: int16 | uint16): Future[AsyncOutputStream] {.discardable, async.} =
  var n = x
  bigEndian16(addr n, addr n)
  await s.asyncWriteBytes(cast[ptr byte](addr n), sizeInt16Uint16)
  result = s

proc writeBigEndian32*(s: AsyncOutputStream, x: int32 | uint32): Future[AsyncOutputStream] {.discardable, async.} =
  var n = x
  bigEndian32(addr n, addr n)
  await s.asyncWriteBytes(cast[ptr byte](addr n), sizeInt32Uint32)
  result = s

proc writeBigEndian64*(s: AsyncOutputStream, x: int64 | uint64): Future[AsyncOutputStream] {.discardable, async.} =
  var n = x
  bigEndian64(addr n, addr n)
  await s.asyncWriteBytes(cast[ptr byte](addr n), sizeInt64Uint64)
  result = s

proc writeFloat32*(s: AsyncOutputStream, x: float32): Future[AsyncOutputStream] {.discardable, async.} =
  var n = x
  #TODO Need to investigate endiannes
  #bigEndian32(addr n, addr n)
  await s.asyncWriteBytes(cast[ptr byte](addr n), sizeFloat32)
  result = s

proc writeFloat64*(s: AsyncOutputStream, x: float64): Future[AsyncOutputStream] {.discardable, async.} =
  var n = x
  #TODO Need to investigate endiannes
  #bigEndian64(addr n, addr n)
  await s.asyncWriteBytes(cast[ptr byte](addr n), sizeFloat64)
  result = s

proc writeShortString*(s: AsyncOutputStream, str: string): Future[AsyncOutputStream] {.discardable, async.} =
  let slen = str.len()
  if slen > int8.high():
    raise newException(ValueError, "Wrong string size.")
  s.writeBigEndian8(slen.int8)
  await s.asyncWriteBytes(cast[ptr byte](unsafeAddr s), slen)
  result = s

proc writeString*(s: AsyncOutputStream, str: string): Future[AsyncOutputStream] {.discardable, async.} =
  let slen = str.len()
  s.writeBigEndian32(slen.uint32)
  await s.asyncWriteBytes(cast[ptr byte](unsafeAddr s), slen)
  result = s

proc encodeValue(s: AsyncOutputStream, data: DataTable) {.async.}
proc writeArray*(s: AsyncOutputStream, arr: seq[DataTable]): Future[AsyncOutputStream] {.discardable, async.} =
  let tmpStream = memoryOutput()
  let asyncTmpStream = AsyncOutputStream(tmpStream.s)
  for a in arr:
    await asyncTmpStream.encodeValue(a)
  let output: seq[byte] = asyncTmpStream.getOutput()
  discard await s.writeBigEndian32(output.len.uint32)
  await s.asyncWriteBytes(cast[ptr byte](unsafeAddr output[0]), output.len)

proc encodeTable*(s: AsyncOutputStream, data: TableRef[string, DataTable]): Future[AsyncOutputStream] {.discardable, async.} =
  for k, v in data:
    s.writeShortString(k)
    await s.encodeValue(v)

proc encodeValue(s: AsyncOutputStream, data: DataTable) {.async.} =
  case data.dtype
  of dtBool:
    discard await s.writeBigEndian8('t'.uint8)
    discard await s.writeBigEndian8(data.boolVal.uint8)
  of dtByte:
    discard await s.writeBigEndian8('b'.uint8)
    discard await s.writeBigEndian8(data.byteVal)
  of dtUByte:
    discard await s.writeBigEndian8('B'.uint8)
    discard await s.writeBigEndian8(data.uByteVal)
  of dtShort:
    discard await s.writeBigEndian8('U'.uint8)
    discard await s.writeBigEndian16(data.shortVal)
  of dtUShort:
    discard await s.writeBigEndian8('u'.uint8)
    discard await s.writeBigEndian16(data.uShortVal)
  of dtInt:
    discard await s.writeBigEndian8('I'.uint8)
    discard await s.writeBigEndian32(data.intVal)
  of dtUInt:
    discard await s.writeBigEndian8('i'.uint8)
    discard await s.writeBigEndian32(data.uIntVal)
  of dtLong:
    discard await s.writeBigEndian8('L'.uint8)
    discard await s.writeBigEndian64(data.longVal)
  of dtULong:
    discard await s.writeBigEndian8('l'.uint8)
    discard await s.writeBigEndian64(data.uLongVal)
  of dtFloat:
    discard await s.writeBigEndian8('f'.uint8)
    discard await s.writeFloat32(data.floatVal)
  of dtDouble:
    discard await s.writeBigEndian8('d'.uint8)
    discard await s.writeFloat64(data.doubleVal)
  of dtDecimal:
    discard await s.writeBigEndian8('D'.uint8)
    await s.asyncWriteBytes(cast[ptr byte](addr data.decimalVal), DECIMAL_VAL_LENGTH)
  of dtSignedShort:
    discard await s.writeBigEndian8('s'.uint8)
    discard await s.writeBigEndian16(data.shortVal)
  of dtString:
    discard await s.writeBigEndian8('S'.uint8)
    discard await s.writeString(data.stringVal)
  of dtBytes:
    discard await s.writeBigEndian8('x'.uint8)
    discard await s.writeBigEndian32(data.bytesVal.len.uint32)
    await s.asyncWriteBytes(cast[ptr byte](addr data.bytesVal[0]), data.bytesVal.len)
  of dtArray:
    discard await s.writeBigEndian8('A'.uint8)
    discard await s.writeArray(data.arrayVal)
  of dtTimestamp:
    discard await s.writeBigEndian8('T'.uint8)
    discard await s.writeBigEndian64(data.tsVal.toUnix())
  of dtTable:
    discard await s.writeBigEndian8('F'.uint8)
    discard await s.encodeTable(data.tableVal)    
  of dtVoid:
    discard await s.writeBigEndian8('V'.uint8)
