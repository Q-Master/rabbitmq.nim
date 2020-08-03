import streams
import endians
import times
import tables
import ./exceptions

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
    
proc readBigEndian8*(s: Stream): int8 =
  result = s.readInt8

proc readBigEndianU8*(s: Stream): uint8 =
  result = s.readUint8

proc readBigEndian16*(s: Stream): int16 =
  result = s.readInt16
  bigEndian16(addr result, addr result)

proc readBigEndianU16*(s: Stream): uint16 =
  result = s.readUInt16
  bigEndian16(addr result, addr result)

proc readBigEndian32*(s: Stream): int32 =
  result = s.readInt32
  bigEndian32(addr result, addr result)

proc readBigEndianU32*(s: Stream): uint32 =
  result = s.readUInt32
  bigEndian32(addr result, addr result)

proc readBigEndian64*(s: Stream): int64 =
  result = s.readInt64
  bigEndian64(addr result, addr result)

proc readBigEndianU64*(s: Stream): uint64 =
  result = s.readUInt64
  bigEndian64(addr result, addr result)

proc readShortString*(s: Stream): string =
  let length = s.readBigEndian8()
  return s.readStr(length)

proc readString*(s: Stream): string =
  let length = s.readBigEndianU32().int
  return s.readStr(length)

proc writeBigEndian8*(s: Stream, x: int8 | uint8): Stream {.discardable.} =
  s.write(x)
  result = s

proc writeBigEndian16*(s: Stream, x: int16 | uint16): Stream {.discardable.} =
  var n = x
  bigEndian16(addr n, addr n)
  s.write(n)
  result = s

proc writeBigEndian32*(s: Stream, x: int32 | uint32): Stream {.discardable.} =
  var n = x
  bigEndian32(addr n, addr n)
  s.write(n)
  result = s

proc writeBigEndian64*(s: Stream, x: int64 | uint64): Stream {.discardable.} =
  var n = x
  bigEndian64(addr n, addr n)
  s.write(n)
  result = s

proc writeShortString*(s: Stream, str: string): Stream {.discardable.} =
  let slen = str.len()
  if slen > int8.high():
    raise newException(ValueError, "Wrong string size.")
  s.writeBigEndian8(slen.int8)
  s.write(str)
  result = s

proc writeString*(s: Stream, str: string): Stream {.discardable.} =
  s.writeBigEndian32(str.len().uint32)
  s.write(str)
  result = s

proc decodeValue(s: Stream): DataTable
proc decodeTable*(s: Stream): TableRef[string, DataTable] =
  result = newTable[string, DataTable]()
  let offset = s.getPosition()
  let tableSize = readBigEndian32(s)
  while s.getPosition() < offset+tableSize:
    let key = s.readShortString()
    let value = s.decodeValue()
    result[key] = value

proc decodeValue(s: Stream): DataTable =
  let kind = s.readChar()
  case kind
  of 't':
    result = DataTable(dtype: dtBool)
    result.boolVal = s.readBigEndianU8().bool
  of 'b':
    result = DataTable(dtype: dtByte)
    result.byteVal = s.readBigEndian8()
  of 'B':
    result = DataTable(dtype: dtUByte)
    result.uByteVal = s.readBigEndianU8()
  of 'U':
    result = DataTable(dtype: dtShort)
    result.shortVal = s.readBigEndian16()
  of 'u':
    result = DataTable(dtype: dtUShort)
    result.uShortVal = s.readBigEndianU16()
  of 'I':
    result = DataTable(dtype: dtInt)
    result.intVal = s.readBigEndian32()
  of 'i':
    result = DataTable(dtype: dtUInt)
    result.uIntVal = s.readBigEndianU32()
  of 'L':
    result = DataTable(dtype: dtLong)
    result.longVal = s.readBigEndian64()
  of 'l':
    result = DataTable(dtype: dtULong)
    result.uLongVal = s.readBigEndianU64()  
  of 'f':
    result = DataTable(dtype: dtFloat)
    #TODO Need to investigate endiannes
    result.floatVal = s.readFloat32()
  of 'd':
    result = DataTable(dtype: dtDouble)
    #TODO Need to investigate endiannes
    result.doubleVal = s.readFloat64()
  of 'D':
    result = DataTable(dtype: dtDecimal)
    result.decimalVal = s.readStr(5)
  of 's':
    result = DataTable(dtype: dtSignedShort)
    result.shortVal = s.readBigEndian16()
  of 'S':
    result = DataTable(dtype: dtString)
    let length = s.readBigEndianU32().int
    result.stringVal = s.readStr(length)
  of 'x':
    result = DataTable(dtype: dtBytes)
    var length = s.readBigEndianU32().int
    while length > 0:
      result.bytesVal.add(s.readBigEndian8())
      length -= 1
  of 'A':
    result = DataTable(dtype: dtArray)
    let length = s.readBigEndianU32().int
    let offset = s.getPosition()
    result.arrayVal = @[]
    while s.getPosition() <  offset+length:
      result.arrayVal.add(s.decodeValue())
  of 'T':
    result = DataTable(dtype: dtTimestamp)
    let ts = s.readBigEndian64()
    result.tsVal = fromUnix(ts)
  of 'F':
    result = DataTable(dtype: dtTable)
    result.tableVal = decodeTable(s)
  of 'V':
    result = DataTable(dtype: dtVoid)
  else:
    raise newException(InvalidFieldTypeException, "Unknown field type: " & $kind)

proc encodeValue(s: Stream, data: DataTable)
proc encodeTable*(s: Stream, data: TableRef[string, DataTable]): Stream {.discardable.} =
  for k, v in data:
    s.writeShortString(k)
    s.encodeValue(v)

proc encodeValue(s: Stream, data: DataTable) =
  case data.dtype
  of dtBool:
    s.write('t')
    s.writeBigEndian8(data.boolVal.uint8)
  of dtByte:
    s.write('b')
    s.writeBigEndian8(data.byteVal)
  of dtUByte:
    s.write('B')
    s.writeBigEndian8(data.uByteVal)
  of dtShort:
    s.write('U')
    s.writeBigEndian16(data.shortVal)
  of dtUShort:
    s.write('u')
    s.writeBigEndian16(data.uShortVal)
  of dtInt:
    s.write('I')
    s.writeBigEndian32(data.intVal)
  of dtUInt:
    s.write('i')
    s.writeBigEndian32(data.uIntVal)
  of dtLong:
    s.write('L')
    s.writeBigEndian64(data.longVal)
  of dtULong:
    s.write('l')
    s.writeBigEndian64(data.uLongVal)
  of dtFloat:
    s.write('f')
    #TODO Need to investigate endiannes
    s.write(data.floatVal)
  of dtDouble:
    s.write('d')
    #TODO Need to investigate endiannes
    s.write(data.doubleVal)
  of dtDecimal:
    s.write('D')
    s.write(data.decimalVal)
  of dtSignedShort:
    s.write('s')
    s.writeBigEndian16(data.shortVal)
  of dtString:
    s.write('S')
    s.writeBigEndian32(data.stringVal.len.uint32)
    s.write(data.stringVal)
  of dtBytes:
    s.write('x')
    s.writeBigEndian32(data.bytesVal.len.uint32)
    for x in data.bytesVal:
      s.writeBigEndian8(x)
  of dtArray:
    s.write('A')
    s.writeBigEndian32(data.arrayVal.len.uint32)
    for a in data.arrayVal:
      s.encodeValue(a)
  of dtTimestamp:
    s.write('T')
    s.writeBigEndian64(data.tsVal.toUnix())
  of dtTable:
    s.write('F')
    s.encodeTable(data.tableVal)    
  of dtVoid:
    s.write('V')
