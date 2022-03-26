import std/[times, tables, asyncdispatch]
import pkg/networkutils/buffered_socket
import ./exceptions
import ./spec

type
  Decimal = string
  FieldType* = enum
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

  U32t2U16* {.union.} = object
    unsigned32: uint32
    unsigned16: array[2, uint16]

  FieldTable* = TableRef[string, Field]

  Field* = ref FieldObj
  FieldObj* = object
    case kind*: FieldType
    of dtBool:
      boolVal*: bool
    of dtByte:
      byteVal*: int8
    of dtUByte:
      uByteVal*: uint8
    of dtShort, dtSignedShort:
      shortVal*: int16
    of dtUShort:
      uShortVal*: uint16
    of dtInt:
      intVal*: int32
    of dtUInt:
      uIntVal*: uint32
    of dtLong:
      longVal*: int64
    of dtULong:
      uLongVal*: uint64
    of dtFloat:
      floatVal*: float32
    of dtDouble:
      doubleVal*: float64
    of dtDecimal:
      decimalVal*: Decimal
    of dtString:
      stringVal*: string
    of dtBytes:
      bytesVal*: seq[byte]
    of dtArray:
      arrayVal*: seq[Field]
    of dtTimestamp:
      tsVal*: Time
    of dtTable:
      tableVal*: TableRef[string, Field]
    of dtVoid:
      discard

proc len*(f: Field): int =
  result = 0
  case f.kind
  of dtBool, dtByte, dtUByte:
    result.inc(sizeInt8Uint8)
  of dtShort, dtSignedShort, dtUShort:
    result.inc(sizeInt16Uint16)
  of dtInt, dtUInt:
    result.inc(sizeInt32Uint32)
  of dtLong, dtULong, dtTimestamp:
    result.inc(sizeInt64Uint64)
  of dtFloat:
    result.inc(sizeFloat32)
  of dtDouble:
    result.inc(sizeFloat64)
  of dtDecimal:
    result.inc(DECIMAL_VAL_LENGTH)
  of dtString:
    result.inc(f.stringVal.len() + sizeInt32Uint32)
  of dtBytes:
    result.inc(f.bytesVal.len() + sizeInt32Uint32)
  of dtArray:
    result.inc(sizeInt32Uint32)
    for field in f.arrayVal:
      result.inc(field.len() + sizeInt8Uint8)
  of dtTable:
    result.inc(sizeInt32Uint32)
    for k,v in f.tableVal.pairs():
      result.inc(k.len() + sizeInt8Uint8)
      result.inc(v.len() + sizeInt8Uint8)
  of dtVoid:
    result.inc(0)

proc len*(f: FieldTable): int =
  result.inc(sizeInt32Uint32)
  for k,v in f.pairs():
    result.inc(k.len() + sizeInt8Uint8)
    result.inc(v.len() + sizeInt8Uint8)

converter asField*(x: int8): Field =
  result = Field(kind: dtByte, byteVal: x)

converter asField*(x: uint8): Field =
  result = Field(kind: dtUByte, uByteVal: x)

converter asField*(x: int16): Field =
  result = Field(kind: dtShort, shortVal: x)

converter asField*(x: uint16): Field =
  result = Field(kind: dtUShort, uShortVal: x)

converter asField*(x: int32): Field =
  result = Field(kind: dtInt, intVal: x)

converter asField*(x: uint32): Field =
  result = Field(kind: dtUInt, uIntVal: x)

converter asField*(x: int64): Field =
  result = Field(kind: dtLong, longVal: x)

converter asField*(x: uint64): Field =
  result = Field(kind: dtULong, uLongVal: x)

converter asField*(x: float32): Field =
  result = Field(kind: dtFloat, floatVal: x)

converter asField*(x: float64): Field =
  result = Field(kind: dtDouble, doubleVal: x)

converter asField*(x: string): Field =
  result = Field(kind: dtString, stringVal: x)

converter asField*(x: bool): Field =
  result = Field(kind: dtBool, boolVal: x)

converter asField*(x: FieldTable): Field =
  result = Field(kind: dtTable, tableVal: x)

proc decodeField(s: AsyncBufferedSocket): Future[Field] {.async.}
proc decodeArray*(s: AsyncBufferedSocket): Future[seq[Field]] {.async.} =
  var arr: seq[Field] = @[]
  let length = await s.readBE32()
  var cnt = 0
  while cnt < length.int:
    let val = await s.decodeField()
    arr.add(val)
  result = arr

proc decodeShortString*(s: AsyncBufferedSocket): Future[string] {.async.} =
  let size = await s.read8()
  result = await s.readString(size)

proc decodeString*(s: AsyncBufferedSocket): Future[string] {.async.} =
  let size = await s.readBEU32()
  result = await s.readString(size.int)

proc decodeTable*(s: AsyncBufferedSocket): Future[TableRef[string, Field]] {.async.} =
  result = newTable[string, Field]()
  var bytesToRead = await s.readBEU32()
  while bytesToRead > 0:
    let key = await s.decodeShortString()
    bytesToRead.dec(key.len() + sizeInt8Uint8)
    let value = await s.decodeField()
    result[key] = value
    bytesToRead.dec(value.len()+sizeInt8Uint8)

proc decodeField(s: AsyncBufferedSocket): Future[Field] {.async.} =
  var kind = await s.readU8()
  case kind.char
  of 't':
    let bval = await s.readU8()
    result = Field(kind: dtBool, boolVal: bval.bool)
  of 'b':
    let byteVal = await s.read8()
    result = Field(kind: dtByte, byteVal: byteVal)
  of 'B':
    let uByteVal = await s.readU8()
    result = Field(kind: dtUByte, uByteVal: uByteVal)
  of 'U':
    let shortVal = await s.readBE16()
    result = Field(kind: dtShort, shortVal: shortVal)
  of 'u':
    let uShortVal = await s.readBEU16()
    result = Field(kind: dtUShort, uShortVal: uShortVal)
  of 'I':
    let intVal = await s.readBE32()
    result = Field(kind: dtInt, intVal: intVal)
  of 'i':
    let uIntVal = await s.readBEU32()
    result = Field(kind: dtUInt, uIntVal: uIntVal)
  of 'L':
    let longVal = await s.readBE64()
    result = Field(kind: dtLong, longVal: longVal)
  of 'l':
    let uLongVal = await s.readBEU64()
    result = Field(kind: dtULong, uLongVal: uLongVal)
  of 'f':
    let floatVal = await s.readFloat32()
    result = Field(kind: dtFloat, floatVal: floatVal)
  of 'd':
    let doubleVal = await s.readFloat64()
    result = Field(kind: dtDouble, doubleVal: doubleVal)
  of 'D':
    var decimalVal = newString(DECIMAL_VAL_LENGTH)
    await s.recvInto(cast[ptr byte](addr decimalVal), DECIMAL_VAL_LENGTH)
    result = Field(kind: dtDecimal, decimalVal: decimalVal)
  of 's':
    let shortVal = await s.readBE16()
    result = Field(kind: dtSignedShort, shortVal: shortVal)
  of 'S':
    let stringVal = await s.decodeString()
    result = Field(kind: dtString, stringVal: stringVal)
  of 'x':
    result = Field(kind: dtBytes)
    let length = await s.readBEU32()
    result.bytesVal.setLen(length)
    await s.recvInto(result.bytesVal[0].addr, length.int32)
  of 'A':
    let arrayVal= await s.decodeArray()
    result = Field(kind: dtArray, arrayVal: arrayVal)
  of 'T':
    let ts = await s.readBE64()
    result = Field(kind: dtTimestamp, tsVal: fromUnix(ts))
  of 'F':
    let tableVal = await s.decodeTable()
    result = Field(kind: dtTable, tableVal: tableVal)
  of 'V':
    result = Field(kind: dtVoid)
  else:
    raise newException(InvalidFieldTypeException, "Unknown field type: " & $kind)

#----------------------------------------------------------------------------------#

proc encodeField(s: AsyncBufferedSocket, data: Field) {.async.}
proc encodeArray*(s: AsyncBufferedSocket, arr: Field) {.async.} =
  await s.write(arr.len().uint32 - sizeInt32Uint32.uint32)
  for elem in arr.arrayVal:
    await s.encodeField(elem)

proc encodeShortString*(s: AsyncBufferedSocket, str: string) {.async.} =
  await s.write(str.len().uint8)
  await s.writeString(str)

proc encodeString*(s: AsyncBufferedSocket, str: string) {.async.} =
  await s.write(str.len().uint32)
  await s.writeString(str)

proc encodeTable*(s: AsyncBufferedSocket, table: Field) {.async.} =
  await s.writeBE(table.len().uint32 - sizeInt32Uint32.uint32)
  for k, v in table.tableVal.pairs():
    await s.encodeShortString(k)
    await s.encodeField(v)

proc encodeField(s: AsyncBufferedSocket, data: Field) {.async.} =
  case data.kind
  of dtBool:
    await s.write('t'.uint8)
    await s.write(data.boolVal.uint8)
  of dtByte:
    await s.write('b'.uint8)
    await s.write(data.byteVal)
  of dtUByte:
    await s.write('B'.uint8)
    await s.write(data.uByteVal)
  of dtShort:
    await s.write('U'.uint8)
    await s.writeBE(data.shortVal)
  of dtUShort:
    await s.write('u'.uint8)
    await s.writeBE(data.uShortVal)
  of dtInt:
    await s.write('I'.uint8)
    await s.writeBE(data.intVal)
  of dtUInt:
    await s.write('i'.uint8)
    await s.writeBE(data.uIntVal)
  of dtLong:
    await s.write('L'.uint8)
    await s.writeBE(data.longVal)
  of dtULong:
    await s.write('l'.uint8)
    await s.writeBE(data.uLongVal)
  of dtFloat:
    await s.write('f'.uint8)
    await s.write(data.floatVal)
  of dtDouble:
    await s.write('d'.uint8)
    await s.write(data.doubleVal)
  of dtDecimal:
    await s.write('D'.uint8)
    await s.writeString(data.decimalVal)
  of dtSignedShort:
    await s.write('s'.uint8)
    await s.writeBE(data.shortVal)
  of dtString:
    await s.write('S'.uint8)
    await s.writeString(data.stringVal)
  of dtBytes:
    await s.write('x'.uint8)
    await s.writeBE(data.bytesVal.len.uint32)
    let x {.used.} = await s.send(data.bytesVal)
  of dtArray:
    await s.write('A'.uint8)
    await s.encodeArray(data)
  of dtTimestamp:
    await s.write('T'.uint8)
    await s.writeBE(data.tsVal.toUnix())
  of dtTable:
    await s.write('F'.uint8)
    await s.encodeTable(data)    
  of dtVoid:
    await s.write('V'.uint8)
