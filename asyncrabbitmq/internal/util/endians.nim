type
  U32t2U16* {.union.} = object
    unsigned32: uint32
    unsigned16: array[2, uint16]

proc uint32touints16*(source: uint32): (uint16, uint16) =
  let src: U32t2U16 = U32t2U16(unsigned32: source)
  when system.cpuEndian == bigEndian:
    result = (src.unsigned16[0], src.unsigned16[1])
  else:
    result = (src.unsigned16[1], src.unsigned16[0])

proc uints16touint32*(srcHi: uint16, srcLo: uint16): uint32 =
  when system.cpuEndian == bigEndian:
    let src: U32t2U16 = U32t2U16(unsigned16: [srcHi, srcLo])
  else:
    let src: U32t2U16 = U32t2U16(unsigned16: [srcLo, srcHi])
  result = src.unsigned32