import ./submethods
import ../data
import ../streams

const CONFIRM_METHODS* = 0x0055.uint16
const CONFIRM_SELECT_METHOD_ID = 0x0055000A.uint32
const CONFIRM_SELECT_OK_METHOD_ID = 0x0055000B.uint32

type
  ConfirmVariants* = enum
    NONE = 0
    CONFIRM_SELECT_METHOD = (CONFIRM_SELECT_METHOD_ID and 0x0000FFFF).uint16
    CONFIRM_SELECT_OK_METHOD = (CONFIRM_SELECT_OK_METHOD_ID and 0x0000FFFF).uint16

type 
  ConfirmMethod* = ref object of SubMethod
    case indexLo*: ConfirmVariants
    of CONFIRM_SELECT_METHOD:
      noWait*: bool
    of CONFIRM_SELECT_OK_METHOD:
      discard
    else:
      discard

proc decodeConfirmSelect(encoded: InputStream): (bool, seq[uint16], ConfirmMethod)
proc encodeConfirmSelect(to: OutputStream, data: ConfirmMethod)
proc decodeConfirmSelectOk(encoded: InputStream): (bool, seq[uint16], ConfirmMethod)
proc encodeConfirmSelectOk(to: OutputStream, data: ConfirmMethod)

proc decode*(_: type[ConfirmMethod], submethodId: ConfirmVariants, encoded: InputStream): (bool, seq[uint16], ConfirmMethod) =
  case submethodId
  of CONFIRM_SELECT_METHOD:
    result = decodeConfirmSelect(encoded)
  of CONFIRM_SELECT_OK_METHOD:
    result = decodeConfirmSelectOk(encoded)
  else:
      discard

proc encode*(to: OutputStream, data: ConfirmMethod) =
  case data.indexLo
  of CONFIRM_SELECT_METHOD:
    to.encodeConfirmSelect(data)
  of CONFIRM_SELECT_OK_METHOD:
    to.encodeConfirmSelectOk(data)
  else:
    discard

#--------------- Confirm.Select ---------------#

proc newConfirmSelect*(noWait=false): (bool, seq[uint16], ConfirmMethod) =
  var res = ConfirmMethod(indexLo: CONFIRM_SELECT_METHOD)
  res.noWait = noWait
  result = (true, @[ord(CONFIRM_SELECT_OK_METHOD).uint16], res)

proc decodeConfirmSelect(encoded: InputStream): (bool, seq[uint16], ConfirmMethod) =
  let (_, bbuf) = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newConfirmSelect(noWait)

proc encodeConfirmSelect(to: OutputStream, data: ConfirmMethod) =
  let bbuf = (if data.noWait: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Confirm.SelectOk ---------------#

proc newConfirmSelectOk*(): (bool, seq[uint16], ConfirmMethod) =
  result = (false, @[], ConfirmMethod(indexLo: CONFIRM_SELECT_OK_METHOD))

proc decodeConfirmSelectOk(encoded: InputStream): (bool, seq[uint16], ConfirmMethod) = newConfirmSelectOk()

proc encodeConfirmSelectOk(to: OutputStream, data: ConfirmMethod) = discard
