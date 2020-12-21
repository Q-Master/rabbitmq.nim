import ./submethods
import ../streams

const TX_METHODS* = 0x005A.uint16
const TX_SELECT_METHOD_ID = 0x005A000A.uint32
const TX_SELECT_OK_METHOD_ID = 0x005A000B.uint32
const TX_COMMIT_METHOD_ID = 0x005A0014.uint32
const TX_COMMIT_OK_METHOD_ID = 0x005A0015.uint32
const TX_ROLLBACK_METHOD_ID = 0x005A001E.uint32
const TX_ROLLBACK_OK_METHOD_ID = 0x005A001F.uint32

type
  TxVariants* = enum
    NONE = 0
    TX_SELECT_METHOD = (TX_SELECT_METHOD_ID and 0x0000FFFF).uint16
    TX_SELECT_OK_METHOD = (TX_SELECT_OK_METHOD_ID and 0x0000FFFF).uint16
    TX_COMMIT_METHOD = (TX_COMMIT_METHOD_ID and 0x0000FFFF).uint16
    TX_COMMIT_OK_METHOD = (TX_COMMIT_OK_METHOD_ID and 0x0000FFFF).uint16
    TX_ROLLBACK_METHOD = (TX_ROLLBACK_METHOD_ID and 0x0000FFFF).uint16
    TX_ROLLBACK_OK_METHOD = (TX_ROLLBACK_OK_METHOD_ID and 0x0000FFFF).uint16

type 
  TxMethod* = ref object of SubMethod
    case indexLo*: TxVariants
    of TX_SELECT_METHOD, TX_SELECT_OK_METHOD:
      discard
    of TX_COMMIT_METHOD, TX_COMMIT_OK_METHOD:
      discard
    of TX_ROLLBACK_METHOD, TX_ROLLBACK_OK_METHOD:
      discard
    else:
      discard

proc decodeTxSelect(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxSelect(to: OutputStream, data: TxMethod)
proc decodeTxSelectOk(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxSelectOk(to: OutputStream, data: TxMethod)
proc decodeTxCommit(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxCommit(to: OutputStream, data: TxMethod)
proc decodeTxCommitOk(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxCommitOk(to: OutputStream, data: TxMethod)
proc decodeTxRollback(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxRollback(to: OutputStream, data: TxMethod)
proc decodeTxRollbackOk(encoded: InputStream): (bool, seq[uint16], TxMethod)
proc encodeTxRollbackOk(to: OutputStream, data: TxMethod)

proc decode*(_ : type[TxMethod], submethodId: TxVariants, encoded: InputStream): (bool, seq[uint16], TxMethod) =
  case submethodId
  of TX_SELECT_METHOD:
    result = decodeTxSelect(encoded)
  of TX_SELECT_OK_METHOD:
    result = decodeTxSelectOk(encoded)
  of TX_COMMIT_METHOD:
    result = decodeTxCommit(encoded)
  of TX_COMMIT_OK_METHOD:
    result = decodeTxCommitOk(encoded)
  of TX_ROLLBACK_METHOD:
    result = decodeTxRollback(encoded)
  of TX_ROLLBACK_OK_METHOD:
    result = decodeTxRollbackOk(encoded)
  else:
      discard

proc encode*(to: OutputStream, data: TxMethod) =
  case data.indexLo
  of TX_SELECT_METHOD:
    to.encodeTxSelect(data)
  of TX_SELECT_OK_METHOD:
    to.encodeTxSelectOk(data)
  of TX_COMMIT_METHOD:
    to.encodeTxCommit(data)
  of TX_COMMIT_OK_METHOD:
    to.encodeTxCommitOk(data)
  of TX_ROLLBACK_METHOD:
    to.encodeTxRollback(data)
  of TX_ROLLBACK_OK_METHOD:
    to.encodeTxRollbackOk(data)
  else:
    discard
#--------------- Tx.Select ---------------#

proc newTxSelect*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[ord(TX_SELECT_OK_METHOD).uint16], TxMethod(indexLo: TX_SELECT_METHOD))

proc decodeTxSelect(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxSelect()

proc encodeTxSelect(to: OutputStream, data: TxMethod) = discard

#--------------- Tx.SelectOk ---------------#

proc newTxSelectOk*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[], TxMethod(indexLo: TX_SELECT_OK_METHOD))

proc decodeTxSelectOk(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxSelectOk()

proc encodeTxSelectOk(to: OutputStream, data: TxMethod) = discard

#--------------- Tx.Commit ---------------#

proc newTxCommit*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[ord(TX_COMMIT_OK_METHOD).uint16], TxMethod(indexLo: TX_COMMIT_METHOD))

proc decodeTxCommit(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxCommit()

proc encodeTxCommit(to: OutputStream, data: TxMethod) = discard

#--------------- Tx.CommitOk ---------------#

proc newTxCommitOk*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[], TxMethod(indexLo: TX_COMMIT_OK_METHOD))

proc decodeTxCommitOk(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxCommitOk()

proc encodeTxCommitOk(to: OutputStream, data: TxMethod) = discard

#--------------- Tx.Rollback ---------------#

proc newTxRollback*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[ord(TX_ROLLBACK_OK_METHOD).uint16], TxMethod(indexLo: TX_ROLLBACK_METHOD))

proc decodeTxRollback(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxRollback()

proc encodeTxRollback(to: OutputStream, data: TxMethod) = discard

#--------------- Tx.RollbackOk ---------------#

proc newTxRollbackOk*(): (bool, seq[uint16], TxMethod) =
  result = (true, @[], TxMethod(indexLo: TX_ROLLBACK_OK_METHOD))

proc decodeTxRollbackOk(encoded: InputStream): (bool, seq[uint16], TxMethod) = newTxRollbackOk()

proc encodeTxRollbackOk(to: OutputStream, data: TxMethod) = discard
