import streams
import ./mthd

type 
  TxSelect* = ref object of Method
  TxSelectOk* = ref object of Method
  TxCommit* = ref object of Method
  TxCommitOk* = ref object of Method
  TxRollback* = ref object of Method
  TxRollbackOk* = ref object of Method

#--------------- Tx.Select ---------------#

proc newTxSelect*(): TxSelect =
  result.new
  result.initMethod(true, 0x005A000A)

proc decode*(_: type[TxSelect], encoded: Stream): TxSelect = newTxSelect()

proc encode*(self: TxSelect, to: Stream) = discard

#--------------- Tx.SelectOk ---------------#

proc newTxSelectOk*(): TxSelectOk =
  result.new
  result.initMethod(false, 0x005A000B)

proc decode*(_: type[TxSelectOk], encoded: Stream): TxSelectOk = newTxSelectOk()

proc encode*(self: TxSelectOk, to: Stream) = discard

#--------------- Tx.Commit ---------------#

proc newTxCommit*(): TxCommit =
  result.new
  result.initMethod(true, 0x005A0014)

proc decode*(_: type[TxCommit], encoded: Stream): TxCommit = newTxCommit()

proc encode*(self: TxCommit, to: Stream) = discard

#--------------- Tx.CommitOk ---------------#

proc newTxCommitOk*(): TxCommitOk =
  result.new
  result.initMethod(false, 0x005A0015)

proc decode*(_: type[TxCommitOk], encoded: Stream): TxCommitOk = newTxCommitOk()

proc encode*(self: TxCommitOk, to: Stream) = discard

#--------------- Tx.Rollback ---------------#

proc newTxRollback*(): TxRollback =
  result.new
  result.initMethod(true, 0x005A001E)

proc decode*(_: type[TxRollback], encoded: Stream): TxRollback = newTxRollback()

proc encode*(self: TxRollback, to: Stream) = discard

#--------------- Tx.RollbackOk ---------------#

proc newTxRollbackOk*(): TxRollbackOk =
  result.new
  result.initMethod(false, 0x005A001F)

proc decode*(_: type[TxRollbackOk], encoded: Stream): TxRollbackOk = newTxRollbackOk()

proc encode*(self: TxRollbackOk, to: Stream) = discard
