import asyncdispatch
import faststreams/[inputs, outputs]
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

proc decode*(_: type[TxSelect], encoded: AsyncInputStream): Future[TxSelect] {.async.} = newTxSelect()

proc encode*(self: TxSelect, to: AsyncOutputStream) {.async.} = discard

#--------------- Tx.SelectOk ---------------#

proc newTxSelectOk*(): TxSelectOk =
  result.new
  result.initMethod(false, 0x005A000B)

proc decode*(_: type[TxSelectOk], encoded: AsyncInputStream): Future[TxSelectOk] {.async.} = newTxSelectOk()

proc encode*(self: TxSelectOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Tx.Commit ---------------#

proc newTxCommit*(): TxCommit =
  result.new
  result.initMethod(true, 0x005A0014)

proc decode*(_: type[TxCommit], encoded: AsyncInputStream): Future[TxCommit] {.async.} = newTxCommit()

proc encode*(self: TxCommit, to: AsyncOutputStream) {.async.} = discard

#--------------- Tx.CommitOk ---------------#

proc newTxCommitOk*(): TxCommitOk =
  result.new
  result.initMethod(false, 0x005A0015)

proc decode*(_: type[TxCommitOk], encoded: AsyncInputStream): Future[TxCommitOk] {.async.} = newTxCommitOk()

proc encode*(self: TxCommitOk, to: AsyncOutputStream) {.async.} = discard

#--------------- Tx.Rollback ---------------#

proc newTxRollback*(): TxRollback =
  result.new
  result.initMethod(true, 0x005A001E)

proc decode*(_: type[TxRollback], encoded: AsyncInputStream): Future[TxRollback] {.async.} = newTxRollback()

proc encode*(self: TxRollback, to: AsyncOutputStream) {.async.} = discard

#--------------- Tx.RollbackOk ---------------#

proc newTxRollbackOk*(): TxRollbackOk =
  result.new
  result.initMethod(false, 0x005A001F)

proc decode*(_: type[TxRollbackOk], encoded: AsyncInputStream): Future[TxRollbackOk] {.async.} = newTxRollbackOk()

proc encode*(self: TxRollbackOk, to: AsyncOutputStream) {.async.} = discard
