import streams
import ./mthd
import ../data

type 
  ConfirmSelect* = ref object of Method
    noWait: bool
  ConfirmSelectOk* = ref object of Method

#--------------- Confirm.Select ---------------#

proc newConfirmSelect*(noWait=false): ConfirmSelect =
  result.new
  result.initMethod(true, 0x0055000A)
  result.noWait = noWait

proc decode*(_: type[ConfirmSelect], encoded: Stream): ConfirmSelect =
  let bbuf = encoded.readBigEndianU8()
  let noWait = (bbuf and 0x01) != 0
  result = newConfirmSelect(noWait)

proc encode*(self: ConfirmSelect, to: Stream) =
  let bbuf = (if self.noWait: 0x01.uint8 else: 0x00.uint8)
  to.writeBigEndian8(bbuf)

#--------------- Confirm.SelectOk ---------------#

proc newConfirmSelectOk*(): ConfirmSelectOk =
  result.new
  result.initMethod(false, 0x0055000B)

proc decode*(_: type[ConfirmSelectOk], encoded: Stream): ConfirmSelectOk = newConfirmSelectOk()

proc encode*(self: ConfirmSelectOk, to: Stream) = discard
