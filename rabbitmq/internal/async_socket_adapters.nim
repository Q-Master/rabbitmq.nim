import asyncdispatch, asyncnet
import faststreams/[async_backend, inputs, outputs, buffers, multisync]
import stew/ptrops
export fsMultiSync, asyncdispatch

type AsyncSocketInputStream* = ref object of InputStream
  socket: AsyncSocket

type AsyncSocketOutputStream* = ref object of OutputStream
  socket: AsyncSocket

proc recvAdapter(s: AsyncSocket, dst: pointer, dstLen: Natural): Future[Natural] {.async.} =
  let res = await s.recvInto(dst, dstLen)
  result = Natural(res)

# TODO: Use the Raising type here
let asyncSocketInputVTable = InputStreamVTable(
  readSync: proc (s: InputStream, dst: pointer, dstLen: Natural): Natural {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors "Unexpected exception from async macro":
      let cs = AsyncSocketInputStream(s)
      return waitFor cs.socket.recvInto(dst, dstLen)
  ,
  readAsync: proc (s: InputStream, dst: pointer, dstLen: Natural): Future[Natural] {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors "Unexpected exception from merely forwarding a future":
      let cs = AsyncSocketInputStream(s)
      return cs.socket.recvAdapter(dst, dstLen)
  ,
  closeSync: proc (s: InputStream) {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors "Failed to close async socket":
      let cs = AsyncSocketInputStream(s)
      cs.socket.close()
  ,
  closeAsync: nil
)

func asyncSocketInput*(s: AsyncSocket, pageSize = defaultPageSize, allowWaitFor = false): AsyncInputStream =
  AsyncInputStream AsyncSocketInputStream(
    vtable: vtableAddr asyncSocketInputVTable,
    buffers: initPageBuffers(pageSize),
    socket: s)

let asyncSocketOutputVTable = OutputStreamVTable(
  writeSync: proc (s: OutputStream, src: pointer, srcLen: Natural) {.nimcall, gcsafe, raises: [IOError, Defect].} =
    let cs = AsyncSocketOutputStream(s)
    fsTranslateErrors "Error sending to socket":
      waitFor cs.socket.send(src, srcLen)
  ,
  writeAsync: proc (s: OutputStream, src: pointer, srcLen: Natural): Future[void] {.nimcall, gcsafe, raises: [IOError, Defect].} =
    let cs = AsyncSocketOutputStream(s)
    fsTranslateErrors "Error sending to socket":
      return cs.socket.send(src, srcLen)
  ,
  closeSync: proc (s: OutputStream) {.nimcall, gcsafe, raises: [IOError, Defect].} =
    fsTranslateErrors "Failed to close async socket":
      let cs = AsyncSocketOutputStream(s)
      cs.socket.close()
  ,
  closeAsync: nil
)

func asyncSocketOutput*(s: AsyncSocket, pageSize = defaultPageSize): AsyncOutputStream =
  AsyncOutputStream AsyncSocketOutputStream(
    vtable: vtableAddr(asyncSocketOutputVTable),
    buffers: initPageBuffers(pageSize),
    socket: s)

proc disconnectInputDevice(s: InputStream) =
  # TODO
  # Document the behavior that closeAsync is preferred
  if s.vtable != nil:
    if s.vtable.closeAsync != nil:
      s.closeFut = s.vtable.closeAsync(s)
    elif s.vtable.closeSync != nil:
      s.vtable.closeSync(s)
    s.vtable = nil

proc asyncReadIntoEx*(s: AsyncInputStream, dst: ptr byte, dstLen: Natural): Future[Natural] {.async, discardable.} =
  let str = InputStream(s)
  let totalBytesDrained = drainBuffersInto(str, dst, dstLen)
  var bytesDeficit = (dstLen - totalBytesDrained)

  if bytesDeficit > 0:
    var adjustedDst = offset(dst, totalBytesDrained)

    while true:
      let newBytesRead = await str.vtable.readAsync(str, adjustedDst, bytesDeficit)

      str.spanEndPos += newBytesRead
      bytesDeficit -= newBytesRead

      if str.buffers.eofReached:
        disconnectInputDevice(str)
        break

      if bytesDeficit == 0:
        break

      adjustedDst = offset(dst, newBytesRead)

  result = dstLen - bytesDeficit


proc asyncWriteBytes*(sp: AsyncOutputStream, src: ptr byte, srcLen: Natural) {.async.}=
  let s = OutputStream(sp)
  if srcLen == 0: return

  # We have a short inlinable function handling the case when the input is
  # short enough to fit in the current page. We'll keep buffering until the
  # page is full:
  let runway = s.span.len
  if srcLen <= runway:
    copyMem(s.span.startAddr, src, srcLen)
    s.span.startAddr = offset(s.span.startAddr, srcLen)
  elif s.vtable == nil:
    # We are not ready to flush, so we must create pending pages.
    # We'll try to create them as large as possible:
    var 
      runway = s.span.len
      source = src
      length = srcLen

    if runway > 0:
      copyMem(s.span.startAddr, source, runway)
      source = offset(source, runway)
      length -= runway

    fsAssert s.buffers != nil

    let nextPageSize = nextAlignedSize(length, s.buffers.pageSize)
    let nextPage = s.buffers.addWritablePage(nextPageSize)

    s.span = nextPage.fullSpan
    s.spanEndPos += nextPageSize

    copyMem(s.span.startAddr, src, length)
    s.span.startAddr = offset(s.span.startAddr, length)
  else:
    trackWrittenTo(s.buffers, s.span.startAddr)
    await s.vtable.writeAsync(s, src, srcLen)