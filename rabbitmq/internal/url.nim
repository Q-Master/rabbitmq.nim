import uri
import strutils
import tables
export uri

proc decodeQuery*(query: string): Table[string, string] =
  result = initTable[string, string]()
  let kvarr = query.split('&')
  for pair in kvarr:
    let kvargs = pair.split('=')
    case kvargs.len()
    of 2:
      let k = kvargs[0].decodeUrl()
      let v = kvargs[1].decodeUrl()
      result[k] = v
    of 1:
      let k = kvargs[0].decodeUrl()
      result[k] = ""
    else:
      discard
