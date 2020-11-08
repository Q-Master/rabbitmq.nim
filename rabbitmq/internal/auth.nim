import tables
import strutils
import asyncdispatch
import faststreams/[inputs, outputs]
import ./exceptions
import ./async_socket_adapters

type
  AuthMechanism* = enum
    AUTH_PLAIN = "PLAIN"

const AUTH_METHODS = {
  "PLAIN": AUTH_PLAIN
}.toTable()

proc getAuthMechanism*(mechanisms: string): AuthMechanism =
  for mechanism in mechanisms.split({' ', ',', ';', '|'}):
    if AUTH_METHODS.hasKey(mechanism):
      return AUTH_METHODS[mechanism]
  raise newException(AuthenticationError, mechanisms)
  
proc encode*(auth: AuthMechanism, user = "guest", password = "guest", to: AsyncOutputStream) {.async.} =
  case auth
  of AUTH_PLAIN:
    let authstr: string = "\x00" & user & "\x00" & password
    await to.asyncWriteBytes(cast[ptr byte](unsafeAddr authstr), authstr.len())
#  else:
#    raise newException(AuthenticationError, $auth & " not supported")
