import tables
import strutils
import asyncdispatch
import faststreams/[inputs, outputs]
import ./exceptions
import ./async_socket_adapters

type
  AuthMechanism* = enum
    AUTH_NOT_SET = "NONE"
    AUTH_PLAIN = "PLAIN"

const AUTH_METHODS = {
  "PLAIN": AUTH_PLAIN
}.toTable()

const REV_AUTH_METHODS = {
  AUTH_PLAIN: "PLAIN"
}.toTable()

proc getAuthMechanism*(mechanisms: string): AuthMechanism =
  for mechanism in mechanisms.split({' ', ',', ';', '|'}):
    if AUTH_METHODS.hasKey(mechanism):
      return AUTH_METHODS[mechanism]
  raise newException(AuthenticationError, mechanisms)
  
proc getAuthMechanismName*(auth: AuthMechanism): string =
  result = REV_AUTH_METHODS[auth]

proc encodeAuth*(auth: AuthMechanism, user = "guest", password = "guest"): string =
  case auth
  of AUTH_PLAIN:
    result = "\x00" & user & "\x00" & password
  else:
    raise newException(AuthenticationError, $auth & " not supported")
