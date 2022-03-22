import tables
import strutils
import ./exceptions

type
  AMQPAuthMechanism* = enum
    AUTH_NOT_SET = "NONE"
    AUTH_PLAIN = "PLAIN"

const AUTH_METHODS = {
  "PLAIN": AUTH_PLAIN
}.toTable()

proc getCheckAuthSupported*(mechanisms: string): AMQPAuthMechanism =
  for mechanism in mechanisms.split({' ', ',', ';', '|'}):
    if AUTH_METHODS.hasKey(mechanism):
      return AUTH_METHODS[mechanism]
  raise newException(AuthenticationError, mechanisms)
  
proc encodeAuth*(auth: AMQPAuthMechanism, user = "guest", password = "guest"): string =
  case auth
  of AUTH_PLAIN:
    result = "\x00" & user & "\x00" & password
  else:
    raise newException(AuthenticationError, $auth & " not supported")
