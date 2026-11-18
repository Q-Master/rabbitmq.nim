import asyncrabbitmq/internal/spec
# Package
description = "Pure Nim asyncronous driver for RabbitMQ"
version     = RMQVERSION
license     = "MIT"
author      = AUTHOR
srcDir      = "asyncrabbitmq"


# Dependencies
requires "nim >= 2.0"
requires "networkutils >= 0.7.3"


task test, "tests":
  let tests = @["connection", "channel", "exchange", "queue", "basic"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
