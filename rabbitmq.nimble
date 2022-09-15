import rabbitmq/internal/spec
# Package
description = "Pure Nim asyncronous driver for RabbitMQ"
version     = RMQVERSION
license     = "MIT"
author      = AUTHOR

# Dependencies
requires "nim >= 0.20.00", "networkutils >= 0.6.1"

task test, "tests":
  let tests = @["connection", "channel", "exchange", "queue", "basic"]
  #let tests = @["basic"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
