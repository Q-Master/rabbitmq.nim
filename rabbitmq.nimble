import rabbitmq/internal/spec
# Package
description = "Pure Nim asyncronous driver for RabbitMQ"
version     = RMQVERSION
license     = "MIT"
author      = AUTHOR

# Dependencies
requires "nim >= 0.20.00", "https://github.com/yglukhov/iface", "networkutils >= 0.6.1"

task test, "tests":
  let tests = @["connection"]
  for test in tests:
    echo "Running " & test & " test"
    try:
      exec "nim c -r tests/" & test & ".nim"
    except OSError:
      continue
