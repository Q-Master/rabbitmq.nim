# Asyncronous RabbitMQ Pure Nim library
This is a asyncronous driver to use with [RabbitMQ message broker](https://www.rabbitmq.com).

Driver features
-----------------
This driver implements AMQP 0.9.1 specification. This table shows working AMQP classes.

| Classes    | Status             |  Notes |
|------------:|:-------------------|:-------|
| Connection  | Tested and working | Not fully tested amqps connection and connection to multiple instances, closing a connection from server is not properly working yet|
| Channel     | Tested and working | Not tested: flow methods |
| Exchange    | Tested and working | Not tested: binding and unbinding |
| Queue       | Tested and working | |
| Basic       | Tested and working | |
| Confirm     | Not yet completely implemented HL | |
| TX          | Not yet completely implemented HL | |

## Info
Driver is not yet fully implementing all the needed features in high level API, but seems working. Exceptions needed to be reviewed and optimized.

## Examples
All the needed examples are almost self-explanatory in [connection.nim](tests/connection.nim), [channel.nim](tests/channel.nim), [queue.nim](tests/queue.nim), [exchange.nim](tests/exchange.nim)
