#[
Class Grammar:
    basic = C:QOS S:QOS-OK
          / C:CONSUME S:CONSUME-OK
          / C:CANCEL S:CANCEL-OK
          / C:PUBLISH content
          / S:RETURN content
          / S:DELIVER content
          / C:GET ( S:GET-OK content / S:GET-EMPTY )
          / C:ACK
          / S:ACK
          / C:REJECT
          / C:NACK
          / S:NACK
          / C:RECOVER-ASYNC
          / C:RECOVER S:RECOVER-OK
]#