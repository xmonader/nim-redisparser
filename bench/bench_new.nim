import redisparser, tables

var testpairs = initOrderedTable[RedisValue, string]()
testpairs[RedisValue(kind:vkStr, s:"Hello, World")] = "+Hello, World\r\n"
testpairs[RedisValue(kind:vkInt, i:341)] = ":341\r\n"
testpairs[RedisValue(kind:vkError, err:"Not found")] = "-Not found\r\n"
testpairs[RedisValue(kind:vkArray, l: @[RedisValue(kind:vkStr, s:"Hello World"), RedisValue(kind:vkInt, i:23)])] = "*2\r\n+Hello World\r\n:23\r\n\r\n"
testpairs[RedisValue(kind:vkBulkStr, bs:"Hello, World THIS IS REALLY NICE")] = "$32\r\nHello, World THIS IS REALLY NICE\r\n"

proc testNew*() =
  for k, v in testpairs.pairs():
    if encodeValue(k) != v:
      raise newException(ValueError, "parser error")
    if decodeString(v) != k:
      raise newException(ValueError, "parser error")