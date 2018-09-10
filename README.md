# nim-resp

RESP(REdis Serialization Protocol) Serialization for Nim

## How to use 

```nim
  echo $encodeValue(RedisValue(kind:vkStr, s:"Hello, World"))
  # # +Hello, World
  echo $encodeValue(RedisValue(kind:vkInt, i:341))
  # # :341
  echo $encodeValue(RedisValue(kind:vkError, err:"Not found"))
  # # -Not found
  echo $encodeValue(RedisValue(kind:vkArray, l: @[RedisValue(kind:vkStr, s:"Hello World"), RedisValue(kind:vkInt, i:23)]  ))
  # #*2
  # #+Hello World
  # #:23

  echo $encodeValue(RedisValue(kind:vkBulkStr, bs:"Hello, World THIS IS REALLY NICE"))
  # #$32
  # # Hello, World THIS IS REALLY NICE  
  echo decodeString("*3\r\n:1\r\n:2\r\n:3\r\n\r\n")
  # # @[1, 2, 3]
  echo decodeString("+Hello, World\r\n")
  # # Hello, World
  echo decodeString("-Not found\r\n")
  # # Not found
  echo decodeString(":1512\r\n")
  # # 1512
  echo $decodeString("$32\r\nHello, World THIS IS REALLY NICE\r\n")
  # Hello, World THIS IS REALLY NICE
  echo decodeString("*2\r\n+Hello World\r\n:23\r\n")
  # @[Hello World, 23]
  echo decodeString("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n\r\n*5\r\n:5\r\n:7\r\n+Hello Word\r\n-Err\r\n$6\r\nfoobar\r\n")
  # @[@[1, 2, 3], @[5, 7, Hello Word, Err, foobar]]
  echo $decodeString("*4\r\n:51231\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")
  # @[51231, foo, , bar]
```

## Executing commands

### Sync

```nim

  let con = open("localhost", 6379.Port)
  echo $con.execCommand("PING", @[])
  echo $con.execCommand("SET", @["auser", "avalue"])
  echo $con.execCommand("GET", @["auser"])
  echo $con.execCommand("SCAN", @["0"])

```

### Async

```
  let con = await openAsync("localhost", 6379.Port)
  echo "Opened async"
  var res = await con.execCommand("PING", @[])
  echo res
  res = await con.execCommand("SET", @["auser", "avalue"])
  echo res
  res = await con.execCommand("GET", @["auser"])
  echo res
  res = await con.execCommand("SCAN", @["0"])
  echo res
  res = await con.execCommand("SET", @["auser", "avalue"])
  echo res
  res = await con.execCommand("GET", @["auser"])
  echo res
  res = await con.execCommand("SCAN", @["0"])
  echo res 

  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  res = await con.commitCommands()
  echo res
```


## Pipelining
You can use `enqueueCommand` and `commitCommands` to make use of redis pipelining
```nim
  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])
  
  echo $con.commitCommands()
```





## Roadmap

- [X] Protocol serializer/deserializer
- [] Tests
- [X] Pipelining
- [X] Async APIs
