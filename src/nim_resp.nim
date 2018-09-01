
import strformat, tables, json, strutils, hashes

const CRLF = "\r\n"
const REDISNIL = "\0\0"

type
  ValueKind = enum
    redStr, redError, redInt, redBulkStr, redArray

  RedisValue* = ref object
    case kind*: ValueKind
    of redStr: s*: string
    of redError : err*: string
    of redInt: i*: int
    of redBulkStr: bs*: string
    of redArray: l*: seq[RedisValue]




proc `$`*(obj: RedisValue): string = 
  case obj.kind
    of redStr : return  fmt"redisString <{$(obj.s)}>"
    of redBulkStr: return fmt"redisBulk <{$(obj.bs)}>"
    of redInt : return fmt"redisInt <{$(obj.i)}>"
    of redArray: return fmt"redisArr <{$(obj.l)}>"
    of redError: return fmt"redisErr <{$(obj.err)}>"

proc hash*(obj: RedisValue): Hash = 
  case obj.kind
  of redStr : !$(hash(obj.s))
  of redBulkStr: !$(hash(obj.bs))
  of redInt : !$(hash(obj.i))
  of redArray: !$(hash(obj.l))
  of redError: !$(hash(obj.err))

proc `==`* (a, b: RedisValue): bool =
  ## Check two nodes for equality
  if a.isNil:
      if b.isNil: return true
      return false
  elif b.isNil or a.kind != b.kind:
      return false
  else:
      case a.kind
      of redStr:
          result = a.s == b.s
      of redBulkStr:
          result = a.s == b.s
      of redInt:
          result = a.i == b.i
      of redArray:
          result = a.l == b.l
          result = true
      of redError:
          result = a.err == b.err
  return false

proc encode(v: RedisValue) : string 
proc encodeStr(v: RedisValue) : string =
  return fmt"+{v.s}{CRLF}"

proc encodeBulkStr(v: RedisValue) : string =
  return fmt"${v.bs.len}{CRLF}{v.bs}{CRLF}"

proc encodeErr(v: RedisValue) : string =
  return fmt"-{v.err}{CRLF}"

proc encodeInt(v: RedisValue) : string =
  return fmt":{v.i}{CRLF}"

proc encodeArray(v: RedisValue): string = 
  var res = "*" & $len(v.l) & CRLF
  for el in v.l:
    res &= encode(el)
  res &= CRLF
  return res


proc decodeStr(s: string): (RedisValue, int) =
  let crlfpos = s.find(CRLF)
  return (RedisValue(kind:redStr, s:s[1..crlfpos-1]), crlfpos+len(CRLF))

proc decodeBulkStr(s:string): (RedisValue, int) = 
  let crlfpos = s.find(CRLF)
  var bulklen = 0
  let slen = s[1..crlfpos-1]
  bulklen = parseInt(slen)
  var bulk: string
  if bulklen == -1:
      bulk = nil
      return (RedisValue(kind:redBulkStr, bs:REDISNIL), crlfpos+len(CRLF))
  else:
    let nextcrlf = s.find(CRLF, crlfpos+len(CRLF))
    bulk = s[crlfpos+len(CRLF)..nextcrlf-1] 
    return (RedisValue(kind:redBulkStr, bs:bulk), nextcrlf+len(CRLF))

proc decodeError(s: string): (RedisValue, int) =
  let crlfpos = s.find(CRLF)
  return (RedisValue(kind:redError, err:s[1..crlfpos-1]), crlfpos+len(CRLF))

proc decodeInt(s: string): (RedisValue, int) =
  var i: int
  let crlfpos = s.find(CRLF)
  let sInt = s[1..crlfpos-1]
  if sInt.isDigit():
    i = parseInt(sInt)
  return (RedisValue(kind:redInt, i:i), crlfpos+len(CRLF))


proc decode(s: string): (RedisValue, int)

proc decodeArray(s: string): (RedisValue, int) =
  var arr = newSeq[RedisValue]()
  var arrlen = 0
  var crlfpos = s.find(CRLF)
  var arrlenStr = s[1..crlfpos-1]
  if arrlenStr.isDigit():
     arrlen = parseInt(arrlenStr)
  
  var nextobjpos = s.find(CRLF)+len(CRLF)
  var i = nextobjpos 
  
  if arrlen == -1:
    
    return (RedisValue(kind:redArray, l:arr), i)
  
  while i < len(s) and len(arr) < arrlen:
    var pair = decode(s[i..len(s)])
    var obj = pair[0]
    arr.add(obj)
    i += pair[1]
  return (RedisValue(kind:redArray, l:arr), i+len(CRLF))


proc decode(s: string): (RedisValue, int) =
  var i = 0 
  while i < len(s):
    var curchar = $s[i]
    if curchar == "+":
      var pair = decodeStr(s[i..s.find(CRLF, i)+len(CRLF)])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "-":
      var pair = decodeError(s[i..s.find(CRLF, i)+len(CRLF)])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "$":
      var pair = decodeBulkStr(s[i..len(s)])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == ":":
      var pair = decodeInt(s[i..s.find(CRLF, i)+len(CRLF)])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "*":
      var pair = decodeArray(s[i..len(s)])
      let obj = pair[0]
      let count =  pair[1]
      i += count 
      return (obj, i)
    else:
      echo fmt"Unrecognized char {curchar}"
      break

proc encode(v: RedisValue) : string =
  case v.kind 
  of redStr: return encodeStr(v)
  of redInt:    return encodeInt(v)
  of redError:  return encodeErr(v)
  of redBulkStr: return encodeBulkStr(v)
  of redArray: return encodeArray(v)


when isMainModule:
  echo $encodeStr(RedisValue(kind:redStr, s:"Hello, World"))
  echo $encodeInt(RedisValue(kind:redInt, i:341))
  echo $encodeErr(RedisValue(kind:redError, err:"Not found"))
  echo $encodeArray(RedisValue(kind:redArray, l: @[RedisValue(kind:redStr, s:"Hello World"), RedisValue(kind:redInt, i:23)]  ))
  echo $encodeBulkStr(RedisValue(kind:redBulkStr, bs:"Hello, World THIS IS REALLY NICE"))

  let s = "*3\r\n:1\r\n:2\r\n:3\r\n\r\n"
  echo $decode(s)
  echo $decodeStr("+Hello, World\r\n")
  echo $decodeError("-Not found\r\n")
  echo $decodeInt(":1512\r\n")
  echo $decodeBulkStr("$32\r\nHello, World THIS IS REALLY NICE\r\n")
  
  echo $decodeArray("*2\r\n+Hello World\r\n:23\r\n")
  echo $decodeArray("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n\r\n*5\r\n:5\r\n:7\r\n+Hello Word\r\n-Err\r\n$6\r\nfoobar\r\n")
  
  echo $decodeArray("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")