
import strformat, tables, json, strutils, sequtils, hashes, net, asyncdispatch, asyncnet, os, strutils, parseutils, deques, options

const CRLF = "\r\n"
const REDISNIL = "\0\0"

type
  ValueKind = enum
    vkStr, vkError, vkInt, vkBulkStr, vkArray

  RedisValue* = ref object
    case kind*: ValueKind
    of vkStr: s*: string
    of vkError : err*: string
    of vkInt: i*: int
    of vkBulkStr: bs*: string
    of vkArray: l*: seq[RedisValue]

proc `$`*(obj: RedisValue): string = 
  case obj.kind
    of vkStr : return  fmt"{$(obj.s)}"
    of vkBulkStr: return fmt"{$(obj.bs)}"
    of vkInt : return fmt"{$(obj.i)}"
    of vkArray: return fmt"{$(obj.l)}"
    of vkError: return fmt"{$(obj.err)}"

proc hash*(obj: RedisValue): Hash = 
  case obj.kind
  of vkStr : !$(hash(obj.s))
  of vkBulkStr: !$(hash(obj.bs))
  of vkInt : !$(hash(obj.i))
  of vkArray: !$(hash(obj.l))
  of vkError: !$(hash(obj.err))

proc `==`* (a, b: RedisValue): bool =
  ## Check two nodes for equality
  if a.isNil:
      if b.isNil: return true
      return false
  elif b.isNil or a.kind != b.kind:
      return false
  else:
      case a.kind
      of vkStr:
          result = a.s == b.s
      of vkBulkStr:
          result = a.s == b.s
      of vkInt:
          result = a.i == b.i
      of vkArray:
          result = a.l == b.l
          result = true
      of vkError:
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
  return (RedisValue(kind:vkStr, s:s[1..crlfpos-1]), crlfpos+len(CRLF))

proc decodeBulkStr(s:string): (RedisValue, int) = 
  let crlfpos = s.find(CRLF)
  var bulklen = 0
  let slen = s[1..crlfpos-1]
  bulklen = parseInt(slen)
  var bulk: string
  if bulklen == -1:
      bulk = nil
      return (RedisValue(kind:vkBulkStr, bs:REDISNIL), crlfpos+len(CRLF))
  else:
    let nextcrlf = s.find(CRLF, crlfpos+len(CRLF))
    bulk = s[crlfpos+len(CRLF)..nextcrlf-1] 
    return (RedisValue(kind:vkBulkStr, bs:bulk), nextcrlf+len(CRLF))

proc decodeError(s: string): (RedisValue, int) =
  let crlfpos = s.find(CRLF)
  return (RedisValue(kind:vkError, err:s[1..crlfpos-1]), crlfpos+len(CRLF))

proc decodeInt(s: string): (RedisValue, int) =
  var i: int
  let crlfpos = s.find(CRLF)
  let sInt = s[1..crlfpos-1]
  if sInt.isDigit():
    i = parseInt(sInt)
  return (RedisValue(kind:vkInt, i:i), crlfpos+len(CRLF))


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
    
    return (RedisValue(kind:vkArray, l:arr), i)
  
  while i < len(s) and len(arr) < arrlen:
    var pair = decode(s[i..len(s)])
    var obj = pair[0]
    arr.add(obj)
    i += pair[1]
  return (RedisValue(kind:vkArray, l:arr), i+len(CRLF))


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
      echo fmt"Unreognized char {curchar}"
      break

proc encode*(v: RedisValue) : string =
  case v.kind 
  of vkStr: return encodeStr(v)
  of vkInt:    return encodeInt(v)
  of vkError:  return encodeErr(v)
  of vkBulkStr: return encodeBulkStr(v)
  of vkArray: return encodeArray(v)


type
  RedisBase[TSocket] = ref object of RootObj
    socket: TSocket
    connected: bool

  Redis* = ref object of RedisBase[net.Socket]


proc open*(host = "localhost", port = 6379.Port): Redis =
  result = Redis(
    socket: newSocket(buffered = true),
  )
  result.socket.connect(host, port)

proc decodeResponse*(resp: string): RedisValue = 
  let pair = decode(resp)
  return pair[0]

let decodeForm = decodeResponse

proc readResponse(this:Redis): string = 
  var data = ""
  var b = ""
  var closed = 999999

  while true:
    try:
      closed = this.socket.recv(b, 1, 1)
    except TimeoutError:
      break
    if closed == 0:
      break
    else:
      data &= b
  return data
    
proc readStream(this:Redis, breakAfter:string): string=
  var b = ""
  var data = ""
  var closed = 99999
  while true:
    if data.endsWith(breakAfter):
      break
    closed = this.socket.recv(b, 1)
    if closed == 0:
      break
    else:
      data &= b
  return data


proc readMany(this:Redis, count:int=1): string=
  var data = ""
  discard this.socket.recv(data, count)
  return data 
      

proc readForm(this:Redis): string =
  var form = ""
  var b = ""
  var closed = 9999
  while true:
    closed = this.socket.recv(b,1)
    if closed == 0:
      break
    else:
      form &= b
      if b == "+":
        form &= this.readStream("\r\n")
        return form
      elif b == "-":
        form &= this.readStream("\r\n")
        return form
      elif b == ":":
        form &= this.readStream("\r\n")
        return form
      elif b == "$":
        let bulklenstr = this.readStream("\r\n")
        form &= bulklenstr
        let bulklenI = parseInt(bulklenstr.strip()) 
        form &= this.readMany(bulklenI)
        form &= this.readStream("\r\n")
        return form
      elif b == "*":
          let lenstr = this.readStream("\r\n")
          form &= lenstr
          let lenstrAsI = parseInt(lenstr.strip())
          for i in countup(1, lenstrAsI):
            form &= this.readForm()
          return form
  return form


proc execCommand*(this: Redis, command: string, args:seq[string]): RedisValue =
  let cmdArgs = concat(@[command], args)
  var cmdAsRedisValues = newSeq[RedisValue]()
  for cmd in cmdArgs:
    cmdAsRedisValues.add(RedisValue(kind:vkBulkStr, bs:cmd))
  var arr = RedisValue(kind:vkArray, l: cmdAsRedisValues)
  this.socket.send(encode(arr))
  let form = this.readForm()
  result = decodeResponse(form) 


when isMainModule:
  echo $encodeStr(RedisValue(kind:vkStr, s:"Hello, World"))
  echo $encodeInt(RedisValue(kind:vkInt, i:341))
  echo $encodeErr(RedisValue(kind:vkError, err:"Not found"))
  echo $encodeArray(RedisValue(kind:vkArray, l: @[RedisValue(kind:vkStr, s:"Hello World"), RedisValue(kind:vkInt, i:23)]  ))
  echo $encodeBulkStr(RedisValue(kind:vkBulkStr, bs:"Hello, World THIS IS REALLY NICE"))

  let s = "*3\r\n:1\r\n:2\r\n:3\r\n\r\n"
  echo $decode(s)
  echo $decodeStr("+Hello, World\r\n")
  echo $decodeError("-Not found\r\n")
  echo $decodeInt(":1512\r\n")
  echo $decodeBulkStr("$32\r\nHello, World THIS IS REALLY NICE\r\n")
  
  echo $decodeArray("*2\r\n+Hello World\r\n:23\r\n")
  echo $decodeArray("*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n\r\n*5\r\n:5\r\n:7\r\n+Hello Word\r\n-Err\r\n$6\r\nfoobar\r\n")
  
  echo $decodeArray("*4\r\n:51231\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")

  let con = open("localhost", 6379.Port)
  echo $con.execCommand("PING", @[])
  echo $con.execCommand("SET", @["auser", "avalue"])
  echo $con.execCommand("GET", @["auser"])
  echo $con.execCommand("SCAN", @["0"])
