import strformat, tables, json, strutils, sequtils, hashes, net, asyncdispatch, asyncnet, os, strutils, parseutils, deques, options

const CRLF* = "\r\n"
const REDISNIL = "\0\0"

type
  ValueKind* = enum
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
          result = a.bs == b.bs
      of vkInt:
          result = a.i == b.i
      of vkArray:
          result = a.l == b.l
          result = true
      of vkError:
          result = a.err == b.err

proc encode*(v: RedisValue) : string 
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


proc encode*(v: RedisValue) : string =
  case v.kind 
  of vkStr: return encodeStr(v)
  of vkInt:    return encodeInt(v)
  of vkError:  return encodeErr(v)
  of vkBulkStr: return encodeBulkStr(v)
  of vkArray: return encodeArray(v)


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
      bulk = REDISNIL 
      return (RedisValue(kind:vkBulkStr, bs:bulk), crlfpos+len(CRLF))
  # elif bulklen == 0:
  #     bulk = ""
  #     return (RedisValue(kind:vkBulkStr, bs:bulk), crlfpos+len(CRLF)+len(CRLF))
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
    var pair = decode(s[i..^1])
    var obj = pair[0]
    arr.add(obj)
    i += pair[1]
  return (RedisValue(kind:vkArray, l:arr), i+len(CRLF))


proc decode(s: string): (RedisValue, int) =
  var i = 0 
  while i < len(s):
    var curchar = $s[i]
    var endindex = s.find(CRLF, i)+len(CRLF)
    if endindex >= s.len:
      endindex = s.len-1
    if curchar == "+":
      var pair = decodeStr(s[i..endindex])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "-":
      var pair = decodeError(s[i..endindex])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "$":
      var pair = decodeBulkStr(s[i..^1])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == ":":
      var pair = decodeInt(s[i..endindex])
      var obj =  pair[0]
      var count =  pair[1]
      i += count
      return (obj, i)
    elif curchar == "*":
      var pair = decodeArray(s[i..^1])
      let obj = pair[0]
      let count =  pair[1]
      i += count 
      return (obj, i)
    else:
      echo fmt"Unreognized char {curchar}"
      break


proc decodeResponse(resp: string): RedisValue = 
  let pair = decode(resp)
  return pair[0]

let decodeForm = decodeResponse
let encodeValue* = encode
let decodeString* = decodeResponse
