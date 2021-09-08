import strformat, strutils, hashes, net,  strutils

const
  CRLF = "\r\n"
  CRLF_LEN = len(CRLF)
  REDIS_NIL = "\0\0"

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
      return a.s == b.s
    of vkBulkStr:
      return a.bs == b.bs
    of vkInt:
      return a.i == b.i
    of vkArray:
      return a.l == b.l
    of vkError:
      return a.err == b.err



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


proc decodeStr(s: string, pos: var int): RedisValue =
  let crlfPos = s.find(CRLF, pos)
  result = RedisValue(kind:vkStr, s: s[pos..<crlfPos])
  pos = crlfPos + CRLF_LEN

proc decodeError(s: string, pos: var int): RedisValue =
  let crlfPos = s.find(CRLF, pos)
  result = RedisValue(kind:vkError, err: s[pos..<crlfPos])
  pos = crlfPos + CRLF_LEN

proc decodeBulkStr(s: string, pos: var int): RedisValue =
  let
    crlfPos = s.find(CRLF, pos)
    bulkLen = parseInt(s[pos..<crlfPos])
  pos = crlfPos + CRLF_LEN
  result = RedisValue(kind:vkBulkStr)
  if bulkLen == -1:
    result.bs = REDIS_NIL
  elif bulklen == 0:
    inc(pos, CRLF_LEN)
  else:
    result.bs = newString(bulklen)
    for i in 0..<bulklen:
      result.bs[i] = s[pos + i]
    inc(pos, bulklen + CRLF_LEN)

proc decodeInt(s: string, pos: var int): RedisValue =
  let
    crlfPos = s.find(CRLF, pos)
    ival = parseInt(s[pos..<crlfPos])
  result = RedisValue(kind:vkInt, i: ival)
  pos = crlfPos + CRLF_LEN

proc decode(s: string, pos: var int): RedisValue
proc decodeArray(s: string, pos: var int): RedisValue =
  var
    crlfPos = s.find(CRLF, pos)
    arrLen = parseInt(s[pos..<crlfPos])

  result = RedisValue(kind:vkArray)
  if arrLen == -1:
    pos = crlfPos + CRLF_LEN
  else:
    result.l = newSeq[RedisValue](arrLen)
    pos = s.find(CRLF, crlfPos) + CRLF_LEN # next obj pos

    var i = 0
    while i < arrLen:
      result.l[i] = decode(s, pos)
      inc(i)

proc decode(s: string, pos: var int): RedisValue =
  let c = s[pos]
  inc(pos)
  case c
  of '+':
    return decodeStr(s, pos)
  of '-':
    return decodeError(s, pos)
  of '$':
    return decodeBulkStr(s, pos)
  of ':':
    return decodeInt(s, pos)
  of '*':
    return decodeArray(s, pos)
  else:
    raise newException(ValueError, fmt"Unreognized char {repr c}")

const encodeValue* = encode
proc decodeString*(resp: string): RedisValue =
  var pos = 0
  return decode(resp, pos)


