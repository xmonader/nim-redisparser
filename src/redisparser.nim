import strformat, strutils, hashes, net,  strutils

const
  CRLF* = "\r\n"
  CRLF_LEN* = len(CRLF)

type
  RespError* = object of IOError
  TypeError* = object of IOError

  ValueKind* = enum
    vkStr, vkError, vkInt, vkBulkStr, vkArray

  RedisValue* = ref object
    case kind: ValueKind
    of vkStr: s: string
    of vkError : err: string
    of vkInt: i: int
    of vkBulkStr: bs: string
    of vkArray: l: seq[RedisValue]

proc `$`*(v: RedisValue): string =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  case v.kind
    of vkStr : return v.s
    of vkBulkStr: return v.bs
    of vkInt : return $v.i
    of vkArray: return $v.l
    of vkError: return v.err

proc hash*(v: RedisValue): Hash =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  var h: Hash = 0
  h = h !& hash(v.kind)
  case v.kind
  of vkStr : h = h !& hash(v.s)
  of vkBulkStr: h = h !& hash(v.bs)
  of vkInt : h = h !&  hash(v.i)
  of vkArray: h = h !& hash(v.l)
  of vkError: h = h !& hash(v.err)
  result = !$h

proc `==`*(a, b: RedisValue): bool =
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

proc len*(v: RedisValue): int =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  case v.kind
  of vkStr:
    result = v.s.len
  of vkError:
    result = v.err.len
  of vkBulkStr:
    result = v.bs.len
  of vkArray:
    result = v.l.len
  else:
    raise newException(TypeError, fmt"Invalid data type for `len`: {v.kind}")

proc `[]`*(v: RedisValue, idx: int): RedisValue =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  if v.kind != vkArray:
    raise newException(TypeError, fmt"Array expected but got {v.kind}")
  result = v.l[idx]

iterator items*(v: RedisValue): RedisValue =
  if v.kind == vkArray:
    for i in 0..<v.l.len:
      yield v.l[0]

proc newRedisString*(input: string = ""): RedisValue {.inline.} = RedisValue(kind: vkStr, s: input)
proc newRedisError*(input: string = ""): RedisValue {.inline.} = RedisValue(kind: vkError, err: input)
proc newRedisInt*(input: SomeInteger = 0): RedisValue {.inline.} = RedisValue(kind: vkInt, i: input)
proc newRedisBulkString*(input: string = ""): RedisValue {.inline.} = RedisValue(kind: vkBulkStr, bs: input)
proc newRedisArray*(input: seq[RedisValue] = @[]): RedisValue {.inline.} = RedisValue(kind: vkArray, l: input)

proc isString*(v: RedisValue): bool {.inline.} =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  v.kind == vkStr
proc isError*(v: RedisValue): bool {.inline.} =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  v.kind == vkError
proc isInteger*(v: RedisValue): bool {.inline.} =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  v.kind == vkInt
proc isBulkString*(v: RedisValue): bool {.inline.} =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  v.kind == vkBulkStr
proc isArray*(v: RedisValue): bool {.inline.} =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  v.kind == vkArray

proc getStr*(v: RedisValue): string =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  if v.isString():
    return v.s
  elif v.isBulkString():
    return v.bs
  raise newException(TypeError, fmt"Value is not a string or bulk string, got kind: {v.kind}")

proc getError*(v: RedisValue): string =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  if v.isError():
    return v.err
  raise newException(TypeError, fmt"Value is not an error, got kind: {v.kind}")

proc getInt*(v: RedisValue): int =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  if v.isInteger():
    return v.i
  raise newException(TypeError, fmt"Value is not an interger, got kind: {v.kind}")

proc getItems*(v: RedisValue): seq[RedisValue] =
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
  if v.isArray():
    return v.l
  raise newException(TypeError, fmt"Value is not an array, got kind: {v.kind}")

proc encode*(v: RedisValue) : string {.gcsafe.}
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
  if v.isNil:
    raise newException(ValueError, "Redis value is nil")
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
  if bulkLen == -1:
    # result must be nil
    inc(pos, CRLF_LEN)
  elif bulklen == 0:
    result = RedisValue(kind:vkBulkStr)
    inc(pos, CRLF_LEN)
  else:
    result = RedisValue(
      kind: vkBulkStr,
      bs: newString(bulklen)
    )
    for i in 0..<bulklen:
      result.bs[i] = s[pos + i]
    inc(pos, bulklen + CRLF_LEN)

proc decodeInt(s: string, pos: var int): RedisValue =
  let
    crlfPos = s.find(CRLF, pos)
    ival = parseInt(s[pos..<crlfPos])
  result = RedisValue(kind:vkInt, i: ival)
  pos = crlfPos + CRLF_LEN

proc decode(s: string, pos: var int): RedisValue {.gcsafe.}
proc decodeArray(s: string, pos: var int): RedisValue =
  var
    crlfPos = s.find(CRLF, pos)
    arrLen = parseInt(s[pos..<crlfPos])
  if arrLen == -1:
    # result must be nil
    pos = crlfPos + CRLF_LEN
  else:
    result = RedisValue(
      kind: vkArray,
      l: newSeq[RedisValue](arrLen)
    )
    pos = s.find(CRLF, crlfPos) + CRLF_LEN # next obj pos

    var i = 0
    while i < arrLen:
      result.l[i] = decode(s, pos)
      inc(i)

proc decode(s: string, pos: var int): RedisValue =
  if s.len == 0:
    return nil
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
    let raw = s.multiReplace(@[("\r", "\\r"), ("\n", "\\n")])
    raise newException(RespError, fmt"Unrecognized char {repr c} at pos {pos} in '{raw}'")

const encodeValue* = encode
proc decodeString*(resp: string): RedisValue =
  var pos = 0
  return decode(resp, pos)


