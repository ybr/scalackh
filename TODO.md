Milestone

# 1
- algebra to drive protocol steps
- support for column types (date, datetime, string, float32/64, int8/16/32/64)

# 2
- shapeless auto-derivation (transform case class from/to columns)
- java.io client frontend

# 3
- performance (jmh, benchmarking)

# 4
- tests
- support for column types (simple 1d-array, simple nullable, enum8/16, fixedstring, uint8/16/32/64, nested, uuid)

# 5
- reactive frontend (netty + monix)

# 7
- settings
 
# 8
- support for column types (array, tuple)

# 9
- performance jmh
- optimization


Reads

[v] Array
[v] Date
[v] DateTime
[ ] Decimal
[v] Enum8
[v] Enum16
[v] FixedString
[v] Float32
[v] Float64
[v] Int8
[v] Int16
[v] Int32
[v] Int64
[v] Nested
[v] Nullable
[v] String
[v] Tuple
[v] UInt8
[v] UInt16
[v] UInt32
[v] UInt64
[v] UUID
[ ] generic data types (array of array, tuples, ...)

Writes

[ ] Array
[v] Date
[v] DateTime
[ ] Decimal
[ ] Enum8
[ ] Enum16
[ ] FixedString
[v] Float32
[v] Float64
[v] Int8
[v] Int16
[v] Int32
[v] Int64
[ ] Nested
[v] Nullable
[v] String
[ ] Tuple
[ ] UInt8
[ ] UInt16
[ ] UInt32
[ ] UInt64
[ ] UUID
[ ] generic data types (array of array, tuples, ...)