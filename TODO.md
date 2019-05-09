Milestone

# 1
- algebra to drive protocol steps
- support for column types (date, datetime, string, float32/64, int8/16/32/64)

# 2
- shapeless auto-derivation (transform case class from/to columns)
- java.io client frontend

# 3
- benchmarking vs VirtusAI
- fix decoders for leb128 and blockinfo fields to return a NotEnough in case, bench for 50M read

# 4
- tests
- support for column types (
    [X] simple 1d-array of native elements (ints, floats, strings),
    [X] simple nullable,
    [X] enum8/16,
    [X] fixedstring,
    [X] uint8/16/32/64,
    [X] uuid)

# 5
- settings
- manage exceptions from the server (forinstance when sending bad settings)
- max_insert_block_size to split block while sending data
- client buffer size ?
- first time the in buffer is touched to manage overflow of buffer with a user firendly exception message


# 6
- reactive frontend (netty + monix)

# 7
- cancel queries
- queries with progress

# 8
- performance jmh
- optimization
 
# 9
- support for column types (generalized array, generalized tuple, decimal)


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
[ ] Tuple
[v] UInt8
[v] UInt16
[v] UInt32
[v] UInt64
[v] UUID
[ ] generic data types (array of array, tuples, ...)