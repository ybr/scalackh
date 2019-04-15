package scalackh.protocol

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

sealed trait ClickhouseArray
sealed trait ScalaNativeArray extends ClickhouseArray
case class DateArray(dates: Array[Array[LocalDate]]) extends ScalaNativeArray
case class DateTimeArray(datetimes: Array[Array[LocalDateTime]]) extends ScalaNativeArray
case class Float32Array(floats: Array[Array[Float]]) extends ScalaNativeArray
case class Float64Array(doubles: Array[Array[Double]]) extends ScalaNativeArray
case class Int8Array(bytes: Array[Array[Byte]]) extends ScalaNativeArray
case class Int16Array(shorts: Array[Array[Short]]) extends ScalaNativeArray
case class Int32Array(ints: Array[Array[Int]]) extends ScalaNativeArray
case class Int64Array(longs: Array[Array[Long]]) extends ScalaNativeArray
case class StringArray(strings: Array[Array[String]]) extends ScalaNativeArray
case class UInt8Array(bytes: Array[Array[Byte]]) extends ScalaNativeArray
case class UInt16Array(shorts: Array[Array[Short]]) extends ScalaNativeArray
case class UInt32Array(ints: Array[Array[Int]]) extends ScalaNativeArray
case class UInt64Array(longs: Array[Array[Long]]) extends ScalaNativeArray
case class UuidArray(uuids: Array[Array[UUID]]) extends ScalaNativeArray