package ckh.native

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID

sealed trait Column
case class DateColumn(name: String, data: Array[LocalDate]) extends Column
case class DateTimeColumn(name: String, data: Array[LocalDateTime]) extends Column // UTC zone offset
case class EnumColumn(name: String, bits: Int, enums: Map[Int, String], data: Array[Int]) extends Column
case class FixedStringColumn(name: String, strLength: Int, data: Array[String]) extends Column
case class Float32Column(name: String, data: Array[Float]) extends Column
case class Float64Column(name: String, data: Array[Double]) extends Column
case class Int8Column(name: String, data: Array[Byte]) extends Column
case class Int16Column(name: String, data: Array[Short]) extends Column
case class Int32Column(name: String, data: Array[Int]) extends Column
case class Int64Column(name: String, data: Array[Long]) extends Column
case class NullableColumn(name: String, nulls: Array[Boolean], column: Column) extends Column
case class StringColumn(name: String, data: Array[String]) extends Column
case class UuidColumn(name: String, data: Array[UUID]) extends Column