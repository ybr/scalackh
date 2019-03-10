name := "clickhouse-client"

version := "1.0"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
libraryDependencies += "io.monix" %% "minitest" % "2.3.2" % "test"

testFrameworks += new TestFramework("minitest.runner.Framework")

scalaVersion := "2.12.8"