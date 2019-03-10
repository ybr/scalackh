package ckh.protocol

object PacketTypes {
  object Client {
    // Name, version, revision, default DB
    val HELLO = 0

    // Query id, query settings, stage up to which the query must be executed,
    // whether the compression must be used, query text (without data for INSERTs)
    val QUERY = 1

    // A block of data
    val DATA = 2

    // Cancel the query execution
    val CANCEL = 3

    // Check that connection to the server is alive
    val PING = 4

    // Check status of tables on the server
    val TABLES_STATUS_REQUEST = 5 // not implemented
  }

  object Server {
    // Server info
    val HELLO = 0

    // Block of data
    val DATA = 1

    // Exception during query execution
    val EXCEPTION = 2

    // Query execution progress (rows read, bytes read)
    val PROGRESS = 3

    // Ping response
    val PONG = 4

    // All packets were transmitted
    val END_OF_STREAM = 5

    // Packet with profiling info
    val PROFILE_INFO = 6

    // Block with totals
    val TOTALS = 7

    // Block with mins and maxs
    val EXTREMES = 8

    // Response to TablesStatus request
    val TABLES_STATUS_RESPONSE = 9 // not implemented

    // System logs of the query execution
    val LOG = 10
  }
}