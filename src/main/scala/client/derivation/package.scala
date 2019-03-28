package scalackh.client

import scala.util.Try

package object derivation {
  implicit class TryConnectionDerivationOps(connection: Try[Connection]) extends AnyRef {
    def withDerivation(): Try[DerivationConnection] = connection.map(ConnectionDerivationOps(_).withDerivation)
  }

  implicit class ConnectionDerivationOps(connection: Connection) extends AnyRef {
    def withDerivation(): DerivationConnection = ConnectionForDerivation(connection)
  }
}