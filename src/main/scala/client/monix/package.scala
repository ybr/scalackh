package scalackh.client

import _root_.monix.eval.Task

package object monix {
  implicit class TaskConnectionDerivationOps(connection: Task[ConnectionForMonix]) extends AnyRef {
    def withDerivation(): Task[DerivationConnectionForMonix] = connection.map(ConnectionDerivationOps(_).withDerivation)
  }

  implicit class ConnectionDerivationOps(connection: ConnectionForMonix) extends AnyRef {
    def withDerivation(): DerivationConnectionForMonix = new DerivationConnectionForMonixImpl(connection)
  }
}