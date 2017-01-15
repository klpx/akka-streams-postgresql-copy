package ru.arigativa.akka.streams

import org.postgresql.PGConnection
import org.postgresql.core.BaseConnection

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
  * Created by hsslbch on 1/15/17.
  */
trait ConnectionProvider {
  def acquire(): Try[PGConnection]
  def release(): Unit
}

object ConnectionProvider {
  implicit def pgConnectionGetterToCloseableProvider(getConn: () => BaseConnection): ConnectionProvider =
    new ConnectionProvider {
      private var conn: Try[BaseConnection] = Failure(new RuntimeException("Connection is not acquired"))
      def acquire(): Try[PGConnection] = {
        release()
        conn = Try(getConn())
        conn
      }
      def release(): Unit = conn.foreach(_.close())
    }

  implicit def pgConnectionToWrapperProvider(conn: PGConnection): ConnectionProvider =
    new ConnectionProvider {
      def acquire(): Try[PGConnection] = Success(conn)
      def release(): Unit = ()
    }
}
