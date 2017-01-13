package com.github.klpx.akka

import akka.stream.scaladsl.Sink
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.Future

/**
  * Created by hsslbch on 1/13/17.
  */
object PgCopyStreamConverters {

  def bytesSink(sql: String, getConnection: => PGConnection): Sink[ByteString, Future[Long]] =
    Sink.fromGraph(new PgCopySinkStage(sql, getConnection))
}
