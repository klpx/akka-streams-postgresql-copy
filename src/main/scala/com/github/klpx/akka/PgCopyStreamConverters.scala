package com.github.klpx.akka

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.Future

/**
  * Created by hsslbch on 1/13/17.
  */
object PgCopyStreamConverters {

  private def esc(s: String) = s.replace("""\""", """\\""")

  def encodeTuples(encoding: String = "UTF-8"): Flow[Product, ByteString, NotUsed] =
    Flow[Product]
      .map {
        _.productIterator
          .map {
            case None | null => """\N"""
            case Some(value) => esc(value.toString)
            case value => esc(value.toString)
          }
          .mkString("", "\t", "\n")
      }
      .map(ByteString.apply(_: String, encoding))


  def bytesSink(sql: String, getConnection: => PGConnection): Sink[ByteString, Future[Long]] =
    Sink.fromGraph(new PgCopySinkStage(sql, getConnection))
}
