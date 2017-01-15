package ru.arigativa.akka.streams

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.Future

/**
  * Created by hsslbch on 1/13/17.
  */
object PgCopyStreamConverters {

  def sink(sql: String, getConnection: => PGConnection, encoding: String = "UTF-8"): Sink[Product, Future[Long]] =
    encodeTuples(encoding)
      .toMat(bytesSink(sql, getConnection))(Keep.right)
      .named("pgCopySink")

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
          .getBytes(encoding)
      }
      .map(ByteString.fromArray)

  private def esc(s: String) = s.replace("""\""", """\\""")

  def bytesSink(sql: String, getConnection: => PGConnection): Sink[ByteString, Future[Long]] =
    Sink.fromGraph(new PgCopySinkStage(sql, getConnection))
}
