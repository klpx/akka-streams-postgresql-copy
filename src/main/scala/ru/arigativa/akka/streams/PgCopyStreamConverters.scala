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

  private val escapeSpecialChars: String => String =
    Seq(
      "\\" -> "\\\\", // escape `escape` character is first
      "\b" -> "\\b", "\f" -> "\\f", "\n" -> "\\n",
      "\r" -> "\\r", "\t" -> "\\t", "\u0011" -> "\\v"
    ).foldLeft(identity[String] _) {
      case (resultFunction, (sFrom, sTo)) =>
        resultFunction.andThen(_.replace(sFrom, sTo))
    }

  def sink(connectionProvider: ConnectionProvider, query: String, encoding: String = "UTF-8"): Sink[Product, Future[Long]] =
    encodeTuples(encoding)
      .toMat(bytesSink(connectionProvider, query))(Keep.right)
      .named("pgCopySink")

  def bytesSink(connectionProvider: ConnectionProvider, query: String): Sink[ByteString, Future[Long]] =
    Sink.fromGraph(new PgCopySinkStage(connectionProvider, query))

  def encodeTuples(encoding: String = "UTF-8"): Flow[Product, ByteString, NotUsed] =
    Flow[Product]
      .map {
        _.productIterator
          .map {
            case None | null => """\N"""
            case Some(value) => escapeSpecialChars(value.toString)
            case value => escapeSpecialChars(value.toString)
          }
          .mkString("", "\t", "\n")
          .getBytes(encoding)
      }
      .map(ByteString.fromArray)
}
