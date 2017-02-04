package ru.arigativa.akka.streams

import java.nio.charset.Charset

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Keep, Sink}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.Future
import scala.io.Codec

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

  def sink(query: String, settings: PgCopySinkSettings)(implicit codec: Codec): Sink[Product, Future[Long]] =
    encodeTuples(codec)
      .toMat(bytesSink(query, settings))(Keep.right)
      .named("pgCopySink")

  def bytesSink(query: String, settings: PgCopySinkSettings): Sink[ByteString, Future[Long]] =
    Sink.fromGraph(new PgCopySinkStage(query,  settings))

  def encodeTuples(implicit codec: Codec): Flow[Product, ByteString, NotUsed] =
    Flow[Product]
      .map {
        _.productIterator
          .map {
            case None | null => """\N"""
            case Some(value) => escapeSpecialChars(value.toString)
            case value => escapeSpecialChars(value.toString)
          }
          .mkString("", "\t", "\n")
          .getBytes(codec.charSet)
      }
      .map(ByteString.fromArray)
}
