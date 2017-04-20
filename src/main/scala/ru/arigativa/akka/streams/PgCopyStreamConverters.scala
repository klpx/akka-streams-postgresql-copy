package ru.arigativa.akka.streams

import java.nio.charset.Charset

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.Future
import scala.io.Codec

/**
  * Created by hsslbch on 1/13/17.
  */
object PgCopyStreamConverters {

  private val escapeChars = Seq(
    "\\" -> "\\\\", // escape `escape` character is first
    "\b" -> "\\b", "\f" -> "\\f", "\n" -> "\\n",
    "\r" -> "\\r", "\t" -> "\\t", "\u0011" -> "\\v"
  )

  private val escape: String => String =
    escapeChars.foldLeft(identity[String] _) {
      case (resultFunction, (sFrom, sTo)) =>
        resultFunction.andThen(_.replace(sFrom, sTo))
    }

  private val unescape: String => String =
    escapeChars.foldLeft(identity[String] _) {
      case (resultFunction, (sTo, sFrom)) =>
        resultFunction.andThen(_.replace(sFrom, sTo))
    }

  def source(query: String, settings: PgCopySourceSettings)(implicit codec: Codec): Source[Seq[String], Future[Long]] =
    bytesSource(query, settings)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
      .map(_.decodeString(codec.charSet))
      .map { line =>
        line.split('\t')
          .map {
            case "\\N" => null
            case field => unescape(field)
          }
      }

  def bytesSource(query: String, settings: PgCopySourceSettings): Source[ByteString, Future[Long]] =
    Source.fromGraph(new PgCopySourceStage(query, settings))

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
            case Some(value) => escape(value.toString)
            case value => escape(value.toString)
          }
          .mkString("", "\t", "\n")
          .getBytes(codec.charSet)
      }
      .map(ByteString.fromArray)
}
