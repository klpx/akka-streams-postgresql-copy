package com.github.klpx.akka

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
  * Sinks ByteString as postgres COPY data, returns count of rows copied
  */
private[klpx] class PgCopySinkStage(sql: String,
                      getConnection: => PGConnection
                     ) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]] {

  private val in = Inlet[ByteString]("PgCopySink.in")
  override def shape: SinkShape[ByteString] = SinkShape.of(in)

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {
    val completePromise = Promise[Long]()
    val stageLogic = new GraphStageLogic(shape) with InHandler {
      private val conn = getConnection

      private val copyIn = conn.getCopyAPI.copyIn(sql)
      private var bytesCopied = 0

      override def preStart(): Unit = pull(in)

      def onPush(): Unit = {
        val buf = grab(in)
        try {
          copyIn.writeToCopy(buf.toArray, bytesCopied, buf.length)
          bytesCopied += buf.length
          pull(in)
        } catch {
          case ex: Throwable => onUpstreamFailure(ex)
        }
      }

      override def onUpstreamFinish(): Unit = {
        val rowsCopiedTry = Try(copyIn.endCopy())
        completePromise.tryComplete(rowsCopiedTry)
        completeStage()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          if (copyIn.isActive) {
            copyIn.cancelCopy()
          }
        } finally {
          completePromise.tryFailure(ex)
          failStage(ex)
        }
      }

      setHandler(in, this)
    }

    stageLogic -> completePromise.future
  }
}


/*
        def esc(s: String) = s.replace("\\", "\\\\")
            .productIterator
            .map {
              case None => """\N"""
              case Some(value) => esc(value.toString)
              case value => esc(value.toString)
            }
            .mkString("", "\t", "\n")
            .getBytes
 */
