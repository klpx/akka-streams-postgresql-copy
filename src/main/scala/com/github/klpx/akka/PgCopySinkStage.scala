package com.github.klpx.akka

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString
import org.postgresql.PGConnection

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

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

      override def preStart(): Unit = pull(in)

      def onPush(): Unit = {
        val buf = grab(in)
        try {
          copyIn.writeToCopy(buf.toArray, 0, buf.length)
          pull(in)
        } catch {
          case ex: Throwable => onUpstreamFailure(ex)
        }
      }

      override def onUpstreamFinish(): Unit = {
        Try(copyIn.endCopy()) match {
          case Success(rowsCopied) =>
            completePromise.trySuccess(rowsCopied)
            completeStage()
          case Failure(ex) =>
            completePromise.tryFailure(ex)
            failStage(ex)
        }
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
