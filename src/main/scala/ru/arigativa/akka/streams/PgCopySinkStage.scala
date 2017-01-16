package ru.arigativa.akka.streams

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString
import org.postgresql.PGConnection
import org.postgresql.copy.CopyIn

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Sinks ByteString as postgres COPY data, returns count of rows copied
  */
private[streams] class PgCopySinkStage(connectionProvider: ConnectionProvider, query: String) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]] {

  private val in = Inlet[ByteString]("PgCopySink.in")

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val completePromise = Promise[Long]()
    val stageLogic = new GraphStageLogic(shape) with InHandler {
      private var copyIn: CopyIn = _

      override def preStart(): Unit = {
        connectionProvider.acquire() match {
          case Success(conn) =>
            copyIn = conn.getCopyAPI.copyIn(query)
            pull(in)
          case Failure(ex) => fail(ex)
        }
      }

      def onPush(): Unit = {
        val buf = grab(in)
        try {
          copyIn.writeToCopy(buf.toArray, 0, buf.length)
          pull(in)
        } catch {
          case ex: Throwable => fail(ex)
        }
      }

      override def onUpstreamFinish(): Unit = {
        Try(copyIn.endCopy()) match {
          case Success(rowsCopied) =>
            completePromise.trySuccess(rowsCopied)
            connectionProvider.release(None)
            completeStage()
          case Failure(ex) => fail(ex)
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          if (copyIn.isActive) {
            copyIn.cancelCopy()
          }
        } finally {
          fail(ex)
        }
      }

      private def fail(ex: Throwable): Unit = {
        connectionProvider.release(Some(ex))
        completePromise.tryFailure(ex)
        failStage(ex)
      }

      setHandler(in, this)
    }

    stageLogic -> completePromise.future
  }

  override def shape: SinkShape[ByteString] = SinkShape.of(in)
}
