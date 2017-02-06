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
  * @param connectionProvider Object that provides connection and releases it when Sink is complete
  * @param initialBufferSize COPY won't start until initial buffer is filled
  */
case class PgCopySinkSettings(
                             connectionProvider: ConnectionProvider,
                             initialBufferSize: Long = 0
                             )

/**
  * Sinks ByteString as postgres COPY data, returns count of rows copied
  */
private[streams] class PgCopySinkStage(
                                        query: String,
                                        settings: PgCopySinkSettings
                                      ) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]] {

  private val in = Inlet[ByteString]("PgCopySink.in")

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val completePromise = Promise[Long]()
    val maxInitialBufferSize = settings.initialBufferSize
    val connectionProvider = settings.connectionProvider

    val stageLogic = new GraphStageLogic(shape) with InHandler {

      private var initialBuffer: ByteString = ByteString.empty
      private var copyIn: CopyIn = _

      override def preStart(): Unit = {
        pull(in)
      }

      private def initConnectionAndWriteBuffer(): Unit = {
        connectionProvider.acquire() match {
          case Success(conn) =>
            try {
              copyIn = conn.getCopyAPI.copyIn(query)
              copyIn.writeToCopy(initialBuffer.toArray, 0, initialBuffer.length)
            } catch {
              case ex: Throwable => fail(ex)
            }
          case Failure(ex) => fail(ex)
        }
      }

      def onPush(): Unit = {
        val buf = grab(in)
        try {
          if (copyIn == null) {
            initialBuffer = initialBuffer ++ buf
            if (initialBuffer.size >= maxInitialBufferSize) {
              initConnectionAndWriteBuffer()
            }
          } else {
            copyIn.writeToCopy(buf.toArray, 0, buf.length)
          }
          pull(in)
        } catch {
          case ex: Throwable => fail(ex)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (copyIn == null && initialBuffer.isEmpty) success(0)
        else {
          if (copyIn == null) {
            initConnectionAndWriteBuffer()
          }
          Try(copyIn.endCopy()) match {
            case Success(rowsCopied) => success(rowsCopied)
            case Failure(ex) => fail(ex)
          }
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          if (copyIn != null && copyIn.isActive) {
            copyIn.cancelCopy()
          }
        } finally {
          fail(ex)
        }
      }

      private def success(rowsCopied: Long): Unit = {
        if (copyIn != null) {
          connectionProvider.release(None)
        }
        completePromise.trySuccess(rowsCopied)
        completeStage()
      }

      private def fail(ex: Throwable): Unit = {
        if (copyIn != null) {
          connectionProvider.release(Some(ex))
        }
        completePromise.tryFailure(ex)
        failStage(ex)
      }

      setHandler(in, this)
    }

    stageLogic -> completePromise.future
  }

  override def shape: SinkShape[ByteString] = SinkShape.of(in)
}
