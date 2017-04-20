package ru.arigativa.akka.streams

import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import org.postgresql.copy.CopyOut

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}


/**
  * Sinks ByteString as postgres COPY data, returns count of rows copied
  */
private[streams] class PgCopySourceStage(
                                        query: String,
                                        settings: PgCopySourceSettings
                                      ) extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[Long]] {

  private val out = Outlet[ByteString]("PgCopySource.out")

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val completePromise = Promise[Long]()
    val connectionProvider = settings.connectionProvider

    val stageLogic = new GraphStageLogic(shape) with OutHandler {

      private var copyOut: CopyOut = _
      private var bytesCopied: Long = 0

      override def onPull(): Unit = {
        if (copyOut == null) {
          connectionProvider.acquire()
            .map(_.getCopyAPI.copyOut(query)) match {
            case Success(co) =>
              copyOut = co
            case Failure(ex) =>
              fail(ex)
              return
          }
        }
        copyOut.readFromCopy() match {
          case null => success(bytesCopied)
          case bytes =>
            bytesCopied += bytes.size
            push(out, ByteString(bytes))
        }
      }

      override def onDownstreamFinish(): Unit = {
        if (copyOut != null && copyOut.isActive) {
          copyOut.cancelCopy()
          success(bytesCopied)
        }
      }

      private def success(bytesCopied: Long): Unit = {
        if (copyOut != null) {
          connectionProvider.release(None)
        }
        completePromise.trySuccess(bytesCopied)
        completeStage()
      }

      private def fail(ex: Throwable): Unit = {
        if (copyOut != null) {
          connectionProvider.release(Some(ex))
        }
        completePromise.tryFailure(ex)
        failStage(ex)
      }

      setHandler(out, this)
    }

    stageLogic -> completePromise.future
  }

  override def shape: SourceShape[ByteString] = SourceShape.of(out)
}
