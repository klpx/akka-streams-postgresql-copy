package ru.arigativa.akka.streams

import akka.stream.ActorAttributes.Dispatcher
import akka.stream._
import akka.stream.impl.Stages.DefaultAttributes.IODispatcher
import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import org.postgresql.copy.CopyOut

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}


/**
  * Sinks ByteString as postgres COPY data, returns count of rows copied
  */
private[streams] class PgCopySourceStage(
                                        query: String,
                                        settings: PgCopySourceSettings
                                      ) extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[Long]] {

  private val out = Outlet[ByteString]("PgCopySource.out")

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val dispatcherId = "akka.stream.default-blocking-io-dispatcher"

    val completePromise = Promise[Long]()
    val connectionProvider = settings.connectionProvider

    val stageLogic = new GraphStageLogic(shape) with OutHandler {

      private var copyOut: CopyOut = _
      private var bytesCopied: Long = 0

      private implicit var executionContext: ExecutionContext = _

      private val downstreamCallback: AsyncCallback[Try[Option[ByteString]]] =
        getAsyncCallback {
          case Success(None)       => success(bytesCopied)
          case Success(Some(elem)) => push(out, elem)
          case Failure(ex)         => fail(ex)
        }

      override def preStart(): Unit = {
        executionContext = materializer.asInstanceOf[ActorMaterializer].system.dispatchers.lookup(dispatcherId)
        super.preStart()
      }

      override def onPull(): Unit = {
        Future {
          blocking {
            if (copyOut == null) {
              val conn = connectionProvider.acquire().get
              copyOut = conn.getCopyAPI.copyOut(query)
            }
            Option(copyOut.readFromCopy())
              .map { bytes =>
                bytesCopied += bytes.length
                ByteString(bytes)
              }
          }
        }.onComplete(downstreamCallback.invoke)
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
