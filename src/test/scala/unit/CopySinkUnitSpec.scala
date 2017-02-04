package unit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import ru.arigativa.akka.streams.ConnectionProvider._
import ru.arigativa.akka.streams.{PgCopySinkSettings, PgCopyStreamConverters}
import util.PostgresMock

import scala.util.{Failure, Success}

/**
  * Created by hsslbch on 1/14/17.
  */
class CopySinkUnitSpec extends AsyncFlatSpec with Matchers with MockitoSugar with PostgresMock with BeforeAndAfterAll {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  override def afterAll(): Unit = {
    system.terminate()
  }

  private def CheckOrder(mocks: AnyRef*) = Mockito.inOrder(mocks: _*)

  "PgCopySink" should "write all bytes as is" in {
    val input = (0 to 255).map(_.toByte).toArray
    withCopyInMockedPostgres { (copyIn, conn) =>
      Source.single(ByteString(input)).runWith(PgCopyStreamConverters.bytesSink("", PgCopySinkSettings(conn)))
        .map { _ =>
          val checkOrder = CheckOrder(copyIn)
          checkOrder.verify(copyIn).writeToCopy(input, 0, input.length)
          checkOrder.verify(copyIn).endCopy()
          verifyNoMoreInteractions(copyIn)
          succeed
        }
    }
  }

  it should "not open connection before initial buffer get filled" in {
    val partSize = 256
    val inputPart = (0 until partSize).map(_.toByte).toArray
    val input = (1 to 10).map(_ => ByteString(inputPart))
    val bufferParts = 5

    withCopyInMockedPostgres { (copyIn, conn) =>
      Source.fromIterator(() => input.iterator)
        .runWith(PgCopyStreamConverters.bytesSink("", PgCopySinkSettings(conn, initialBufferSize = bufferParts*partSize)))
        .map { _ =>
          val checkOrder = CheckOrder(copyIn)
          // check buffer flushes
          checkOrder.verify(copyIn).writeToCopy(
            input
              .take(bufferParts)
              .foldLeft(ByteString.empty)(_++_)
              .toArray,
            0, bufferParts*partSize
          )
          checkOrder.verify(copyIn, times(5)).writeToCopy(inputPart, 0, inputPart.length)
          checkOrder.verify(copyIn).endCopy()
          verifyNoMoreInteractions(copyIn)
          succeed
        }
    }
  }

  it should "cancel copy on upstream fail if copy is active" in {
    val inputError = new Error("some error")
    val input = List(Success(ByteString("ok")), Failure(inputError))
    withCopyInMockedPostgres { (copyIn, conn) =>
      when(copyIn.isActive).thenReturn(true)
      Source(input).map(_.get).runWith(PgCopyStreamConverters.bytesSink("", PgCopySinkSettings(conn)))
        .map(_ => fail("Stream should fail"))
        .recover {
          case outputError =>
            outputError.getCause shouldBe inputError
            verify(copyIn).writeToCopy("ok".getBytes, 0, 2)
            verify(copyIn).isActive
            verify(copyIn).cancelCopy()
            verifyNoMoreInteractions(copyIn)
            succeed
        }
    }
  }
}
