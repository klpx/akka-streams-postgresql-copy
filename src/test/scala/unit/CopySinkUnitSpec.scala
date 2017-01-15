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
import ru.arigativa.akka.streams.PgCopyStreamConverters
import util.PostgresMock

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
      Source.single(ByteString(input)).runWith(PgCopyStreamConverters.bytesSink(conn, ""))
        .map { _ =>
          val checkOrder = CheckOrder(copyIn)
          checkOrder.verify(copyIn).writeToCopy(input, 0, input.length)
          checkOrder.verify(copyIn).endCopy()
          verifyNoMoreInteractions(copyIn)
          succeed
        }
    }
  }

  it should "cancel copy on upstream fail if copy is active" in {
    val inputError = new Error("some error")
    withCopyInMockedPostgres { (copyIn, conn) =>
      when(copyIn.isActive).thenReturn(true)
      Source.failed(inputError).runWith(PgCopyStreamConverters.bytesSink(conn, ""))
        .map(_ => fail("Stream should fail"))
        .recover {
          case outputError =>
            outputError.getCause shouldBe inputError
            verify(copyIn).isActive
            verify(copyIn).cancelCopy()
            verifyNoMoreInteractions(copyIn)
            succeed

        }
    }
  }

  it should "do nothing on upstream fail if copy is not active" in {
    val inputError = new Error("some error")
    withCopyInMockedPostgres { (copyIn, conn) =>
      when(copyIn.isActive).thenReturn(false)
      Source.failed(inputError).runWith(PgCopyStreamConverters.bytesSink(conn, ""))
        .map(_ => fail("Stream should fail"))
        .recover {
          case outputError =>
            outputError.getCause shouldBe inputError
            verify(copyIn).isActive
            verifyNoMoreInteractions(copyIn)
            succeed
        }
    }
  }
}
