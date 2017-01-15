package unit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, Matchers}
import ru.arigativa.akka.streams.PgCopyStreamConverters

import scala.concurrent.Future

/**
  * Created by hsslbch on 1/14/17.
  */
class EncodeTuplesSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  override def afterAll(): Unit = {
    system.terminate()
  }

  def encodeTuples(input: Seq[Product]): Future[ByteString] = {
    Source.fromIterator(() => input.iterator)
      .via(PgCopyStreamConverters.encodeTuples())
      .runWith(Sink.fold(ByteString("")) {
        case (acc, next) => acc ++ next
      })
  }

  "encodeTuples" should "encode simple values" in {
    for {
      result <- encodeTuples(Seq(
        (1L, "Alex"),
        (2L, "Liza")
      ))
    } yield {
      result shouldBe ByteString("1\tAlex\n2\tLiza\n")
    }
  }

  it should "handle Option values" in {
    for {
      result <- encodeTuples(Seq(
        (1L, "Alex", Some("tool")),
        (2L, "Liza", None)
      ))
    } yield {
      result shouldBe ByteString("1\tAlex\ttool\n2\tLiza\t\\N\n")
    }
  }

  it should "handle null values" in {
    for {
      result <- encodeTuples(Seq(
        (1L, "Alex"),
        (2L, null)
      ))
    } yield {
      result shouldBe ByteString("1\tAlex\n2\t\\N\n")
    }
  }

}
