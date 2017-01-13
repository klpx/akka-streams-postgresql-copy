package unit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.github.klpx.akka.PgCopyStreamConverters
import org.scalatest.{AsyncFlatSpec, Matchers}
import util.ActorSystemFixture
import akka.util

import scala.concurrent.Future

/**
  * Created by hsslbch on 1/14/17.
  */
class EncodeTuplesSpec extends AsyncFlatSpec with Matchers with ActorSystemFixture {

  def encodeTuples(input: Seq[Product])(implicit system: ActorSystem): Future[ByteString] = {
    implicit val mat = ActorMaterializer()
    Source.fromIterator(() => input.iterator)
      .via(PgCopyStreamConverters.encodeTuples())
      .runWith(Sink.fold(ByteString("")) {
        case (acc, next) => acc ++ next
      })
  }

  "encodeTuples" should "encode simple values" in withActorSystem { implicit system =>
    for {
      result <- encodeTuples(Seq(
        (1L, "Alex"),
        (2L, "Liza")
      ))
    } yield {
      result shouldBe ByteString("1\tAlex\n2\tLiza\n")
    }
  }

}
