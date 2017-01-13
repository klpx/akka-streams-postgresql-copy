package util

import akka.actor.ActorSystem

import scala.reflect.ClassTag

/**
  * Created by hsslbch on 11/16/16.
  */
trait ActorSystemFixture extends BaseFixture {
  def withActorSystem[T : ClassTag](code: ActorSystem => T): T = {
    val as = ActorSystem()
    withOneArg(as)(code) {
      as.terminate()
    }
  }
}
