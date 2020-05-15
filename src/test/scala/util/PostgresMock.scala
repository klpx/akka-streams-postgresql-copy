package util

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.postgresql.PGConnection
import org.postgresql.copy.{CopyIn, CopyManager}
import org.scalatestplus.mockito.MockitoSugar

import scala.reflect.ClassTag

/**
  * Created by hsslbch on 1/14/17.
  */
trait PostgresMock extends BaseFixture with MockitoSugar {
  def withCopyInMockedPostgres[T : ClassTag](testCode: (CopyIn, PGConnection) => T): T = {
    val conn = mock[PGConnection]
    val copyAPI = mock[CopyManager]
    val copyIn = mock[CopyIn]

    when(copyAPI.copyIn(any())).thenReturn(copyIn)
    when(conn.getCopyAPI).thenReturn(copyAPI)

    withOneArg(copyIn -> conn)(testCode.tupled) {}
  }
}
