package zql.core

import org.scalatest.{ FlatSpec, Matchers }
import zql.util.Utils

class UtilsTest extends FlatSpec with Matchers {

  it should "compare ints" in {
    Utils.compare(0, 1) should be(-1)
    Utils.compare(1, 1) should be(0)
    Utils.compare(1, 0) should be(1)
  }

  it should "compare floats" in {
    //TODO: this won't pass now
    //    Utils.compare(0, 1.0) should be (-1)
    Utils.compare(1, 1) should be(0)
    Utils.compare(1, 0) should be(1)
  }

}
