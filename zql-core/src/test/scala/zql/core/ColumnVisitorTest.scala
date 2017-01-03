package zql.core

import org.scalatest.{ Matchers, FlatSpec }

class ColumnVisitorTest extends FlatSpec with Matchers {

  it should "throw unsupported operation if not handling the class" in {
    val visitor = new ColumnVisitor[String, AnyRef]
    try {
      visitor.visit(new StringColumn('test), null)
      assert(false)
    } catch {
      case e: UnsupportedOperationException =>
        assert(true)
      case e2 =>
        e2.printStackTrace()
        assert(false)
    }
  }

  it should "support complain about bad return type" in {
    val visitor = new ColumnVisitor[String, AnyRef] {
      def handle(c: DataColumn, d: AnyRef) = {
        println(c) //bad return type
      }
    }

    try {
      visitor.visit(new StringColumn('test), null)
      assert(false)
    } catch {
      case e =>
        e.printStackTrace()
        assert(true)
    }
  }

  it should "support allow correct return type" in {
    val visitor = new ColumnVisitor[String, AnyRef] {
      def handle(c: TypedDataColumn[_], d: AnyRef): String = {
        return "test"
      }
    }
    visitor.visit(new StringColumn('test), null)

  }

}
