package zql.list

import zql.core._
import zql.core.util.Utils
import scala.reflect.ClassTag

class ListTable[ROW: ClassTag](schema: RowBasedSchema[ROW], list: List[ROW]) extends RowBasedTable[ROW](schema) {

  val data = new ListData(list)

  override def createTable[T: ClassTag](rowBased: RowBasedData[T], newSchema: RowBasedSchema[T]): RowBasedTable[T] = {
    val list = rowBased.asInstanceOf[ListData[T]].list
    new ListTable(newSchema, list)
  }
}

object ListTable {

  type ROWFUNC[ROW] = (ROW) => Any

  def apply[ROW: ClassTag](cols: ROWFUNC[ROW]*)(data: List[ROW]) = {
    val schema = new RowBasedSchema[ROW](cols.map(_.asInstanceOf[TypedColumnDef[_]]): _*)
    new ListTable[ROW](schema, data)
  }

  def create[ROW: ClassTag](cols: (Symbol, ROWFUNC[ROW])*)(data: List[ROW]) = {
    val typeCols = cols.map {
      case (sym, func) => new TypedColumnDef[ROW](sym) {
        override def apply(v1: ROW): Any = func.apply(v1)
      }
    }
    val schema = new RowBasedSchema[ROW](typeCols: _*)
    new ListTable[ROW](schema, data)
  }
}

class ListData[ROW](val list: List[ROW], option: CompileOption = new CompileOption) extends RowBasedData[ROW] {

  implicit def listToListData[T](list: List[T]) = new ListData[T](list, option)

  override def select(r: (ROW) => Row): RowBasedData[Row] = list.map(r).toList

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = list.filter(filter)

  override def groupBy(keyFunc: (ROW) => Seq[Any], valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    Utils.groupBy[ROW, Row](list, keyFunc(_), valueFunc(_), _.aggregate(_, aggregatableIndices)).map(_.normalize).toList
  }

  override def reduce(reduceFunc: (ROW, ROW) => ROW) = List(list.reduce(reduceFunc))

  override def map(mapFunc: (ROW) => Row) = list.map(mapFunc)

  override def sortBy[K](keyFunc: (ROW) => K, ordering: Ordering[K], ctag: ClassTag[K]) = new ListData(list.sortBy(keyFunc)(ordering))

  override def slice(offset: Int, until: Int) = list.slice(offset, until)

  override def size() = list.length

  override def asList = list

  override def isLazy = false

  override def withOption(opt: CompileOption): RowBasedData[ROW] = new ListData(list, opt)

  override def distinct() = list.distinct
}