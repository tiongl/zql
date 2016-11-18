package zql.core

import java.lang.reflect.Field

import scala.reflect.ClassTag

/**
  * Created by tiong on 6/2/16.
  */

abstract class Schema {
  def columnMap(): Map[Symbol, Column]
}

abstract class Table(val schema: Schema) {
  def select(selects: Column*): Selected = {
    new Selected(selects, this)
  }

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]
}


class ReflectedSchema[T](implicit ctag: reflect.ClassTag[T]) extends Schema{
  val fields = ctag.runtimeClass.getFields.map(f => (f.getName, f)).toMap
  val columnMap = fields.map{case (name, field) => (Symbol(name), toColumn(field))}

  def toColumn(f: Field): Column = {
    if (f.getType.isAssignableFrom(classOf[Number])){
      new NumericNamedColumn(Symbol(f.getName))
    } else if (f.getType.isAssignableFrom(classOf[String])) {
      new StringNamedColumn(Symbol(f.getName))
    } else {
      new GenericNamedColumn(Symbol(f.getName))
    }
  }
}


class Row(val data: Array[Any]){
  def aggregate(row: Row, indices: Array[Int]): Row = {
    //TODO: make sure this won't have side effect as we use shallow copy
    val newRow = new Row(data)
    indices.foreach{i => newRow.data(i) = data(i).asInstanceOf[Aggregatable].aggregate(row.data(i).asInstanceOf[Aggregatable])}
    newRow
  }

  override def toString = data.mkString(",")
}

class ListTable[T: ClassTag](val data: List[T]) extends Table(new ReflectedSchema[T]()) {
  implicit val ctag = scala.reflect.classTag[T].runtimeClass

  type Getter[E] = (T) => E

  class DefaultCompiler(schema: Schema) {
    def compile(stmt: Statement): Executable[Table] = {
      import zql.core.ExecutionPlan._
      val execPlan = plan("Query"){
        first("Filter the data"){
          val filteredData = if (stmt._where!=null){
            val filterExtractor = compile[T](stmt._where)
            data.filter(d => filterExtractor.get(d).asInstanceOf[Boolean])
          } else data
          filteredData
        }.next("Grouping the data") {
          filteredData =>
            val selects = stmt._selects.map(compile[T](_))
            val groupByIndices = selects.zipWithIndex.filter(_._1.isInstanceOf[Aggregator[_]]).map(_._2).toArray
            val groupedProcessData = if (stmt._groupBy!=null){
              val groupByExtractor = stmt._groupBy.map(compile(_))
              val groupedList = data.groupBy{
                d => groupByExtractor
              }.map(_._2)
              val groupedData = groupedList.map{
                list => list.map{
                  data => new Row(selects.map(_.get(data)).toArray)
                }.reduce [Row]{
                  case (a: Row, b: Row) => a.aggregate(b, groupByIndices)
                }
              }
              val havingData = if (stmt._having!=null){
                val havingExtractor = compile[Row](stmt._having)
                groupedData.filter(d => havingExtractor.get(d).asInstanceOf[Boolean])
              } else {
                groupedData
              }
              havingData
            } else if (groupByIndices.size>0){//this will trigger group by all
              val singleRow = data.map(
                row => new Row(selects.map(_.get(row)).toArray)
              ).reduce[Row] {
                case (a, b) => a.aggregate(b, groupByIndices)
              }
              List(singleRow)
            }
            else {
              data.map{
                d =>
                  selects.map(_.get(d))
              }
            }
            groupedProcessData
        }.next("Return the table") {
          groupedProcessData =>
          new ListTable(groupedProcessData.toList)
        }
      }
      execPlan
    }

    def compile[T: ClassTag](column: Column): ValueGetter[T] = {
      column match {
          case nc: NamedColumn[T] =>
            new ReflectionGetter[T](nc.name.name)
          case lc: LiteralColumn[T] =>
            new LiteralGetter[T](lc.value)
          case ac: AndCondition =>
            new AndGetter[T](
              compile(ac.a),
              compile(ac.b)
            )
          case oc: OrCondition =>
            new OrGetter[T](
              compile(oc.a),
              compile(oc.b)
            )
          case sc: Sum =>
            new SumGetter[T](compile(sc.column))


      }
    }
//
//    def compileCondition(condition: Condition): Extractor[Boolean] = {
//      compile[Boolean](condition: Column)
//    }
  }

  def compile(stmt: Statement) = {
    new DefaultCompiler(schema).compile(stmt)
  }

  override def collectAsList = data
}

object Example {
  def main(args: Array[String]) {
    case class Person(firstName: String, lastName: String)

    val listTable = new ListTable[Person](List(new Person("adam", "smith"), new Person("john", "smith")))

    val selected = listTable select('firstName, 'lastName, "Test")

    val where1 = selected where (('firstName === 'lastName) and ('firstName !== 'lastName))
    val where2 = selected groupBy('firstName)
    val where3 = selected orderBy ('firstName)
    val where4 = selected limit(1, 10)

    val groupby = where1 groupBy('firstName)

    val having = groupby having('test === 'test)
    val order = groupby orderBy('test)
    val limit = groupby limit (1, 10)

    selected.compile
  }
}


