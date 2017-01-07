package zql.core

import java.util.Date

import com.sun.org.apache.xalan.internal.xsltc.compiler.sym
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema(val name: String, val alias: String = null) {

  def allColumns(): Seq[ColumnDef]

  def allColumnNames = allColumns.map(_.name)

  def columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap

  def as(alias: Symbol) = {
    val newColumns = allColumns.map {
      c =>
        c.rename(Symbol(alias.name + "." + c.name.name))
    }
    new DefaultSchema(name, newColumns, alias.name)
  }

  def resolveColumnDef(colName: Symbol): ColumnRef = {
    //this uses a lot of returns statement with exception at the end to simplify the code
    val prefixes = List(name, alias).filter(_!=null)

    if (columnMap.contains(colName)) {
      return new ColumnRef(this, columnMap(colName))
    } else if (!colName.name.contains(".")) {
      prefixes.foreach {
        prefix =>
          //try with name prefix
          val newColName = Symbol(s"${prefix}.${colName.name}")
          if (columnMap.contains(newColName)) {
            return new ColumnRef(this, columnMap(colName))
          }
      }
    } else {
      val splits = colName.name.split("\\.")
      if (splits.length == 2) {
        val (tbName, colN) = (splits(0), Symbol(splits(1)))
        if (prefixes.contains(tbName)) {
          if (columnMap.contains(colN)) {
            return new ColumnRef(this, columnMap(colN))
          }
        }
      }
    }
    //throw new IllegalArgumentException(s"Column ${colName} not found in [" + allColumnNames.mkString(", ") + "] with prefixes [" + prefixes.mkString(", ") + "]" )
    null
  }
}

class SimpleSchema(name: String, alias: String = null) extends Schema(name, alias) {
  protected val columns = new ArrayBuffer[ColumnDef]()

  def allColumns = columns

  def o() = this

  def addColumnDef(colDef: ColumnDef) = columns += colDef

  def addColumn[T: ClassTag](sym: Symbol): Unit = addSimpleColDef[Byte](sym)

  def addSimpleColDef[T: ClassTag](sym: Symbol): Unit = addColumnDef(new SimpleColumnDef(sym, scala.reflect.classTag[T].runtimeClass))

  def addSimpleColDef(sym: Symbol, dataType: Class[_]): Unit = columns += new SimpleColumnDef(sym, dataType)

  def BYTE(sym: Symbol) = addColumn[Byte](sym)

  def SHORT(sym: Symbol) = addColumn[Short](sym)

  def INT(sym: Symbol) = addColumn[Int](sym)

  def LONG(sym: Symbol) = addColumn[Long](sym)

  def FLOAT(sym: Symbol) = addColumn[Float](sym)

  def DOUBLE(sym: Symbol) = addColumn[Double](sym)

  def DATE(sym: Symbol) = addColumn[Date](sym)

  def BOOLEAN(sym: Symbol) = addColumn[Boolean](sym)

  def STRING(sym: Symbol) = addColumn[String](sym)

  def CUSTOM[T: ClassTag](sym: Symbol) = addColumn[T](sym)

}

class ReflectedSchema[R: ClassTag](name: String, alias: String = null) extends SimpleSchema(name, alias) {
  @transient implicit val ctag = scala.reflect.classTag[R].runtimeClass

  def resolveGetter(fieldName: String): Getter = {
    val field = Try(ctag.getField(fieldName))
      .getOrElse(null)
    if (field != null) {
      new LazyField(ctag.getCanonicalName, fieldName)
    } else {
      val getter = Try(ctag.getMethod(fieldName))
        .getOrElse(null)
      if (getter != null) {
        new LazyMethod(ctag.getCanonicalName, fieldName)
      } else {
        throw new IllegalArgumentException("Unknown column " + fieldName + " for type " + ctag)
      }
    }
  }

  def i = this

  override def addColumn[T: ClassTag](sym: Symbol) = {
    columns += new ReflectedColumnDef[R](sym, resolveGetter(sym.name))
  }

  def func[B <: R] (sym: Symbol, func: (R) => Any) = addColumnDef(new FuncColumnDef[R](sym, func))

}

class JoinedSchema(tb1: Table, tb2: Table) extends Schema(tb1.name + "_" + tb2.name) {
  val logger = LoggerFactory.getLogger(classOf[JoinedSchema])

  override def allColumns(): Seq[ColumnDef] = {
    val tb1Cols = tb1.schema.allColumns()
    val tb2Cols = tb2.schema.allColumns()
    tb1Cols ++ tb2Cols
  }
  logger.debug("all columns = " + allColumns().map(_.name).mkString(", "))

  override def resolveColumnDef(colName: Symbol): ColumnRef = {
    val colDef1 = tb1.schema.resolveColumnDef(colName)
    val colDef2 = tb2.schema.resolveColumnDef(colName)
    (colDef1, colDef2) match {
      case (a: ColumnRef, null) => a
      case (null, b: ColumnRef) => b
      case (a: ColumnRef, b: ColumnRef) => throw new IllegalArgumentException("Ambiguous column " + colName.name)
      case (null, null) => throw new IllegalArgumentException("Unknown column " + colName.name)
    }
  }
}

abstract class ColumnDef(val name: Symbol) extends Serializable {
  def rename(newName: Symbol): ColumnDef
  def dataType: Class[_]
}

class SimpleColumnDef(name: Symbol, val dataType: Class[_]) extends ColumnDef(name) {
  def rename(newName: Symbol) = new SimpleColumnDef(newName, dataType)
}