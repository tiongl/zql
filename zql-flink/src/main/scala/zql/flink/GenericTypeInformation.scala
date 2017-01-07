package zql.flink

import java.io._
import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.{TypeComparatorBuilder, FlatFieldDescriptor}
import org.apache.flink.api.common.typeutils.{TypeComparator, CompositeType, TypeSerializer}
import org.apache.flink.api.java.typeutils.runtime.TupleComparatorBase
import org.apache.flink.core.memory.{MemorySegment, DataOutputView, DataInputView}
import zql.core.Row

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by tiong on 1/3/17.
  */
class GenericTypeInfo[T: ClassTag](func: () => T, numOfFields: Int = 1, isKey: Boolean = false) extends TypeInformation[T] {
  override def isBasicType: Boolean = false

  override def getTotalFields: Int = numOfFields

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[Row]

  override def createSerializer(config: ExecutionConfig): TypeSerializer[T] = new GenericTypeSerializer[T](func)

  override def getArity: Int = numOfFields

  override def isKeyType: Boolean = isKey

  override def getTypeClass: Class[T] = scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def isTupleType: Boolean = false

  override def toString: String = "RowTypeInformation"

  override def equals(obj: Any): Boolean = false

  override def hashCode: Int = 0
}

class GenericTypeSerializer[T: ClassTag](func: ()=>T) extends TypeSerializer[T] {
  override def createInstance(): T = func.apply()

  override def getLength: Int = -1

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[Row]

  override def copy(from: T): T = {
    val stream = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(from)
    oos.close
    val istream = new ByteArrayInputStream(stream.toByteArray)
    val ois = new ObjectInputStream(istream)
    ois.readObject().asInstanceOf[T]
  }

  override def copy(from: T, reuse: T): T = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val length = source.readInt()
    val bytes = new Array[Byte](length)
    source.readFully(bytes)
    target.writeInt(length)
    target.write(bytes)
  }

  override def serialize(record: T, target: DataOutputView): Unit = {
    val stream = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(record)
    oos.close
    val data = stream.toByteArray
    target.writeInt(data.length)
    target.write(data)
  }

  override def isImmutableType: Boolean = true

  override def duplicate(): TypeSerializer[T] = new GenericTypeSerializer(func)

  override def deserialize(source: DataInputView): T = {
    val length = source.readInt()
    val bytes = new Array[Byte](length)
    source.readFully(bytes)
    val stream = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(stream)
    ois.readObject().asInstanceOf[T]
  }

  override def deserialize(reuse: T, source: DataInputView): T = deserialize(source)

  override def toString: String = "RowTypeSerializer"

  override def equals(obj: Any): Boolean = false

  override def hashCode: Int = 0
}

class GenericCompositeTypeInfo[T: ClassTag] extends CompositeType[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]) {
  override def getTypeAt[X](fieldExpression: String): TypeInformation[X] = ???

  override def getTypeAt[X](pos: Int): TypeInformation[X] = ???

  override def getFieldIndex(fieldName: String): Int = ???

  override def createTypeComparatorBuilder(): TypeComparatorBuilder[T] = ???

  override def getFlatFields(fieldExpression: String, offset: Int, result: util.List[FlatFieldDescriptor]): Unit = ???

  override def getFieldNames: Array[String] = ???

  override def isBasicType: Boolean = ???

  override def getTotalFields: Int = ???

  override def createSerializer(config: ExecutionConfig): TypeSerializer[T] = ???

  override def getArity: Int = ???

  override def isTupleType: Boolean = ???
}

class RowTypeInfo(typeInfo: TypeInformation[_]*) extends CompositeType[Row](classOf[Row]) {
  override def getTypeAt[X](fieldExpression: String): TypeInformation[X] = ???

  override def getTypeAt[X](pos: Int): TypeInformation[X] = typeInfo(pos).asInstanceOf[TypeInformation[X]]

  override def getFieldIndex(fieldName: String): Int = ???

  override def createTypeComparatorBuilder(): TypeComparatorBuilder[Row] = new RowComparatorBuilder

  override def getFlatFields(fieldExpression: String, offset: Int, result: util.List[FlatFieldDescriptor]): Unit = {
    val descriptors = typeInfo.zipWithIndex.foreach {
      case (info, index) =>
        result.add(new FlatFieldDescriptor(index, info))
    }
  }

  override def getFieldNames: Array[String] = ???

  override def isBasicType: Boolean = false

  override def getTotalFields: Int = typeInfo.length

  override def createSerializer(config: ExecutionConfig): TypeSerializer[Row] = new GenericTypeSerializer[Row](() => new Row(Array()))

  override def getArity: Int = typeInfo.length

  override def isTupleType: Boolean = false

  class RowComparatorBuilder extends TypeComparatorBuilder[Row] {
    val fieldComparators = new ArrayBuffer[TypeComparator[_]]()
    val logicalKeyFields = new ArrayBuffer[Integer]()

    override def addComparatorField(fieldId: Int, comparator: TypeComparator[_]): Unit = {
      fieldComparators.append(comparator)
      logicalKeyFields.append(fieldId)
    }

    override def initializeTypeComparatorBuilder(size: Int): Unit = {

    }

    override def createTypeComparator(config: ExecutionConfig): TypeComparator[Row] = new RowTypeComparator(config) {


    }
  }

  class RowTypeComparator(config: ExecutionConfig) extends
    TupleComparatorBase[Row](Array[Int](), Array[TypeComparator[_]](), Array[TypeSerializer[_]]()) with Serializable {

    val serializer = createSerializer(config)

    var reference: Row = null

    override def putNormalizedKey(record: Row, target: MemorySegment, offset: Int, numBytes: Int): Unit = ???

    override def setReference(toCompare: Row): Unit = reference = toCompare

    override def compare(first: Row, second: Row): Int = first.compareTo(second)

    override def hash(record: Row): Int = record.hashCode

    override def extractKeys(record: scala.Any, target: Array[AnyRef], index: Int): Int = 0

    override def equalToReference(candidate: Row): Boolean = reference.compareTo(candidate) == 0

    override def duplicate(): TypeComparator[Row] = new RowTypeComparator(config)
  }

}