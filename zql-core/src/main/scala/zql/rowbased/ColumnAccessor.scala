package zql.rowbased

/** allow accessing a column from type T **/
abstract class ColumnAccessor[ROW, +T]() extends ((ROW) => T) with Serializable

abstract class ConditionAccessor[ROW]() extends ColumnAccessor[ROW, Boolean]

