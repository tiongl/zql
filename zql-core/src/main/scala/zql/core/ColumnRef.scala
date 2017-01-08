package zql.core

import zql.schema.{ ColumnDef, Schema }

case class ColumnRef(val schema: Schema, val colDef: ColumnDef)
