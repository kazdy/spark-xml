package com.databricks.spark.xml.table

import com.databricks.spark.xml.processor.{XPathHelper, XQueryHelper}
import net.sf.saxon.s9api.{XPathExecutable, XQueryExecutable}
import org.apache.spark.sql.types.StructType


class XmlTable  (
                val xmlRootXQuery: String,
                val xmlNamespaces: Array[XmlNamespace],
                val xmlColumns: Array[XmlColumn],
                val requestedSchema: StructType
              ) extends Serializable {


  def compileRequestedXPaths(): Array[XmlCompiledColumn] = {
    val compiledXpaths: Array[XmlCompiledColumn] = for {
      fieldName <- requestedSchema.fieldNames // compile only what's needed
      fieldType <- requestedSchema.map(x => x.dataType) // need to know datatype for conversions
      column <- xmlColumns
      if fieldName == column.name
    } yield XmlCompiledColumn(fieldName,
      XPathHelper.compile(column.xpath, xmlNamespaces),
      fieldType)
    compiledXpaths
  }

  def compileRootXQuery(): XQueryExecutable = {
    XQueryHelper.compile(xmlRootXQuery, xmlNamespaces)
  }




}

