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

    val compiledXpaths: Seq[XmlCompiledColumn] = for {
      requestedField <- requestedSchema // compile only what's needed
      column <- xmlColumns
      if requestedField.name == column.name
    } yield XmlCompiledColumn(requestedField.name,
      XPathHelper.compile(column.xpath, xmlNamespaces),
      requestedField.dataType)

    compiledXpaths.toArray
  }

  def compileRootXQuery(): XQueryExecutable = {
    XQueryHelper.compile(xmlRootXQuery, xmlNamespaces)
  }




}

