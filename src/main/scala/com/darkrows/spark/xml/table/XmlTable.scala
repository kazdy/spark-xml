package com.darkrows.spark.xml.table

import com.darkrows.spark.xml.processor.{XPathHelper, XQueryHelper}
import com.darkrows.spark.xml.processor.XPathHelper
import net.sf.saxon.s9api.{XPathExecutable, XQueryExecutable}
import org.apache.spark.sql.types.StructType


class XmlTable  (
                val xmlRootXQuery: String,
                val xmlNamespaces: Array[XmlNamespace],
                val xmlColumns: Array[XmlColumn],
                val requestedSchema: StructType
              ) extends Serializable {

  @transient val compiledXpaths = compileRequestedXPaths()
  @transient val compiledRootXQuery = compileRootXQuery()

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

