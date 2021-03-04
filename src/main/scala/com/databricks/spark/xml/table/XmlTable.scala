package com.databricks.spark.xml.table

import com.databricks.spark.xml.processor.{XPathHelper, XQueryHelper}
import net.sf.saxon.s9api.XPathExecutable
import org.apache.spark.sql.types.StructType


class XmlTable(
                xmlRootXQuery: String,
                xmlNamespaces: Array[XmlNamespace],
                xmlColumns: Array[XmlColumn],
                requestedSchema: StructType
              ) {
  // val rootXQuery: compiled XQuery expression
  val rootXQuery = XQueryHelper.compile(xmlRootXQuery, xmlNamespaces)

  // list of requested columns with compiled XPath queries
  // contextItem (rootXQuery result) should be specified just before execution
  val columns: Array[Column] = compileRequestedXPaths(requestedSchema, xmlColumns)


  private def compileRequestedXPaths(
                                       requestedSchema: StructType,
                                       xmlColumns: Array[XmlColumn]
                                     ): Array[XmlColumn] = {
      val compiledXpaths: Array[XmlColumn] = for {
            fieldName <- requestedSchema.fieldNames // compile only what's needed
            column <- xmlColumns
            if fieldName == column.name
          } yield (Column(fieldName, XPathHelper.compile(column.xpath, xmlNamespaces)))

      compiledXpaths
    }

  private case class Column(name: String, xpath: XPathExecutable)
}

