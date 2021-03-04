package com.databricks.spark.xml.table

import com.databricks.spark.xml.processor.{XPathHelper, XQueryHelper}
import org.apache.spark.sql.types.StructType


class XmlTable(
                xmlRootXQuery: String,
                xmlNamespaces: Array[XmlNamespace],
                xmlColumns: Array[XmlColumn],
                userSchema: StructType, // not used for now ??
                requestedSchema: StructType
              ) {
  // val rootXQuery: compiled XQuery expression
  val rootXQuery = XQueryHelper.compile(xmlRootXQuery, xmlNamespaces)

  // list of requested columns with compiled XPath queries
  val columns: Array[XmlColumn] = compileRequestedXPaths(requestedSchema, xmlColumns)


  private def compileRequestedXPaths(
                                       requestedSchema: StructType,
                                       xmlColumns: Array[XmlColumn]
                                     ): Array[XmlColumn] = {
      val compiledXpaths: Array[XmlColumn] = for {
            fieldName <- requestedSchema.fieldNames // compile only what's needed
            column <- xmlColumns
            if fieldName == column.name
          } yield (XmlColumn(fieldName, XPathHelper.compile(column.xpath, xmlNamespaces)))

      compiledXpaths
    }




  // val requestedSchema:
  // val columns: List of columns containing prepared xpath expressions, ready for execution
  //              it needs to have exact fields as requested schema because we want to do pruned
  //              scan with every execution


  // val columns: List[XmlColumn(name, xpath, type)] = ???

  // XPathExecutable dla XmlColumn
  // XQueryExecutable dla root XQuery

  // Dostarczyć namespaces dla XPathCompiler i XQueryCompiler
  // konwersja typów (na razie tylko String)





}

