package com.databricks.spark.xml.table

import org.apache.spark.sql.types.StructType

class XmlTable(
                xmlRootXQuery: String,
                xmlNamespaces: List[XmlNamespace],
                xmlColumns: List[XmlColumn],
                userSchema: StructType
              ) {

  private class XmlColumn{

  }

  val columns: List

  // val columns: List[XmlColumn(name, xpath, type)] = ???

  // XPathExecutable dla XmlColumn
  // XQueryExecutable dla root XQuery

  // Dostarczyć namespaces dla XPathCompiler i XQueryCompiler
  // konwersja typów (na razie tylko String)





}

