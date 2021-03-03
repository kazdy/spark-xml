package com.databricks.spark.xml.table

import org.apache.spark.sql.types.StructType

class XmlTable(
              rootXQuery: String,
              namespaces: List[XmlNamespace],
              columns: List[XmlColumn],
              userSchema: StructType
              ) {

  // val columns: List[XmlColumn(name, xpath, type)] = ???
  //
  // XPathExecutable dla XmlColumn
  // XQueryExecutable dla root XQuery

  // Dostarczyć namespaces dla XPathCompiler i XQueryCompiler
  // konwersja typów (na razie tylko String)





}
