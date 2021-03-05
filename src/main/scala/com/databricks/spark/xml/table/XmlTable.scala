package com.databricks.spark.xml.table

import com.databricks.spark.xml.processor.{XPathHelper, XQueryHelper}
import net.sf.saxon.s9api.XPathExecutable
import org.apache.spark.sql.types.StructType


class XmlTable  (
                val xmlRootXQuery: String,
                val xmlNamespaces: Array[XmlNamespace],
                val xmlColumns: Array[XmlColumn],
                val requestedSchema: StructType
              ) extends Serializable {

}

