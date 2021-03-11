package com.darkrows.spark.xml.table

import net.sf.saxon.s9api.XPathExecutable
import org.apache.spark.sql.types.DataType

case class XmlCompiledColumn(name: String, xpath: XPathExecutable, dataType: DataType)



