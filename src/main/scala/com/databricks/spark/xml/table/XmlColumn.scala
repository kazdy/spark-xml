package com.databricks.spark.xml.table

import net.sf.saxon.s9api.XPathExecutable


case class XmlColumn(name: String, xpath: String)

object XmlColumn {
  // Alternate constructor
  def apply(name: String, xpath: XPathExecutable): XmlColumn = {
    XmlColumn(name, xpath)
  }
}
