package com.databricks.spark.xml.processor

import net.sf.saxon.s9api.{DocumentBuilder, Processor}

object XmlProcessor {

  private val _processor: Processor = new Processor(false)

  val builder: DocumentBuilder = _processor.newDocumentBuilder()

  val xpathCompiler = _processor.newXPathCompiler()

  val xqueryCompiler = _processor.newXQueryCompiler()



}
