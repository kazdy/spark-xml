package com.databricks.spark.xml.processor

import com.databricks.spark.xml.processor.XmlParser.builder
import net.sf.saxon.s9api.{DocumentBuilder, Processor, XdmNode}
import org.apache.spark.sql.Row
import org.xml.sax.InputSource

import java.io.{ByteArrayInputStream, InputStream}
import javax.xml.transform.sax.SAXSource

object XmlProcessor {

// static objects for use in other classes
  private val _processor: Processor = new Processor(false)

  val builder: DocumentBuilder = _processor.newDocumentBuilder()

  val xpathCompiler = _processor.newXPathCompiler()

  val xqueryCompiler = _processor.newXQueryCompiler()

// Methods for building XDM from string
  private def toInputStream(xml: String): InputStream = {
    new ByteArrayInputStream(xml.getBytes())
  }

  private def toSAXSource(stream: InputStream): SAXSource = {
    new SAXSource(new InputSource(stream))
  }

  private def buildSource(source: SAXSource): XdmNode = {
    builder.build(source)
  }

  def buildXdmTree(xml: String): XdmNode = {
    val inputStream = toInputStream(xml)
    val SAXSource = toSAXSource(inputStream)
    val xdmNode = buildSource(SAXSource)
    xdmNode
  }

  def processXmlTable(): Iterator[Row] = ???


}
