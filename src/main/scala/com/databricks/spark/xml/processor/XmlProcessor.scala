package com.databricks.spark.xml.processor

import com.databricks.spark.xml.processor.XmlParser.builder
import com.databricks.spark.xml.table.XmlTable
import net.sf.saxon.s9api.{DocumentBuilder, Processor, XdmNode}
import org.apache.spark.sql.Row
import org.xml.sax.InputSource

import java.io.{ByteArrayInputStream, InputStream}
import javax.xml.transform.sax.SAXSource
import scala.collection.JavaConversions._

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

  // main logic for XmlTable processing
  // must return Iterator[Row] for spark, can be changed to Array[T] for any other use
  def processXmlTable(xmlTable: XmlTable, xmlTree: XdmNode): Iterator[Row] = {
    val rowIterator: Iterator[Row] = Iterator.empty
    // 1. execute XQuery to establish rows
    val xqueryResult = XQueryHelper
                          .prepare(xmlTable.rootXQuery, xmlTree)
                          .iterator()
    // 2. for every row from XQuery execute XPath (for all columns)
      xqueryResult.foreach(contextRow => {
        val rowArray: Array[Any] = xmlTable.columns.map(
          column => {
            // process every column, exec xpath with context item
            XPathHelper.prepare(column.xpath, contextRow)

          }
        )

      })
    // with XQuery returned row as a context item

    // 3. Add row to iterator of rows
    // TODO: Implement XmlTable processing logic
    Iterator(Row())
  }


}
