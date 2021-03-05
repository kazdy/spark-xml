/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml.processor

import java.io.{ByteArrayInputStream, StringReader}
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{Attribute, Characters, EndElement, StartElement, XMLEvent}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.Iterator._
import scala.util.control.NonFatal
import scala.util.Try
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.XmlOptions
import com.databricks.spark.xml.processor.XmlProcessor.buildXdmTree
import com.databricks.spark.xml.table.{XmlColumn, XmlTable}
import com.databricks.spark.xml.util._
import net.sf.saxon.s9api.{DocumentBuilder, Processor, XPathExecutable, XdmItem, XdmNode, XdmSequenceIterator, XdmValue}
import org.apache.spark.{Partition, TaskContext}

import java.io.{ByteArrayInputStream, InputStream}
import javax.xml.transform.sax.SAXSource
import org.xml.sax.InputSource

import scala.collection.JavaConversions._

/**
 * Wraps parser to iteration process.
 */
private[xml] object XmlParser extends Serializable {
  private val logger = LoggerFactory.getLogger(XmlParser.getClass)

  // TODO: change parse implementation to return XMLTABLE as RDD[Row]
  def parse(
             xml: RDD[String],
             xmlTable: XmlTable,
             schema: StructType,
             options: XmlOptions): RDD[Row] = {
    xml.mapPartitions(iter => iter.flatMap(xml => parseXmlTable(xml, xmlTable)) )
  }

  def parseXmlTable(xml: String, xmlTable: XmlTable): Iterator[Row] = {
    val xmlTree = XmlProcessor.buildXdmTree(xml)
    processXmlTable(xmlTable, xmlTree)
  }

  // main logic for XmlTable processing
  // must return Iterator[Row] for spark, can be changed to Array[T] for any other use
  def processXmlTable(xmlTable: XmlTable, xmlTree: XdmNode): Iterator[Row] = {
    case class Column(name: String, xpath: XPathExecutable)
    // val rootXQuery: compiled XQuery expression
    val rootXQuery = XQueryHelper.compile(xmlTable.xmlRootXQuery, xmlTable.xmlNamespaces)

    def compileRequestedXPaths(
                                        requestedSchema: StructType,
                                        xmlColumns: Array[XmlColumn]
                                      ): Array[Column] = {
      val compiledXpaths: Array[Column] = for {
        fieldName <- requestedSchema.fieldNames // compile only what's needed
        column <- xmlColumns
        if fieldName == column.name
      } yield (Column(fieldName, XPathHelper.compile(column.xpath, xmlTable.xmlNamespaces)))

      compiledXpaths
    }

    // list of requested columns with compiled XPath queries
    // contextItem (rootXQuery result) should be specified just before execution
    val columns: Array[Column] = compileRequestedXPaths(xmlTable.requestedSchema, xmlTable.xmlColumns)

    // 1. execute XQuery to establish rows
    val xqueryResult = XQueryHelper
      .prepare(rootXQuery, xmlTree)
      .iterator()
    // 2. for every row from XQuery execute XPath (for all columns)
    // with XQuery returned row as a context item
    val resultIterator = xqueryResult.map(contextRow => {
      val rowArray: Array[Any] = columns.map(
        column => {
          // process every column, exec xpath with context item
          val xpathResult = XPathHelper.prepare(column.xpath, contextRow).evaluate()
          // return null if empty, return string value if not empty
          if(xpathResult.isEmpty) "null" else xpathResult.getUnderlyingValue.getStringValue
        }
      )
      Row.fromSeq(rowArray)
    })

    resultIterator

  }
      // Array(Row.fromSeq(Seq("2", "nie daniel"))).toIterator

}



