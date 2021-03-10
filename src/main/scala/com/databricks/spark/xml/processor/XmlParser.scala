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

import com.databricks.spark.xml.XmlOptions
import com.databricks.spark.xml.table.{XmlCompiledColumn, XmlTable}
import com.databricks.spark.xml.util.TypeCast.{castTo, convertTo}
import net.sf.saxon.s9api.{XdmItem, XdmNode, XdmSequenceIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

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
             options: XmlOptions): RDD[Row] = {
    xml.mapPartitions(iter => iter.flatMap(xml => parseXmlTable(xml, xmlTable, options)) )
  }

  private def parseXmlTable(xml: String, xmlTable: XmlTable, options: XmlOptions): Iterator[Row] = {
    val xmlTree = XmlProcessor.buildXdmTree(xml)
    processXmlTable(xmlTable, xmlTree, options)
  }
  // main logic for XmlTable processing
  // must return Iterator[Row] for spark, can be changed to Array[T] for any other use
  private def processXmlTable(
                               xmlTable: XmlTable,
                               xmlTree: XdmNode,
                               options: XmlOptions): Iterator[Row] = {
    // val rootXQuery: compiled XQuery expression
    val rootXQuery = xmlTable.compileRootXQuery()
    // list of requested columns with compiled XPath queries
    // contextItem (rootXQuery result) should be specified just before execution
    val columns: Array[XmlCompiledColumn] = xmlTable.compileRequestedXPaths()
    // 1. execute XQuery to establish rows
    val xqueryResult = XQueryHelper
      .prepare(rootXQuery, xmlTree)
      .iterator()
    // return Iterator of Row
    processRow(xqueryResult, columns, options)
  }
  private def processRow(xqueryResult: XdmSequenceIterator[XdmItem],
                         columns: Array[XmlCompiledColumn],
                         options: XmlOptions): Iterator[Row] = {
    val resultIterator = xqueryResult.map(contextRow => {
      val rowArray: Array[Any] = columns.map(
        column => {
          // process every column, exec xpath with context item
          val xpathResult = XPathHelper.prepare(column.xpath, contextRow).evaluate()
          // return null if empty, return string value if not empty
          if (xpathResult.isEmpty){
            null
          } else if (xpathResult.size() <= 1) {
            val result = xpathResult.getUnderlyingValue.getStringValue
            castTo(result, column.dataType, options)
          } else {
            throw new RuntimeException(
              s"XPath for column: ${column.name} returned more than 1 row per context row")
          }
        }
      )
      Row.fromSeq(rowArray)
    })
    resultIterator
  }
}



