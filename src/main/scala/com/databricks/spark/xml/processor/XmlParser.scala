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
import com.databricks.spark.xml.util._
import net.sf.saxon.s9api.{DocumentBuilder, Processor, XdmItem, XdmNode, XdmSequenceIterator, XdmValue}
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

  private val processor: Processor = new Processor(false)
  private val builder: DocumentBuilder = processor.newDocumentBuilder()

  def toInputStream(xml: String): InputStream = {
    new ByteArrayInputStream(xml.getBytes())
  }

  def toSAXSource(stream: InputStream): SAXSource = {
    new SAXSource(new InputSource(stream))
  }

  def buildSource(source: SAXSource): XdmNode = {
    builder.build(source)
  }

  def buildXdmTree(xml: String): XdmNode = {
    val inputStream = toInputStream(xml)
    val SAXSource = toSAXSource(inputStream)
    val xdmNode = buildSource(SAXSource)
    xdmNode
  }

  val XQCompiler = processor.newXQueryCompiler()


  // TODO: change parse implementation to return XMLTABLE as RDD[Row]
  def parse(
             xml: RDD[String],
             schema: StructType,
             options: XmlOptions): RDD[Row] = {
    xml.mapPartitions(iter => iter.flatMap(xml => doParseColumn(xml)) )
  }

    def doParseColumn(xml: String): Iterator[Row] = {

      val doc: XdmNode = buildXdmTree(xml)
      val XQExecutable = XQCompiler.compile("//age")
      val XQEvaluator = XQExecutable.load()
      XQEvaluator.setContextItem(doc)

      val result: XdmValue = XQEvaluator.evaluate()

      var id = 0
      val processedRows = for (row <- result) yield {
        id += 1
        Row.apply(id.toString , row.getStringValue)
      }

      processedRows.toIterator



      }

      // Array(Row.fromSeq(Seq("2", "nie daniel"))).toIterator

}



