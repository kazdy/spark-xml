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
package com.databricks.spark.xml.parsers

import java.io.StringReader

import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{Attribute, Characters, EndElement, StartElement, XMLEvent}
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.Try

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.XmlOptions
import com.databricks.spark.xml.util._

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser extends Serializable {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  // TODO: change parse implementation to return XMLTABLE as RDD[Row]
  def parse(
      xml: RDD[String],
      schema: StructType,
      options: XmlOptions): RDD[Row] = {
    xml.mapPartitions { iter =>
      iter.flatMap { xml => Nil} // parse columns
    }
  }

}
