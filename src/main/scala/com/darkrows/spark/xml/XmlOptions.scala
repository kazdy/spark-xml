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
package com.darkrows.spark.xml

import com.darkrows.spark.xml.table.{XmlColumn, XmlNamespace}

import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.StructType

/*
 * Options for the XML data source.
 */
private[xml] class XmlOptions(
    @transient private val parameters: Map[String, String],
    @transient private val userSchema: Option[StructType]
    )
  extends Serializable {

  def this() = this(Map.empty, None)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val startTag = parameters.getOrElse("startTag", XmlOptions.DEFAULT_START_TAG)
    require(startTag.nonEmpty, "'startTag' option should not be empty string.")
  val endTag = parameters.getOrElse("endTag", XmlOptions.DEFAULT_END_TAG)
    require(endTag.nonEmpty, "'endTag' option should not be empty string.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val timestampFormat = parameters.get("timestampFormat")
  val dateFormat = parameters.get("dateFormat")
  val rootXQuery = parameters.getOrElse("rootXQuery" ,
    throw new RuntimeException("No rootXQuery defined in options"))


  val namespaces = getNamespaces() // Array.empty[XmlNamespace]
  val xmlColumnPaths = getXmlColumnPaths()

  // schema to column.xpath.<column_name> options validation
  validateSchemaAndOptions()


  def getNamespaces(): Array[XmlNamespace] = {
    parameters
      .filterKeys(_.matches("namespace.*"))
      .map(kv => {
        XmlNamespace(
          kv._1 match {
          case "namespace" => "" // default namespace has no prefix
          case x if x.matches("namespace\\..*") => x.replaceAll("namespace.", "")
        },
          kv._2)
      })
      .toArray
  }

  def getXmlColumnPaths(): Array[XmlColumn] = {
    parameters
      .filterKeys(_.matches("column\\.xpath\\..*"))
      .map(kv => XmlColumn(kv._1.replaceAll("column.xpath.", ""), kv._2))
      .toArray
  }
  // This just checks the column options early, to keep behavior
  def validateSchemaAndOptions(): Unit = {
   val schema = userSchema
     .getOrElse(throw new IllegalArgumentException("No user schema was provided"))

    val xmlColumnPathsSet = xmlColumnPaths.map(_.name).toSet
    val schemaOptionsDiff = schema.fieldNames.toSet diff xmlColumnPathsSet

    if (schema.size != xmlColumnPaths.size) {
      throw new UnsupportedOperationException(
        "Number of columns in schema is not equal to number of columns in options")
    } else if (schemaOptionsDiff.size > 0){
      throw new UnsupportedOperationException(
        s"""Difference in schema and column names, check: ${schemaOptionsDiff.mkString(", ")}"""
      )
    }
  }

}

private[xml] object XmlOptions {
  val DEFAULT_START_TAG = "<ROW>"
  val DEFAULT_END_TAG = "</ROW>"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null

   def apply(parameters: Map[String, String],
             userSchema: StructType): XmlOptions = new XmlOptions(parameters, Option(userSchema))
}
