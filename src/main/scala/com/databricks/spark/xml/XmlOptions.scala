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
package com.databricks.spark.xml

import com.databricks.spark.xml.XmlOptions.DEFAULT_NULL_VALUE
import com.databricks.spark.xml.table.{XmlColumn, XmlNamespace}

import java.nio.charset.StandardCharsets
import com.databricks.spark.xml.util._
import org.apache.spark.sql.types.StructType

/*
 * Options for the XML data source.
 */
private[xml] class XmlOptions(
    @transient private val parameters: Map[String, String],
    @transient private val userSchema: StructType
    )
  extends Serializable {

  def this() = this(Map.empty, null)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
    require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val ignoreNamespace = parameters.get("ignoreNamespace").map(_.toBoolean).getOrElse(false)
  val timestampFormat = parameters.get("timestampFormat")
  val dateFormat = parameters.get("dateFormat")
  val treatEmptyValuesAsNulls = parameters.getOrElse("treatEmptyValuesAsNulls", "true")
  val rootXQuery = parameters.get("rootXQuery")

//  validateSchemaAndOptions()
// TODO: fixme empty array
  val namespaces = Array.empty[XmlNamespace]  // getNamespaces()
  val xmlColumnPaths = getXmlColumnPaths()


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

  def validateSchemaAndOptions(): Unit = {
    val xmlColumnPathsSet = xmlColumnPaths.map(_.name).toSet
    val schemaOptionsDiff = userSchema.fieldNames.toSet diff xmlColumnPathsSet

    if (userSchema.size != xmlColumnPaths.size) {
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
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null

   def apply(parameters: Map[String, String],
             userSchema: StructType): XmlOptions = new XmlOptions(parameters, userSchema)
}
