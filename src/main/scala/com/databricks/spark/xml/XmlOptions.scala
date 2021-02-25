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

import java.nio.charset.StandardCharsets

import com.databricks.spark.xml.util._

/*
 * Options for the XML data source.
 */
private[xml] class XmlOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable {

  def this() = this(Map.empty)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val ignoreNamespace = parameters.get("ignoreNamespace").map(_.toBoolean).getOrElse(false)
  val timestampFormat = parameters.get("timestampFormat")
  val dateFormat = parameters.get("dateFormat")

}

private[xml] object XmlOptions {
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null

  def apply(parameters: Map[String, String]): XmlOptions = new XmlOptions(parameters)
}
