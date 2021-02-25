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
package com.databricks.spark.xml.util

import java.io.CharArrayWriter
import java.nio.charset.Charset
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map
import com.sun.xml.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.io.{Text, LongWritable}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.databricks.spark.xml.{XmlOptions, XmlInputFormat}

private[xml] object XmlFile {
  val DEFAULT_INDENT = "    "

  // Returns RDD which contains xml files as strings
  def withCharset(
      context: SparkContext,
      location: String,
      charset: String,
      rowTag: String): RDD[String] = {
    // This just checks the charset's validity early, to keep behavior
    Charset.forName(charset)
    context.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag>")
    context.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
    context.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, charset)
    context.newAPIHadoopFile(location,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text]).map { case (_, text) => text.toString }
  }

}
