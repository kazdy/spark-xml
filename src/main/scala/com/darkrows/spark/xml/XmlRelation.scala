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

import com.darkrows.spark.xml.processor.XmlParser
import com.darkrows.spark.xml.table.XmlTable
import com.darkrows.spark.xml.util.XmlFile

import java.io.IOException
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan}
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}

case class XmlRelation protected[spark] (
    baseRDD: () => RDD[String],
    location: Option[String],
    parameters: Map[String, String],
    userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedScan {

  private val options = XmlOptions(parameters, userSchema)

  // TODO: add XMLTable object creation which contains all the data needed for XMLTable processing

  override val schema: StructType = {
    Option(userSchema).getOrElse {
      throw new RuntimeException("Schema must be provided, schema inference is not supported")
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredFields = requiredColumns.map(schema(_))
    val requestedSchema = StructType(requiredFields)

    val xmlTable = new XmlTable(
      options.rootXQuery,
      options.namespaces,
      options.xmlColumnPaths,
      requestedSchema )

    XmlParser.parse(
      baseRDD(),
      xmlTable,
      options)
  }
}
