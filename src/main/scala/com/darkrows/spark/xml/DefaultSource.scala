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

import com.darkrows.spark.xml.util.XmlFile
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Provides access to XML data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister {

  /**
   * Short alias for spark-xml data source.
   */
  override def shortName(): String = "xml"

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path",
      throw new IllegalArgumentException("'path' must be specified for XML data."))
  }

  /**
   * Creates a new relation for data store in XML given parameters.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in XML given parameters and user supported schema.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): XmlRelation = {

    val path = checkPath(parameters)
    // We need the `charset` and `rowTag` before creating the relation.
    val (charset, startTag, endTag) = {
      val options = XmlOptions(parameters, schema)
      (options.charset, options.startTag, options.endTag)
    }

    XmlRelation(
      () => XmlFile.withCharset(sqlContext.sparkContext, path, charset, startTag, endTag),
      Some(path),
      parameters,
      schema)(sqlContext)
  }

}
