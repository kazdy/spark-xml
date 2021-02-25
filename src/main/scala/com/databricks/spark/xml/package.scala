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
package com.databricks.spark

import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, StructType}

import com.databricks.spark.xml.parsers.StaxXmlParser
import com.databricks.spark.xml.util.XmlFile

package object xml {
  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    @deprecated("Use read.format(\"xml\") or read.xml", "0.4.0")
    def xmlFile(
        filePath: String,
        rowTag: String = XmlOptions.DEFAULT_ROW_TAG,
        samplingRatio: Double = 1.0,
        excludeAttribute: Boolean = false,
        treatEmptyValuesAsNulls: Boolean = false,
        failFast: Boolean = false,
        attributePrefix: String = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX,
        valueTag: String = XmlOptions.DEFAULT_VALUE_TAG,
        charset: String = XmlOptions.DEFAULT_CHARSET): DataFrame = {

      val parameters = Map(
        "rowTag" -> rowTag,
        "samplingRatio" -> samplingRatio.toString,
        "excludeAttribute" -> excludeAttribute.toString,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls.toString,
        "failFast" -> failFast.toString,
        "attributePrefix" -> attributePrefix,
        "valueTag" -> valueTag,
        "charset" -> charset)
      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rowTag),
        location = Some(filePath),
        parameters = parameters)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }



  /**
   * Adds a method, `xml`, to DataFrameReader that allows you to read avro files using
   * the DataFileReader
   */
  implicit class XmlDataFrameReader(reader: DataFrameReader) {
    def xml: String => DataFrame = reader.format("com.databricks.spark.xml").load

    def xml(xmlDataset: Dataset[String]): DataFrame = {
      val spark = SparkSession.builder.getOrCreate()
      new XmlReader().xmlDataset(spark, xmlDataset)
    }
  }


  /**
   * @param xml XML document to parse, as string
   * @param schema the schema to use when parsing the XML string
   * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
   * @return [[Row]] representing the parsed XML structure
   */
  @Experimental
  def from_xml_string(xml: String, schema: StructType,
                      options: Map[String, String] = Map.empty): Row = {
    StaxXmlParser.parseColumn(xml, schema, XmlOptions(options))
  }

}
