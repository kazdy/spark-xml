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
package com.darkrows.spark

import com.darkrows.spark.xml.processor.XmlParser
import com.darkrows.spark.xml.util.XmlFile
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, StructType}

package object xml {

  implicit class XmlDataFrameReader(reader: DataFrameReader) {
    def xml: String => DataFrame = reader.format("com.databricks.spark.xml").load

    def xml(xmlDataset: Dataset[String]): DataFrame = {
      val spark = SparkSession.builder.getOrCreate()
      new XmlReader().xmlDataset(spark, xmlDataset)
    }
  }
}
