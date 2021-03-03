package com.databricks.spark.xml.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object xmltest extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
    println(spark.version)

  val schema = StructType(
      StructField("id", StringType) ::
      StructField("name", StringType) :: Nil
  )


    val ids = {
      spark.read
        .format("xml")
        .schema(schema)
        .option("column.xpath.id", "some/example/xpath")
        .option("column.xpath.name", "some/example/xpath2")
        .option("rowTag", "people")
        .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/ages.xml")

    }
  ids.show
}
