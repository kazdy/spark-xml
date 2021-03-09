package com.databricks.spark.xml.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object xmltest extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("xmltest")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ALL")

  val schema = StructType(
      StructField("age", StringType) ::
      StructField("name", IntegerType) :: Nil
  )

  val paths = List("/home/dan/IdeaProjects/spark-xml/src/test/resources/ages.xml",
    "/home/dan/IdeaProjects/spark-xml/src/test/resources/multipleFiles/*.xml")

    val ids = {
      spark.read
        .format("xml")
        .schema(schema)
        .option("rootXQuery", "./Houses/House")
        .option("column.xpath.age", "./@COUNTER")
        .option("column.xpath.name", "./@POSTALCODE")
        .option("rowTag", "Houses")
        .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/fias_house.large.xml")

    }
  ids.select("age", "name").orderBy("age").show(10)
}
