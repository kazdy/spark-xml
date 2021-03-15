package com.darkrows.spark.xml.processor

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
      StructField("id", StringType) ::
      StructField("title", StringType) ::
        StructField("price", DoubleType) :: Nil
  )

    val books = {
      spark.read
        .format("xml")
        .schema(schema)
        .option("rootXQuery", "./catalog/book")
        .option("column.xpath.id", "./@id")
        .option("column.xpath.title", "./title")
        .option("column.xpath.price", "./price")
        .option("startTag", "<catalog>")
        .option("endTag", "</catalog>")
        .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/books.xml")

    }

    books.show


  val schemaHouses = StructType(
    StructField("id", StringType) :: Nil
  )

  val houses = {
    spark.read
      .format("xml")
      .schema(schemaHouses)
      .option("rootXQuery", "./Houses/House")
      .option("column.xpath.id", "./@HOUSEID")
      .option("startTag", "<Houses>")
      .option("endTag", "</Houses>")
      .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/fias_house.xml")

  }

  println(houses.count())
}
