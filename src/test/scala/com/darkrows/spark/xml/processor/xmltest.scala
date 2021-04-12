package com.darkrows.spark.xml.processor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object xmltest extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", true)
    .appName("xmltest")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ALL")
/*
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

    // books.show
    val carsSchema = StructType(
      StructField("year", StringType) ::
        StructField("comment", StringType) :: Nil
    )

    val cars = {
      spark.read
        .format("xml")
        .schema(carsSchema)
        .option("rootXQuery", "./ROW")
        .option("column.xpath.year", "./year")
        .option("column.xpath.comment", "./comment")
        .load("cars.xml")
    }

    cars.show
*/
  val schemaHouses = StructType(
    StructField("id", StringType) ::
      StructField("id1", StringType) ::
      StructField("id2", StringType) ::
      StructField("id3", StringType) ::
      StructField("id4", StringType) ::
      StructField("id5", StringType) :: Nil
  )

  val houses = {
    spark.read
      .format("xml")
      .schema(schemaHouses)
      .option("rootXQuery", "./Houses/House")
      .option("column.xpath.id", "./@HOUSEID")
      .option("column.xpath.id1", "./@HOUSEGUID")
      .option("column.xpath.id2", "./@AOGUID")
      .option("column.xpath.id3", "./@HOUSENUM")
      .option("column.xpath.id4", "./@TERRIFNSFL")
      .option("column.xpath.id5", "./@ENDDATE")
      .option("startTag", "<Houses>")
      .option("endTag", "</Houses>")
      .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/fias_house.xml")

  }


  val startTime = System.currentTimeMillis()
  println(houses.count())
  val endTime = System.currentTimeMillis()
  println(s"Execution time: ${endTime - startTime} milisectonds")
}
