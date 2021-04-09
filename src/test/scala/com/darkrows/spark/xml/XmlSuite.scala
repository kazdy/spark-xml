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

import com.darkrows.spark.xml.TestUtils._
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.nio.file.{Files, Path}
import java.sql.{Date, Timestamp}
import scala.io.Source

final class XmlSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val resDir = "src/test/resources/"

  private lazy val spark: SparkSession = {
    // It is intentionally a val to allow import implicits.
    SparkSession.builder().
      master("local[2]").
      appName("XmlSuite").
      config("spark.ui.enabled", false).
      config("spark.sql.session.timeZone", "UTC").
      getOrCreate()
  }
  private var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark  // Initialize Spark session
    tempDir = Files.createTempDirectory("XmlSuite")
    tempDir.toFile.deleteOnExit()

  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  private def getEmptyTempDir(): Path = {
    Files.createTempDirectory(tempDir, "test")
  }

  // Tests that must be executed first, otherwise suite fails

  test("Skip and project currently XML files without indentation") {
    val df = spark.read
      .schema(buildSchema(field("model", StringType)))
      .option("rootXQuery", "./ROW")
      .option("column.xpath.model", "./model")
      .xml(resDir + "cars-no-indentation.xml")

    val years = df.select("model").collectAsList()

    assert(years.get(0).getAs[Row](0) === "S")
    assert(years.get(1).getAs[Row](0) === "E350")
    assert(years.get(2).getAs[Row](0) === "Volt")

    // assert(years === Set("S", "E350", "Volt"))
  }

  // always treat empty string returned from xpath as null
  test("Produces correct result for a row with a self closing tag inside") {
    val schema = buildSchema(
      field("non-empty-tag", IntegerType),
      field("self-closing-tag", IntegerType))

    val result = new XmlReader(schema, Map(
      "rootXQuery" -> "./ROW",
      "column.xpath.non-empty-tag" -> "./non-empty-tag",
      "column.xpath.self-closing-tag" -> "./self-closing-tag"
    ))
      .xmlFile(spark, resDir + "self-closing-tag.xml")
      .collect()

    assert(result(0) === Row(1, null))
  }

  test("empty string to null and back") {
    val fruit = spark.read
      .schema(buildSchema(field("color")))
      .option("startTag", "<root>")
      .option("endTag", "</root>")
      .option("rootXQuery", "./root/row")
      .option("column.xpath.color", "./color")
      .option("nullValue", "")
      .xml(resDir + "null-empty-string.xml")

    val firstRow = fruit.head()

    assert(firstRow.getAs[String]("color") === null)
  }

  // processing instruction should be omitted by the xml processor
  test("test XML with processing instruction") {
    val processingDF = spark.read
      .schema(buildSchema(field("bar")))
      .option("rootXQuery", "./foo")
      .option("column.xpath.bar", "./bar")
      .option("startTag", "<foo>")
      .option("endTag", "</foo>")
      .xml(resDir + "processing.xml")

    // "lorem ipsum" is expected
    assert(processingDF.count() === 1)
  }

  test("test mixed text and element children") {
    val mixedDF = spark.read
      .schema(buildSchema(field("bar"), field("missing")))
      .option("rootXQuery", "./root")
      .option("column.xpath.bar", "./foo/bar")
      .option("column.xpath.missing", "./missing")
      .option("startTag", "<root>")
      .option("endTag", "</root>")
      .xml(resDir + "mixed_children.xml")
    val mixedRow = mixedDF.head()

    assert(mixedRow.getAs[Row](0) === " lorem ")
    assert(mixedRow.getAs[Row](1) === " ipsum ")
  }


  test("test mixed text and complex element children") {
    val mixedDF = spark.read
      .schema(buildSchema(field("foo_bar"), field("foo_baz_bing", LongType), field("missing")))
      .option("rootXQuery", "./root")
      .option("column.xpath.foo_bar", "./foo/bar")
      .option("column.xpath.foo_baz_bing", "./foo/baz/bing")
      .option("column.xpath.missing", "./missing")
      .option("startTag", "<root>")
      .option("endTag", "</root>")
      .xml(resDir + "mixed_children_2.xml")
    assert(mixedDF.select("foo_bar").head().getString(0) === " lorem ")
    assert(mixedDF.select("foo_baz_bing").head().getLong(0) === 2)
    assert(mixedDF.select("missing").head().getString(0) === " ipsum ")
  }

  test("test xmlRdd") {
    val data = Seq(
      "<ROW><year>2012</year><make>Tesla</make><model>S</model><comment>No comment</comment></ROW>",
      "<ROW><year>1997</year><make>Ford</make><model>E350</model><comment>Get one</comment></ROW>",
      "<ROW><year>2015</year><make>Chevy</make><model>Volt</model><comment>No</comment></ROW>")
    val rdd = spark.sparkContext.parallelize(data)

    val results = new XmlReader(
      buildSchema(field("year")), Map(
        "rootXQuery" -> "./ROW",
        "column.xpath.year" -> "./year")
    ).xmlRdd(spark, rdd).collect()

    assert(results.length === 3)
  }

  test("Test date parsing") {
    val schema = buildSchema(field("author"), field("date", DateType), field("date2", StringType))
    val df = spark.read
      .schema(schema)
      .option("startTag", "<book>")
      .option("endTag", "</book>")
      .option("rootXQuery", "./book")
      .option("column.xpath.author", "./author")
      .option("column.xpath.date", "./date")
      .option("column.xpath.date2", "./date2")
      .xml(resDir + "date.xml")
    assert(df.collect().head.getAs[Date](1).toString === "2021-02-01")
  }

  test("Test timestamp parsing") {
    val schema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", StringType))
    val df = spark.read
      .schema(schema)
      .option("startTag", "<book>")
      .option("endTag", "</book>")
      .option("rootXQuery", "./book")
      .option("column.xpath.author", "./author")
      .option("column.xpath.time", "./time")
      .option("column.xpath.time2", "./time2")
      .xml(resDir + "time.xml")
    assert(df.collect().head.getAs[Timestamp](1).getTime === 1322907330000L)
  }

  test("Test dateFormat") {
    val schema = buildSchema(field("author"), field("date", DateType), field("date2", DateType))
    val df = spark.read
      .schema(schema)
      .option("startTag", "<book>")
      .option("endTag", "</book>")
      .option("rootXQuery", "./book")
      .option("column.xpath.author", "./author")
      .option("column.xpath.date", "./date")
      .option("column.xpath.date2", "./date2")
      .option("dateFormat", "MM-dd-yyyy")
      .xml(resDir + "date.xml")

    // input: 02-01-2021, expected output: 2021-02-01
    assert(df.collect().head.getAs[Date](2).toString === "2021-02-01")
  }


  test("Test timestampFormat") {
    val schema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", TimestampType))
    val df = spark.read
      .schema(schema)
      .option("startTag", "<book>")
      .option("endTag", "</book>")
      .option("rootXQuery", "./book")
      .option("column.xpath.author", "./author")
      .option("column.xpath.time", "./time")
      .option("column.xpath.time2", "./time2")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss z")
      .xml(resDir + "time.xml")

    assert(df.collect().head.getAs[Timestamp](2).getTime === 1322936130000L)
  }

  // Tests DSL

  test("DSL test") {
    val results = spark
      .read
      .schema(buildSchema(field("year", StringType)))
      .option("rootXQuery", "./ROW")
      .option("column.xpath.year", "./year")
      .format("xml")
      .load(resDir + "cars.xml")
      .select("year")
      .collect()

    assert(results.length === 3)
  }

  test("DSL test with xml having unbalanced datatypes") {
    val results = spark
      .read
      .schema(buildSchema(field("hr", StringType)))
      .option("rootXQuery", "./ROW/extensions/TrackPointExtension")
      .option("column.xpath.hr", "./hr")
      .xml(resDir + "gps-empty-field.xml")

    assert(results.collect().length === 2)
  }


  test("DSL test for inconsistent element attributes as fields") {
    val results = spark
      .read
      .schema(buildSchema(field("price_unit", StringType)))
      .option("rootXQuery", "./catalog/book")
      .option("column.xpath.price_unit", "./price/@unit")
      .option("startTag", "<catalog>")
      .option("endTag", "</catalog>")
      .xml(resDir + "books-attributes-in-no-child.xml")
      .select("price_unit")

    // This should not throw an exception `java.lang.ArrayIndexOutOfBoundsException`
    // as non-existing values are represented as `null`s.
    assert(results.collect()(0).getString(0) === null)
    assert(results.collect()(1).getString(0) == "$")
  }

  test("DSL test for iso-8859-1 encoded file") {

    val schema = buildSchema(field("year", IntegerType), field("comment", StringType))

    val dataFrame = new XmlReader(schema, Map("charset" -> StandardCharsets.ISO_8859_1.name,
                                      "rootXQuery" -> "./ROW",
                                      "column.xpath.year"-> "./year",
                                      "column.xpath.comment" -> "./comment"))
      .xmlFile(spark, resDir + "cars-iso-8859-1.xml")
    assert(dataFrame.select("year").collect().length === 3)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.head() === Row("No comment", 2012))
  }

  test("DSL test compressed file") {
    val results = spark.read
      .schema(buildSchema(field("year", StringType)))
      .option("rootXQuery", "./ROW")
      .option("column.xpath.year", "./year")
      .xml(resDir + "cars.xml.gz")
      .select("year")
      .collect()

    assert(results.length === 3)
  }


  test("DSL test splittable compressed file") {
    val results = spark.read
      .schema(buildSchema(field("year", StringType)))
      .option("rootXQuery", "./ROW")
      .option("column.xpath.year", "./year")
      .xml(resDir + "cars.xml.bz2")
      .select("year")
      .collect()

    assert(results.length === 3)
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      spark.read
        .schema(buildSchema(field("year", StringType)))
        .option("rootXQuery", "./ROW")
        .option("column.xpath.year", "./year")
        .option("charset", "1-9588-osi")
        .xml(resDir + "cars.xml")
        .select("year")
        .collect()
    }
    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("DDL test") {
    spark.sql(s"""
                 |CREATE TEMPORARY VIEW carsTable1
                 |(year string, comment string)
                 |USING com.darkrows.spark.xml
                 |OPTIONS (path "${resDir + "cars.xml"}",
                 |rootXQuery "./ROW",
                 |column.xpath.year "./year",
                 |column.xpath.comment "./comment")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year, comment FROM carsTable1").collect().length === 3)
  }


  test("DDL test with alias name") {
    spark.sql(s"""
                 |CREATE TEMPORARY VIEW carsTable2
                 |(year string)
                 |USING xml
                 |OPTIONS (path "${resDir + "cars.xml"}",
                 |rootXQuery "./ROW",
                 |column.xpath.year "./year")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable2").collect().length === 3)
  }
/*
  test("DSL test for parsing a malformed XML file") {
    val exception = intercept[SparkException] {
      val results = spark.read
        .schema(buildSchema(field("year", StringType)))
        .option("rootXQuery", "./ROW")
        .option("column.xpath.year", "./year")
        .xml(resDir + "cars-malformed.xml")
        .select("year")
        .collect()
    }

    assert(exception.getMessage.contains(
      "org.xml.sax.SAXParseException; lineNumber: 6; columnNumber: 7; The element type " +
        "\"model\" must be terminated by the matching end-tag \"</model>\""))
  }
*/


  test("DSL test with empty file and known schema") {

    val schema = buildSchema(field("year", IntegerType), field("comment", StringType))

    val result = new XmlReader(schema, Map(
      "rootXQuery" -> "./ROW",
      "column.xpath.year"-> "./year",
      "column.xpath.comment" -> "./comment"))
      .xmlFile(spark, resDir + "empty.xml")
      .select("year")
      .collect()

    assert(result.length === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val schema = buildSchema(
      field("color"),
      field("year"),
      field("make"),
      field("model"),
      field("comment"))
    val results = new XmlReader(schema, Map(
      "rootXQuery" -> "./ROW",
      "column.xpath.color" -> "./color",
      "column.xpath.year" -> "./year",
      "column.xpath.make" -> "./make",
      "column.xpath.model" -> "./model",
      "column.xpath.comment" -> "./comment"
    ))
      .xmlFile(spark, resDir + "cars-unbalanced-elements.xml")
      .count()

    assert(results === 3)
  }

  test("DDL test with empty file") {
    spark.sql(s"""
                 |CREATE TEMPORARY VIEW carsTable3
                 |(year double, make string, model string, comments string)
                 |USING com.darkrows.spark.xml
                 |OPTIONS (path "${resDir + "empty.xml"}"
                 |,rootXQuery "./ROW"
                 |,column.xpath.year "./year"
                 |,column.xpath.make "./make"
                 |,column.xpath.model "./model"
                 |,column.xpath.comments "./comments")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT count(*) FROM carsTable3").collect().head(0) === 0)
  }


  test("DSL test nullable fields") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age"))
    val results = new XmlReader(schema, Map(
      "rootXQuery" -> "./ROW",
      "column.xpath.name"-> "./name",
      "column.xpath.age" -> "./age"))
      .xmlFile(spark, resDir + "null-numbers.xml")
      .collect()

    assert(results(0) === Row("alice", "35"))
    assert(results(1) === Row("bob", "    "))
    assert(results(2) === Row("coc", "24"))
  }

  test("DSL test for using nullValue to express empty string as null value") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age", IntegerType))
    val results = new XmlReader(schema, Map(
      "nullValue" -> "    ",
      "rootXQuery" -> "./ROW",
      "column.xpath.name"-> "./name",
      "column.xpath.age" -> "./age"
    ))
      .xmlFile(spark, resDir + "null-numbers.xml")
      .collect()

    assert(results(1) === Row("bob", null))
  }

  test("DSL test with default namespace") {
    val results = spark.read
      .schema(buildSchema(field("catid", StringType)))
      .option("startTag", "<RDF ")
      .option("endTag", "</RDF> ")
      .option("rootXQuery", "./RDF/Topic")
      .option("column.xpath.catid", "./catid")
      .option("namespace", "http://dmoz.org/rdf/")
      .xml(resDir + "topics-namespaces.xml")
      .collect()

    assert(results.length === 1)
  }

  private def getLines(path: Path): Seq[String] = {
    val source = Source.fromFile(path.toFile)
    try {
      source.getLines.toList
    } finally {
      source.close()
    }
  }

}