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

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.nio.file.{Files, Path, Paths}
import java.sql.{Date, Timestamp}
import java.util.TimeZone
import scala.io.Source
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.darkrows.spark.xml.TestUtils._
import com.darkrows.spark.xml.XmlOptions._
import com.darkrows.spark.xml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.SparkException
import org.xml.sax.SAXParseException

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

  // Tests

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

    results.show()
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
    spark.sql("SELECT year, comment comment FROM carsTable1").show()
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

  test("Skip and project currently XML files without indentation") {
    val df = spark.read
      .schema(buildSchema(field("model", StringType)))
      .option("rootXQuery", "./ROW")
      .option("column.xpath.model", "./model")
      .xml(resDir + "cars-no-indentation.xml")
    val results = df.select("model").collect()
    val years = results.map(_(0)).toSet

    assert(years === Set("S", "E350", "Volt"))
  }

// allways treat empty string returned from xpath as null
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
    assert(fruit.head().getAs[String]("color") === null)
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
/*
  test("test mixed text and element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(resDir + "mixed_children.xml")
    val mixedRow = mixedDF.head()
    assert(mixedRow.getAs[Row](0).toSeq === Seq(" lorem "))
    assert(mixedRow.getString(1) === " ipsum ")
  }

  test("test mixed text and complex element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(resDir + "mixed_children_2.xml")
    assert(mixedDF.select("foo.bar").head().getString(0) === " lorem ")
    assert(mixedDF.select("foo.baz.bing").head().getLong(0) === 2)
    assert(mixedDF.select("missing").head().getString(0) === " ipsum ")
  }

  test("test XSD validation") {
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", resDir + "basket.xsd")
      .xml(resDir + "basket.xml")
    // Mostly checking it doesn't fail
    assert(basketDF.selectExpr("entry[0].key").head().getLong(0) === 9027)
  }

  test("test XSD validation with validation error") {
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", resDir + "basket.xsd")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(resDir + "basket_invalid.xml")
    assert(basketDF.select("_malformed_records").head().getString(0).startsWith("<basket>"))
  }

  test("test XSD validation with addFile() with validation error") {
    spark.sparkContext.addFile(resDir + "basket.xsd")
    val basketDF = spark.read
      .option("rowTag", "basket")
      .option("inferSchema", true)
      .option("rowValidationXSDPath", "basket.xsd")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(resDir + "basket_invalid.xml")
    assert(basketDF.select("_malformed_records").head().getString(0).startsWith("<basket>"))
  }

  test("test xmlRdd") {
    val data = Seq(
      "<ROW><year>2012</year><make>Tesla</make><model>S</model><comment>No comment</comment></ROW>",
      "<ROW><year>1997</year><make>Ford</make><model>E350</model><comment>Get one</comment></ROW>",
      "<ROW><year>2015</year><make>Chevy</make><model>Volt</model><comment>No</comment></ROW>")
    val rdd = spark.sparkContext.parallelize(data)
    assert(new XmlReader().xmlRdd(spark, rdd).collect().length === 3)
  }

  test("from_xml basic test") {
    val xmlData =
      """<parent foo="bar"><pid>14ft3</pid>
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val xmlSchema = schema_of_xml_df(df.select("payload"))
    val expectedSchema = df.schema.add("decoded", xmlSchema)
    val result = df.withColumn("decoded", from_xml(df.col("payload"), xmlSchema))

    assert(expectedSchema === result.schema)
    assert(result.select("decoded.pid").head().getString(0) === "14ft3")
    assert(result.select("decoded._foo").head().getString(0) === "bar")
  }

  test("from_xml array basic test") {
    val xmlData = Array(
      "<parent><pid>14ft3</pid><name>dave guy</name></parent>",
      "<parent><pid>12345</pid><name>other guy</name></parent>")
    import spark.implicits._
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val xmlSchema = schema_of_xml_array(df.select("payload").as[Array[String]])
    val expectedSchema = df.schema.add("decoded", xmlSchema)
    val result = df.withColumn("decoded", from_xml(df.col("payload"), xmlSchema))

    assert(expectedSchema === result.schema)
    assert(result.selectExpr("decoded[0].pid").head().getString(0) === "14ft3")
    assert(result.selectExpr("decoded[1].pid").head().getString(0) === "12345")
  }

  test("from_xml error test") {
    // XML contains error
    val xmlData =
      """<parent foo="bar"><pid>14ft3
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val xmlSchema = schema_of_xml_df(df.select("payload"))
    val result = df.withColumn("decoded", from_xml(df.col("payload"), xmlSchema))
    assert(result.select("decoded._corrupt_record").head().getString(0).nonEmpty)
  }

  test("from_xml_string basic test") {
    val xmlData =
      """<parent foo="bar"><pid>14ft3</pid>
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val xmlSchema = schema_of_xml_df(df.select("payload"))
    val result = from_xml_string(xmlData, xmlSchema)

    assert(result.getString(0) === "bar")
    assert(result.getString(1) === "dave guy")
    assert(result.getString(2) === "14ft3")
  }

  test("from_xml with PERMISSIVE parse mode with no corrupt col schema") {
    // XML contains error
    val xmlData =
      """<parent foo="bar"><pid>14ft3
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val xmlDataNoError =
      """<parent foo="bar">
        |  <name>dave guy</name>
        |</parent>
       """.stripMargin
    val dfNoError = spark.createDataFrame(Seq((8, xmlDataNoError))).toDF("number", "payload")
    val xmlSchema = schema_of_xml_df(dfNoError.select("payload"))
    val df = spark.createDataFrame(Seq((8, xmlData))).toDF("number", "payload")
    val result = df.withColumn("decoded", from_xml(df.col("payload"), xmlSchema))
    assert(result.select("decoded").head().get(0) === null)
  }

  test("double field encounters whitespace-only value") {
    val schema = buildSchema(struct("Book", field("Price", DoubleType)), field("_corrupt_record"))
    val whitespaceDF = spark.read
      .option("rowTag", "Books")
      .schema(schema)
      .xml(resDir + "whitespace_error.xml")

    assert(whitespaceDF.count() === 1)
    assert(whitespaceDF.take(1).head.getAs[String]("_corrupt_record") !== null)
  }

  test("XML in String field preserves attributes") {
    val schema = buildSchema(field("ROW"))
    val result = spark.read
      .option("rowTag", "ROWSET")
      .schema(schema)
      .xml(resDir + "cars-attribute.xml")
      .collect()
    assert(result.head.get(0) ===
      "<year>2015</year><make>Chevy</make><model>Volt</model><comment foo=\"bar\">No</comment>")
  }

  test("rootTag with simple attributes") {
    val xmlPath = getEmptyTempDir().resolve("simple_attributes")
    val df = spark.createDataFrame(Seq((42, "foo"))).toDF("number", "value").repartition(1)
    df.write.option("rootTag", "root foo='bar' bing=\"baz\"").xml(xmlPath.toString)

    val xmlFile =
      Files.list(xmlPath).iterator.asScala.filter(_.getFileName.toString.startsWith("part-")).next
    val firstLine = getLines(xmlFile).head
    assert(firstLine === "<root foo=\"bar\" bing=\"baz\">")
  }

  test("test ignoreNamespace") {
    val results = spark.read
      .option("rowTag", "book")
      .option("ignoreNamespace", true)
      .xml(resDir + "books-namespaces.xml")
    assert(results.filter("author IS NOT NULL").count() === 3)
    assert(results.filter("_id IS NOT NULL").count() === 3)
  }

  test("MapType field with attributes") {
    val schema = buildSchema(
      field("_startTime"),
      field("_interval"),
      field("PMTarget", MapType(StringType, StringType)))
    val df = spark.read.option("rowTag", "PMSetup").
      schema(schema).
      xml(resDir + "map-attribute.xml").
      select("PMTarget")
    val map = df.collect().head.getAs[Map[String, String]](0)
    assert(map.contains("_measurementType"))
    assert(map.contains("M1"))
    assert(map.contains("M2"))
  }

  test("StructType with missing optional StructType child") {
    val df = spark.read.option("rowTag", "Foo").xml(resDir + "struct_with_optional_child.xml")
    assert(df.selectExpr("SIZE(Bar)").collect().head.getInt(0) === 2)
  }

  test("Manual schema with corrupt record field works on permissive mode failure") {
    // See issue #517
    val schema = StructType(List(
      StructField("_id", StringType),
      StructField("_space", StringType),
      StructField("c2", DoubleType),
      StructField("c3", StringType),
      StructField("c4", StringType),
      StructField("c5", StringType),
      StructField("c6", StringType),
      StructField("c7", StringType),
      StructField("c8", StringType),
      StructField("c9", DoubleType),
      StructField("c11", DoubleType),
      StructField("c20", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c46", StringType),
      StructField("c76", StringType),
      StructField("c78", StringType),
      StructField("c85", DoubleType),
      StructField("c93", StringType),
      StructField("c95", StringType),
      StructField("c99", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c100", ArrayType(StructType(List(
        StructField("_VALUE", StringType),
        StructField("_m", IntegerType)))
      )),
      StructField("c108", StringType),
      StructField("c192", DoubleType),
      StructField("c193", StringType),
      StructField("c194", StringType),
      StructField("c195", StringType),
      StructField("c196", StringType),
      StructField("c197", DoubleType),
      StructField("_corrupt_record", StringType)))

    val df = spark.read
      .option("inferSchema", false)
      .option("rowTag", "row")
      .schema(schema)
      .xml(resDir + "manual_schema_corrupt_record.xml")

    // Assert it works at all
    assert(df.collect().head.getAs[String]("_corrupt_record") !== null)
  }

  test("Test date parsing") {
    val schema = buildSchema(field("author"), field("date", DateType), field("date2", StringType))
    val df = spark.read
      .option("rowTag", "book")
      .schema(schema)
      .xml(resDir + "date.xml")
    assert(df.collect().head.getAs[Date](1).toString === "2021-02-01")
  }

  test("Test date type inference") {
    val df = spark.read
      .option("rowTag", "book")
      .xml(resDir + "date.xml")
    val expectedSchema =
      buildSchema(field("author"), field("date", DateType), field("date2", StringType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Date](1).toString === "2021-02-01")
  }

  test("Test timestamp parsing") {
    val schema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", StringType))
    val df = spark.read
      .option("rowTag", "book")
      .schema(schema)
      .xml(resDir + "time.xml")
    assert(df.collect().head.getAs[Timestamp](1).getTime === 1322907330000L)
  }

  test("Test timestamp type inference") {
    val df = spark.read
      .option("rowTag", "book")
      .xml(resDir + "time.xml")
    val expectedSchema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", StringType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Timestamp](1).getTime === 1322907330000L)
  }

  test("Test dateFormat") {
    val df = spark.read
      .option("rowTag", "book")
      .option("dateFormat", "MM-dd-yyyy")
      .xml(resDir + "date.xml")
    val expectedSchema =
      buildSchema(field("author"), field("date", DateType), field("date2", DateType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Date](2).toString === "2021-02-01")
  }

  test("Test timestampFormat") {
    val df = spark.read
      .option("rowTag", "book")
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss z")
      .xml(resDir + "time.xml")
    val expectedSchema =
      buildSchema(field("author"), field("time", TimestampType), field("time2", TimestampType))
    assert(df.schema === expectedSchema)
    assert(df.collect().head.getAs[Timestamp](2).getTime === 1322936130000L)
  }

  private def getLines(path: Path): Seq[String] = {
    val source = Source.fromFile(path.toFile)
    try {
      source.getLines.toList
    } finally {
      source.close()
    }
  }
*/
}