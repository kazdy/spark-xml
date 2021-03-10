# XML Data Source for Apache Spark

- A library for parsing and querying XML data with [Apache Spark](https://spark.apache.org).
The structure and test tools are mostly copied from [XML Data Source for Apache Spark](https://github.com/databricks/spark-xml).

- This package supports to process XML files in a distributed way.

- It's most useful when working with nested XML files. 

- It is based on a concept of XMLTABLE from SQLXML specification. It uses Saxon (HE) s9api for XQuery, XPath and XML parsing.

## Alternatives

Two main packages are present in the space of xml processing for Apache Spark: 
- spark-xml from Databricks,
- Hive-XML-SerDe from Dmitry Vasilenko

If there two are availabe why bother with this one?
- Databricks spark-xml:
  - not meant for processing very nested XML files,
  - schema inference not always works for very complicated XML files,
  - creating a schema manually for a complicated XML file takes too much time
  
- Hive-XML-SerDe: 
  - no longer maintained,
  - works only with older versions of Spark,
  - must be used with hive-on-spakr,
  - supports XPath 1.0 (node level namespaces are not supported)

When to use this package then?
  - you want to precisely define where your data comes from with XPath and XQuery,
  - you want to process nested XML files,
  - your XML files can fit into memory,
  - your XML files need to be preprocessed before querying it (use XQuery for that),
  - you want to use features of XQuery 3.1 and/or XPath 2.0 (and higher) or work with newer xml standard.  


## Requirements

| spark-xml | Spark         |
| --------- | ------------- |
| 0.0.x+    | 2.3.x+, 3.x   |

## Features

This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).

When reading files the API accepts several options:

* `path`: Location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The first and last xml node, used by XmlInputFormat. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `books`. Default is `ROW`.
* `charset`: Defaults to 'UTF-8' but can be set to other valid charset names
* `timestampFormat`: Specifies an additional timestamp format that will be tried when parsing values as `TimestampType` 
columns. The format is specified as described in [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
Defaults to try several formats, including [ISO_INSTANT](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_INSTANT),
including variations with offset timezones or no timezone (defaults to UTC).  
* `dateFormat`: Specifies an additional timestamp format that will be tried when parsing values as `DateType` 
columns. The format is specified as described in [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
Defaults to [ISO_DATE](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_DATE). 
* `namespace` : Declares default namespace for both root XQuery and columns XPath.
* `namespace.<prefix>` : Declares non default namespace with a prefix assinged for both root XQuery and columns XPath.
* `column.xpath.<column name>` : Declares Xpath for a column specified in schema. i.e `.option("column.xpath.name", "./name")`. 
  Must be valid XPath expression
* `rootXQuery` : Declares a root query which creates rows, it also serves as a context for column XPath queries. Must be a valid XQuery expression. 


This library doesn't write XML files!

It supports the shortened name usage, use `xml` i.e `.format("xml")`


### Parsing XML

This library is primarily used to convert (nested) XML documents into a `DataFrame`. 
It's designed to work with nested XML files that can fit int memory.


### Examples

These examples use a XML file available for download [here](https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml):

```
$ wget https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml
```

```scala
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("books")
        .getOrCreate()

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
        .option("rowTag", "catalog")
        .load("/home/dan/IdeaProjects/spark-xml/src/test/resources/books.xml")

    }

    books.show
```
returns this table:
```
+-----+--------------------+-----+
|   id|               title|price|
+-----+--------------------+-----+
|bk101|XML Developer's G...|44.95|
|bk102|       Midnight Rain| 5.95|
|bk103|     Maeve Ascendant| 5.95|
|bk104|     Oberon's Legacy| 5.95|
|bk105|  The Sundered Grail| 5.95|
|bk106|         Lover Birds| 4.95|
|bk107|       Splish Splash| 4.95|
|bk108|     Creepy Crawlies| 4.95|
|bk109|        Paradox Lost| 6.95|
|bk110|Microsoft .NET: T...|36.95|
|bk111|MSXML3: A Compreh...|36.95|
|bk112|Visual Studio 7: ...|49.95|
+-----+--------------------+-----+
```

## Building From Source

This library is built with [SBT](https://www.scala-sbt.org/). To build a JAR file simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.11 and 2.12.

