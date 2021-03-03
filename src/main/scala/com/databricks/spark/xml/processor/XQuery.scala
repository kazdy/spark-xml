package com.databricks.spark.xml.processor

import com.databricks.spark.xml.processor.XmlProcessor.xqueryCompiler
import com.databricks.spark.xml.table.XmlNamespace
import net.sf.saxon.s9api.{XQueryCompiler, XQueryEvaluator, XQueryExecutable, XdmItem, XdmValue}

class XQuery {
  private val _compiler: XQueryCompiler = xqueryCompiler

//  private var _executable: XQueryExecutable = null
//  private var _evaluator: XQueryEvaluator = null

  def compile(query: String ): XQueryExecutable = {
    _compiler.compile(query)
  }

  def compile(query: String, namespaces: List[XmlNamespace]): XQueryExecutable = {
    namespaces.foreach(ns => _compiler.declareNamespace(ns.prefix, ns.uri))
    _compiler.compile(query)
  }

  def prepare(queryExecutable: XQueryExecutable, contextItem: XdmItem): XQueryEvaluator = {
      val queryEvaluator = queryExecutable.load()
      queryEvaluator.setContextItem(contextItem)
      queryEvaluator
  }

  def execute(queryEvaluator: XQueryEvaluator): XdmValue = {
    queryEvaluator.evaluate()
  }



}
