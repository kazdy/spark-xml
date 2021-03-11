package com.darkrows.spark.xml.processor

import com.darkrows.spark.xml.table.XmlNamespace
import XmlProcessor.xqueryCompiler
import net.sf.saxon.s9api.{XQueryCompiler, XQueryEvaluator, XQueryExecutable, XdmItem, XdmValue}

private[xml] object XQueryHelper {
  private val _compiler: XQueryCompiler = xqueryCompiler

//  private var _executable: XQueryExecutable = null
//  private var _evaluator: XQueryEvaluator = null

  def compile(query: String ): XQueryExecutable = {
    _compiler.compile(query)
  }

  def compile(query: String, namespaces: Array[XmlNamespace]): XQueryExecutable = {
    // if namespaces is empty, no namespace will be declared
    namespaces.foreach(ns => _compiler.declareNamespace(ns.prefix, ns.uri))
    _compiler.compile(query)
  }

  def prepare(queryExecutable: XQueryExecutable, contextItem: XdmItem): XQueryEvaluator = {
      val queryEvaluator = queryExecutable.load()
      queryEvaluator.setContextItem(contextItem)
      queryEvaluator
  }

  // TODO: method execute not needed ?
  def execute(queryEvaluator: XQueryEvaluator): XdmValue = {
    queryEvaluator.evaluate()
  }



}
