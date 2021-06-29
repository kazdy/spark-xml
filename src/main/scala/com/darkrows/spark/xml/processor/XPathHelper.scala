package com.darkrows.spark.xml.processor

import com.darkrows.spark.xml.table.XmlNamespace
import XmlProcessor.xpathCompiler
import net.sf.saxon.s9api.{XPathCompiler, XPathExecutable, XPathSelector, XdmItem, XdmValue}

private[xml] object XPathHelper extends Serializable {

  private val _compiler: XPathCompiler = xpathCompiler

  //  private var _executable: XpathExecutable = null
  //  private var _evaluator: XpathSelector = null

  def compile(query: String ): XPathExecutable = {
    _compiler.compile(query)
  }

  def compile(query: String, namespaces: Array[XmlNamespace]): XPathExecutable = {
    // if namespaces is empty, no namespace will be declared
    namespaces.foreach(ns => _compiler.declareNamespace(ns.prefix, ns.uri))
    _compiler.compile(query)
  }

  def prepare(queryExecutable: XPathExecutable, contextItem: XdmItem): XPathSelector = {
    val queryEvaluator = queryExecutable.load()
    queryEvaluator.setContextItem(contextItem)
    queryEvaluator
  }

  def execute(queryEvaluator: XPathSelector): XdmValue = {
    queryEvaluator.evaluate()
  }

}
