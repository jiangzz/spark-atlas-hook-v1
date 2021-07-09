package com.jd.atlas.interceptor

import org.apache.spark.sql.SparkSessionExtensions

class SparkExtension extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(SparkAtlasParserInterface)
  }
}