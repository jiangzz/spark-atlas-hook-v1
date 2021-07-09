package com.jd.atlas

import org.apache.commons.configuration.PropertiesConfiguration

object TestApplications {
  def main(args: Array[String]): Unit = {
    val url = TestApplications.getClass.getClassLoader.getResource("atlas-application.proeprties")
    val configuration = new PropertiesConfiguration(url)
    val str = configuration.getString("name")
    println(str)
  }
}
