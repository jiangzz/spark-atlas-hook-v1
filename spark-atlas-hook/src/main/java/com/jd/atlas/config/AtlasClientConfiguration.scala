package com.jd.atlas.config

import java.net.URL
import java.util.Properties

import com.jd.atlas.client.ext.UserDefineNotificationProvider
import org.apache.atlas.ApplicationProperties
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}


object AtlasClientConfiguration{
  val ATLAS_SPARK_ENABLED = ConfigEntry("atlas.spark.enabled", "true")
  val ATLAS_REST_ENDPOINT = ConfigEntry("atlas.rest.address", "CentOS:21000")
  val CLIENT_USERNAME = ConfigEntry("atlas.client.username", "admin")
  val CLIENT_PASSWORD = ConfigEntry("atlas.client.password", "admin")
  val CLIENT_TYPE = ConfigEntry("atlas.client.type", "rest")
  val CLUSTER_NAME = ConfigEntry("atlas.metadata.namespace", "5k")

  private lazy val configuration = {
    val input = classOf[UserDefineNotificationProvider].getClassLoader.getResourceAsStream("atlas-application.properties")
    val props = new Properties()
    props.load(input)
    props
  }

  def getUrl(): Object = {
     Option(configuration.getProperty(AtlasClientConfiguration.ATLAS_REST_ENDPOINT.key)).getOrElse(AtlasClientConfiguration.ATLAS_REST_ENDPOINT.defaultValue)
  }
  def getUserName(): String = {
    Option(configuration.getProperty(AtlasClientConfiguration.CLIENT_USERNAME.key)).getOrElse(AtlasClientConfiguration.CLIENT_USERNAME.defaultValue)
  }
  def getPassword(): String = {
    Option(configuration.getProperty(AtlasClientConfiguration.CLIENT_PASSWORD.key)).getOrElse(AtlasClientConfiguration.CLIENT_PASSWORD.defaultValue)
  }
  def getMetaNamespace():String={
     Option(configuration.getProperty(AtlasClientConfiguration.CLUSTER_NAME.key)).getOrElse(AtlasClientConfiguration.CLUSTER_NAME.defaultValue)
  }
  def get(t: ConfigEntry): String = {
    Option(configuration.getProperty(t.key).asInstanceOf[String]).getOrElse(t.defaultValue)
  }
}
