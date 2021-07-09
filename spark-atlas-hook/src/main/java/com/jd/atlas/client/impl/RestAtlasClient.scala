package com.jd.atlas.client.impl

import java.util

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.config.AtlasClientConfiguration
import org.apache.atlas.AtlasClientV2
import org.apache.atlas.model.instance.AtlasEntity

import scala.collection.JavaConverters._

class RestAtlasClient extends AtlasClient{
  private val client = {
       val serverUrls= AtlasClientConfiguration.getUrl() match {
        case a: util.ArrayList[_] => a.toArray().map(b => b.toString)
        case s: String => Array(s)
        case _: Throwable => throw new IllegalArgumentException(s"获取 atlas.rest.address失败！")
       }
      val basicAuth = Array(AtlasClientConfiguration.getUserName(), AtlasClientConfiguration.getPassword())
      new AtlasClientV2(serverUrls, basicAuth)
  }
  override def createEntities(ext: AtlasEntity.AtlasEntitiesWithExtInfo): Unit = {

  }
}
