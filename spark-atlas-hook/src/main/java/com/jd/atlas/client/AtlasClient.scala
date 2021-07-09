package com.jd.atlas.client

import com.jd.atlas.client.impl.{KafkaAtlasClient, RestAtlasClient}
import com.jd.atlas.config.AtlasClientConfiguration
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.spark.internal.Logging

trait AtlasClient extends Logging {
  def createEntities(ext:AtlasEntitiesWithExtInfo):Unit
}
object AtlasClient extends  Logging {
  @volatile private var client: AtlasClient = null

  def atlasClient(): AtlasClient = {
    val clientType = AtlasClientConfiguration.get(AtlasClientConfiguration.CLIENT_TYPE).trim
    require(clientType!=null,"必须指定client类型，目前支持rest和消息队列")
    if (client == null) {
      AtlasClient.synchronized {
        if (client == null) {
          clientType match {
            case "rest" => client = new RestAtlasClient()
            case "kafka"=> client = new KafkaAtlasClient()
            case e => throw new IllegalArgumentException("占时不支持，其他版本客户端")
          }
          logInfo(s"创建${clientType}成功！")
        }
      }
    }
    client
  }
}