package com.jd.atlas.client

import com.jd.atlas.client.impl.{KafkaAtlasClient}
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.spark.internal.Logging

trait AtlasClient extends Logging {
  def createEntities(ext:AtlasEntitiesWithExtInfo):Unit
}
object AtlasClient extends  Logging {
  @volatile private var client: AtlasClient = null

  def atlasClient(): AtlasClient = {
    new KafkaAtlasClient()
  }
}