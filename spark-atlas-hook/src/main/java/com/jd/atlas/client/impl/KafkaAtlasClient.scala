package com.jd.atlas.client.impl

import java.util.{Collections, List}

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.client.ext.UserDefineAtlasHook
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.notification.HookNotification
import org.apache.spark.internal.Logging

class KafkaAtlasClient  extends UserDefineAtlasHook with AtlasClient with Logging{

  override def createEntities(extInfo: AtlasEntity.AtlasEntitiesWithExtInfo): Unit = {
    val notification = new HookNotification.EntityCreateRequestV2("sparkhook", extInfo)
    logInfo(String.format("%såéæ¶æ¯%s","sparkhook",extInfo))
    super.notifyEntities(Collections.singletonList(notification),null)
  }
}
