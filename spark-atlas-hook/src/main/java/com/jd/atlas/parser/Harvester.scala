package com.jd.atlas.parser

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.spark.sql.execution.QueryExecution

trait Harvester[T] {
  def harvest(node: T): AtlasEntitiesWithExtInfo;
}
