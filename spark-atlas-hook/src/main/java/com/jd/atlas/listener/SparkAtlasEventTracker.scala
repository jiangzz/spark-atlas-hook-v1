package com.jd.atlas.listener

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.config.AtlasClientConfiguration
import com.jd.atlas.interceptor.SQLQueryContext
import com.jd.atlas.process.QueryeExecutionDetail
import com.jd.atlas.process.impl.{CatalogEventProcessor, SparkExecutionPlanProcessor}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener

class SparkAtlasEventTracker extends SparkListener with QueryExecutionListener with Logging{

  val catalogEventProcessor:CatalogEventProcessor=new CatalogEventProcessor(AtlasClient.atlasClient())
  catalogEventProcessor.startThread()

  private val sparkExescutionPlanProcessor:SparkExecutionPlanProcessor = new SparkExecutionPlanProcessor(AtlasClient.atlasClient())
  sparkExescutionPlanProcessor.startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    val sparkSessionOption:Option[SparkSession] = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if(sparkSessionOption.isEmpty){
      throw new IllegalStateException("Cannot find active or default SparkSession in the current " + "context")
    }
    val sparkSession = sparkSessionOption.get
    if((sparkSession.sparkContext.getConf.get("spark.sql.catalogImplementation", "in-memory") == "hive")
      && sparkSession.sparkContext.getConf.contains("hive.metastore.uris")){
      logInfo("集成了Hive，先不处理任何操作！")
      return
    }

    event match {
      case event:ExternalCatalogEvent=> catalogEventProcessor.pushEvent(event);
      case _ => logWarning(s"暂时不支持事件处理！")
    }

  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    //获取Spark执行计划
    val plan:SparkPlan = qe.sparkPlan
    sparkExescutionPlanProcessor.pushEvent(QueryeExecutionDetail(qe,Option(SQLQueryContext.get())))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
     //TODO Nothing
  }
}
