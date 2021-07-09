package com.jd.atlas.listener

import java.io.FileInputStream
import java.util.Properties

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.interceptor.SQLQueryContext
import com.jd.atlas.parser.impl.{CreateDataSourceTableAsSelectHarvester, CreateHiveTableAsSelectCommandHarvester, InsertIntoHiveTableHarvester}
import com.jd.atlas.process.{DataWritingCommandDetail, QueryeExecutionDetail}
import com.jd.atlas.process.impl.{CatalogEventProcessor, SparkExecutionPlanProcessor}
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlan, UnionExec}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.util.QueryExecutionListener

class SparkAtlasEventTracker extends SparkListener with QueryExecutionListener with Logging{

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    //获取Spark执行计划
    val plan:SparkPlan = qe.sparkPlan
    val qed = QueryeExecutionDetail(qe, Option(SQLQueryContext.get()))
    //sparkExescutionPlanProcessor.pushEvent(QueryeExecutionDetail(qe,Option(SQLQueryContext.get())))
    val client = AtlasClient.atlasClient()
    //获取所有的节点计划类型
    var planNodes: Seq[SparkPlan] = qed.queryExecution.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    //处理所有的 RunnableCommand
    val runnableComands: Seq[RunnableCommand] = planNodes.collect({ case ExecutedCommandExec(cmd) => cmd })
    if(!runnableComands.isEmpty){
      logInfo("占时不支持解析RunnableCommand计划！")
    }
    //处理所有的写数据处理
    val dataWritingCommands:Seq[DataWritingCommand] = planNodes.collect({ case DataWritingCommandExec(cmd, child) => cmd })
    if(!dataWritingCommands.isEmpty){
      //dataWritingCommands.foreach(cmd=>dataWritingCommandProcessor.pushEvent(DataWritingCommandDetail(cmd,queryDetail.sql)))
      for (cmd <- dataWritingCommands) {
        val commandDetail = DataWritingCommandDetail(cmd, qed.sql)
        SQLQueryContext.set(commandDetail.sql.get)
        commandDetail.cm match {
          //处理create table xxx as query
          case CreateHiveTableAsSelectCommand(tableDesc,query,outputColumnNames,mode)=>{
            val withExtInfo = new CreateHiveTableAsSelectCommandHarvester().harvest(commandDetail.cm.asInstanceOf[CreateHiveTableAsSelectCommand])
            client.createEntities(withExtInfo)
          }
          //处理 Insert Into hive table
          case InsertIntoHiveTable(table,partition,query,overwrite,ifPartitionNotExists,outputColumnNames)=>{
            val withExtInfo = new InsertIntoHiveTableHarvester().harvest(commandDetail.cm.asInstanceOf[InsertIntoHiveTable])
            client.createEntities(withExtInfo)
          }

          case InsertIntoHadoopFsRelationCommand(outputPath,staticPartitions,ifPartitionNotExists,partitionColumns,bucketSpec,fileFormat,options,query,mode,catalogTable,fileIndex,outputColumnNames)=>{
            logInfo("InsertIntoHadoopFsRelationCommand:"+ commandDetail.cm)
          }
          //saveAsTable追踪 saveAsTable
          case CreateDataSourceTableAsSelectCommand(table,mode,query,outputColumnNames)=>{
            val withExtInfo = new CreateDataSourceTableAsSelectHarvester().harvest(commandDetail.cm.asInstanceOf[CreateDataSourceTableAsSelectCommand])
            client.createEntities(withExtInfo)
          }
          case _ => logWarning("目前还未支持处理DataWritingCommand:"+ commandDetail.cm)
        }
      }
    }
    var writeToDataSourceV2Execs: Seq[WriteToDataSourceV2Exec] = planNodes.collect({ case wds: WriteToDataSourceV2Exec => wds })
    if(!writeToDataSourceV2Execs.isEmpty){
      logInfo("占时不支持解析WriteToDataSourceV2Exec计划！")
    }

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
     //TODO Nothing
  }
}
