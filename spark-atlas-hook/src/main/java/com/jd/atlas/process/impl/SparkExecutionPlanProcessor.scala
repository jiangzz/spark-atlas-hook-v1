package com.jd.atlas.process.impl

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.process.{AbstractProcessor, DataWritingCommandDetail, QueryeExecutionDetail}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec

class SparkExecutionPlanProcessor(atlasClient:AtlasClient) extends AbstractProcessor[QueryeExecutionDetail] with Logging{

  val dataWritingCommandProcessor=new DataWritingCommandProcessor(atlasClient)
  dataWritingCommandProcessor.startThread()

  override protected def process(queryDetail: QueryeExecutionDetail): Unit = {

    //获取所有的节点计划类型
    var planNodes: Seq[SparkPlan] = queryDetail.queryExecution.sparkPlan.collect {
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
       dataWritingCommands.foreach(cmd=>dataWritingCommandProcessor.pushEvent(DataWritingCommandDetail(cmd,queryDetail.sql)))
    }
    var writeToDataSourceV2Execs: Seq[WriteToDataSourceV2Exec] = planNodes.collect({ case wds: WriteToDataSourceV2Exec => wds })
    if(!writeToDataSourceV2Execs.isEmpty){
      logInfo("占时不支持解析WriteToDataSourceV2Exec计划！")
    }

  }
}
