package com.jd.atlas.process.impl

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.interceptor.SQLQueryContext
import com.jd.atlas.parser.impl.{CreateDataSourceTableAsSelectHarvester, CreateHiveTableAsSelectCommandHarvester, InsertIntoHiveTableHarvester}
import com.jd.atlas.process.{AbstractProcessor, DataWritingCommandDetail}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
class DataWritingCommandProcessor(atlasClient: AtlasClient) extends AbstractProcessor[DataWritingCommandDetail] with Logging{
  override protected def process(detail: DataWritingCommandDetail): Unit = {
    SQLQueryContext.set(detail.sql.get)
    detail.cm match {
         //处理create table xxx as query
       case CreateHiveTableAsSelectCommand(tableDesc,query,outputColumnNames,mode)=>{
         val withExtInfo = new CreateHiveTableAsSelectCommandHarvester().harvest(detail.cm.asInstanceOf[CreateHiveTableAsSelectCommand])
         atlasClient.createEntities(withExtInfo)
       }
        //处理 Insert Into hive table
       case InsertIntoHiveTable(table,partition,query,overwrite,ifPartitionNotExists,outputColumnNames)=>{
         val withExtInfo = new InsertIntoHiveTableHarvester().harvest(detail.cm.asInstanceOf[InsertIntoHiveTable])
         atlasClient.createEntities(withExtInfo)
       }

       case InsertIntoHadoopFsRelationCommand(outputPath,staticPartitions,ifPartitionNotExists,partitionColumns,bucketSpec,fileFormat,options,query,mode,catalogTable,fileIndex,outputColumnNames)=>{
         println("InsertIntoHadoopFsRelationCommand:"+ detail.cm)
       }
         //saveAsTable追踪 saveAsTable
       case CreateDataSourceTableAsSelectCommand(table,mode,query,outputColumnNames)=>{
         val withExtInfo = new CreateDataSourceTableAsSelectHarvester().harvest(detail.cm.asInstanceOf[CreateDataSourceTableAsSelectCommand])
         atlasClient.createEntities(withExtInfo)
       }
       case _ => logWarning("目前还未支持处理DataWritingCommand:"+ detail.cm)
     }
  }
}
