package com.jd.atlas.parser.impl

import java.util
import java.util.Date

import com.jd.atlas.client.ext.NotificationContextHolder
import com.jd.atlas.config.AtlasClientConfiguration
import com.jd.atlas.interceptor.SQLQueryContext
import com.jd.atlas.parser.Harvester
import org.apache.atlas.`type`.AtlasTypeUtil
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId, AtlasRelatedObjectId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}

import scala.collection.mutable.{HashMap, ListBuffer}

class CreateDataSourceTableAsSelectHarvester extends Harvester[CreateDataSourceTableAsSelectCommand] with Logging{
  override def harvest(node: CreateDataSourceTableAsSelectCommand): AtlasEntity.AtlasEntitiesWithExtInfo = {
    //解析出目标表
    val tableName = node.table.identifier.table
    var database:String="null";
    if(node.table.identifier.database.isEmpty){
      val sparkSession = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      database = sparkSession.get.sessionState.catalog.getCurrentDatabase
    }else{
      database = node.table.identifier.database.getOrElse("default")
    }
    val namespace=AtlasClientConfiguration.getMetaNamespace()
    val resultTable=s"${database}.${tableName}@${namespace}"
    //解析出输入表
    val tableRelations = node.collectLeaves().collect({ case t: HiveTableRelation => t })
    val inputTables = tableRelations.map(table => table.tableMeta).map(t => s"${t.identifier.database.getOrElse("default")}.${t.identifier.table}@${namespace}")

    logInfo(s"创建表血缘:${inputTables.mkString(" , ")} ->${resultTable}")

    //完成Hive表的血缘解析
    val tableProcessEntity = harvestTableLineage(resultTable, inputTables)
    val columnLineageEntities=ListBuffer[AtlasEntity]()
    val logicalPlan = node.query
    if(logicalPlan.isInstanceOf[Project]){
      val tableColumnsMap = new HashMap[Long, String]
      //解析输入表的所有字段信息
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.exprId.id,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
      })

      val project = logicalPlan.asInstanceOf[Project]
      val outputsColumns = project.projectList
      for (elem <- outputsColumns) {
        val expID = elem.exprId.id
        val columnName=s"${database}.${tableName}.${elem.name.toLowerCase}@${namespace}"
        if(tableColumnsMap.contains(expID)){
          columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,List(tableColumnsMap.get(expID).get),null,tableProcessEntity)
        }else{
          //获取子字段血缘信息
          if(!elem.children.isEmpty){
            var expression = elem.children.collect({ case e: Expression => e }).mkString(",")
            //获取所有的关联字段的信息
            var listColumns=ListBuffer[String]()
            elem.collectLeaves().foreach(e=>{
              val namedExpression = e.asInstanceOf[NamedExpression]
              val colId = namedExpression.exprId.id
              if(tableColumnsMap.contains(colId)){
                listColumns +=tableColumnsMap.get(colId).get
              }
            })
            if(listColumns.size==1 && ! (expression.contains("(") && expression.contains(")"))){
              expression=null;
            }
            columnLineageEntities+=harvestColumnLineage(database,tableName,namespace,elem.name.toLowerCase,columnName,listColumns,expression,tableProcessEntity)
          }
        }
      }
    }else if(logicalPlan.isInstanceOf[HiveTableRelation]){
      //说明是直接create xxx as select * from
      //解析输入表的所有字段信息
      val tableColumnsMap = new HashMap[String, String]
      tableRelations.map(table=>(table.tableMeta,table.dataCols,table.partitionCols)).foreach(t=>{
        val identifier = t._1.identifier
        for (elem <- t._2.map(_.asInstanceOf[NamedExpression])) {
          tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
        }
        if(!t._3.isEmpty){//处理分区字段
          for (elem <- t._3.map(_.asInstanceOf[NamedExpression])) {
            tableColumnsMap.put(elem.name,s"${identifier.database.getOrElse("default")}.${identifier.table}.${elem.name}@${namespace}".toLowerCase())
          }
        }
        for (elem <- tableColumnsMap.keySet) {
          columnLineageEntities+=harvestColumnLineage(database,
            tableName,
            namespace,
            elem.toLowerCase, s"${database}.${tableName}.${elem.toLowerCase}@${namespace}",
            List(tableColumnsMap.get(elem).get),"DirectMapping",tableProcessEntity)
        }
      })
    }
    //设置消息key
    NotificationContextHolder.setMessageKey(resultTable)
    val withExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo()
    withExtInfo.addEntity(tableProcessEntity)
    columnLineageEntities.foreach(entity=>withExtInfo.addEntity(entity))
    return withExtInfo
  }

  def harvestColumnLineage(database:String,
                           table:String,
                           cluster:String,
                           shortColumn:String,
                           resultColumn:String,
                           inputColumns:Seq[String], expression:String,hiveProcess:AtlasEntity):AtlasEntity={

    logInfo(s"创建 ${resultColumn} - ${expression} -> ${inputColumns.mkString(",")} 血缘")

    val atlasEntity: AtlasEntity = new AtlasEntity("hive_column_lineage")
    //封装输入表信息
    val inputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    inputColumns.foreach(column=>inputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_column", "qualifiedName", column), "dataset_process_inputs")))

    //封装输出表信息
    val outputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    outputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_column", "qualifiedName", resultColumn), "process_dataset_outputs"))

    atlasEntity.setRelationshipAttribute("inputs", inputs)
    atlasEntity.setRelationshipAttribute("outputs", outputs)

    val qualifiedName=s"${database}.${table}@${cluster}:${shortColumn}"
    atlasEntity.setAttribute("qualifiedName", qualifiedName)
    atlasEntity.setAttribute("name", qualifiedName)
    if(expression==null){
      atlasEntity.setAttribute("depenendencyType", "SIMPLE")
    }else{
      atlasEntity.setAttribute("depenendencyType", "EXPRESSION")
      atlasEntity.setAttribute("expression", expression)
    }
    atlasEntity.setAttribute("startTime", new Date)
    atlasEntity.setAttribute("endTime", new Date)
    atlasEntity.setRelationshipAttribute("query", AtlasTypeUtil.getAtlasRelatedObjectId(hiveProcess,"hive_process_column_lineage"))
    atlasEntity
  }

  def harvestTableLineage(resultTable:String,inputTables:Seq[String]):AtlasEntity={

    val atlasEntity: AtlasEntity = new AtlasEntity("hive_process")
    //封装输入表信息
    val inputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    inputTables.foreach(table=>inputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_table", "qualifiedName", table), "dataset_process_inputs")))

    //封装输出表信息
    val outputs: util.List[AtlasRelatedObjectId] = new util.ArrayList[AtlasRelatedObjectId]
    outputs.add(new AtlasRelatedObjectId(new AtlasObjectId("hive_table", "qualifiedName", resultTable), "process_dataset_outputs"))

    atlasEntity.setRelationshipAttribute("inputs", inputs)
    atlasEntity.setRelationshipAttribute("outputs", outputs)

    val qualifiedName=s"${resultTable}"
    atlasEntity.setAttribute("qualifiedName", "QUERY:"+inputTables.reduce(_+":"+_)+"->:INSERT_OVERWRITE:"+resultTable)
    atlasEntity.setAttribute("name", "QUERY:"+inputTables.reduce(_+":"+_)+"->:INSERT_OVERWRITE:"+resultTable)
    atlasEntity.setAttribute("userName", "Spark")
    atlasEntity.setAttribute("startTime", new Date)
    atlasEntity.setAttribute("endTime", new Date)
    var querySQL = SQLQueryContext.get()
    if(querySQL==null || querySQL==""){
      querySQL="FROM_TABLE_INSERT_SELECT"
    }
    atlasEntity.setAttribute("operationType", "QUERY")
    atlasEntity.setAttribute("queryId", querySQL)
    atlasEntity.setAttribute("queryText", querySQL)
    atlasEntity.setAttribute("recentQueries", util.Collections.singletonList(querySQL))
    atlasEntity.setAttribute("queryPlan", "Not Supported")
    atlasEntity.setAttribute("clusterName", AtlasClientConfiguration.getMetaNamespace())
    atlasEntity
  }
}
