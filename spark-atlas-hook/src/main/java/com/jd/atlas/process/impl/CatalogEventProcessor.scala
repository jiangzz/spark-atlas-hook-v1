package com.jd.atlas.process.impl

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.process.AbstractProcessor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType

/**
 * 仅仅处理Catalog相关的事件
 * @param atlasClient
 */
class CatalogEventProcessor(atlasClient:AtlasClient) extends AbstractProcessor[ExternalCatalogEvent] with Logging{
  override protected def process(e: ExternalCatalogEvent): Unit = {

    val sparkSessionOption:Option[SparkSession] = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if(sparkSessionOption.isEmpty){
      throw new IllegalStateException("Cannot find active or default SparkSession in the current " + "context")
    }
    val sparkSession = sparkSessionOption.get
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val appName = sparkSession.sparkContext.appName
    val master = sparkSession.sparkContext.master

    logInfo("appName:"+appName+" master:"+master+" "+e.getClass)

    e match {
      case DropDatabaseEvent(database)=>{
          logInfo(s"删除spark_db:$database")
      }
      case CreateDatabaseEvent(database)=>{
        val sparkDB = externalCatalog.getDatabase(database)
        logInfo(s"名称：${sparkDB.name}，描述： ${sparkDB.description} 位置：${sparkDB.locationUri}")
      }
      case AlterDatabaseEvent(database)=>{
        println(s"AlterDatabaseEvent:$database")
      }
      case CreateTableEvent(db: String, tableName: String)=>{
        println(s"CreateTableEvent:${db} ${tableName}")
        val catalogTable = externalCatalog.getTable(db,tableName)
        var schema: StructType = catalogTable.schema
        val fields = schema.fields
        for (elem <- fields) {
          println(elem.name+" "+elem.dataType+" "+elem.getComment().getOrElse(""))
        }
      }
      case DropTableEvent(db,table)=>{
        println(s"删除${db}.${table}")
      }
      case AlterTableEvent(db,table,kind)=>{
        println(s"AlterTableEvent:${db}  ${table} $kind")
      }
      case RenameTableEvent(db,oldName,newName)=>{
        println(s"RenameTableEvent:$db $oldName  $newName")
        println(s"CreateTableEvent:${db} ${newName}")
        val catalogTable = externalCatalog.getTable(db,newName)
        var schema: StructType = catalogTable.schema
        val fields = schema.fields
        for (elem <- fields) {
          println(elem.name+" "+elem.dataType+" "+elem.getComment().getOrElse(""))
        }
      }
      case _ =>
    }
  }
}
