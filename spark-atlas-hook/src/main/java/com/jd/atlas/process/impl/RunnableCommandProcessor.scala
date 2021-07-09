package com.jd.atlas.process.impl

import com.jd.atlas.client.AtlasClient
import com.jd.atlas.process.AbstractProcessor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{AlterTableRenameCommand, CreateTableCommand, CreateTableLikeCommand, CreateViewCommand, DropTableCommand, RunnableCommand}

class RunnableCommandProcessor(atlasClient:AtlasClient) extends AbstractProcessor[RunnableCommand] with Logging{
  override protected def process(event: RunnableCommand): Unit = {
     event match {
       case DropTableCommand(tableName,ifExists,isView,purege)=>{
         println("DropTableCommand:"+tableName+" "+isView)
       }
       case  CreateTableCommand(table,ignoreIfExists)=>{
         println("CreateTableCommand：\t"+event)
       }
       case CreateTableLikeCommand(targetTable,sourceTable,location,ifNotExists)=>{
         println("CreateTableLikeCommand：\t"+event)
       }
       case AlterTableRenameCommand(oldName, newName, isView)=>{
         println("AlterTableRenameCommand："+event)
       }
       case CreateViewCommand(name, userSpecifiedColumns, comment, properties, originalText, child, allowExisting, replace, viewType)=>{
         println("CreateViewCommand:\t"+event)
       }
       case _=> logWarning(s"占时不支持该类型操作：${event}")
     }
  }
}
