package com.jd.atlas.process

import org.apache.spark.sql.execution.command.DataWritingCommand
case class DataWritingCommandDetail(cm: DataWritingCommand, sql:Option[String])
