package com.jd

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * 測試CREATE AS
 */
object SaprkSQLAtlasSaveAsTable {
  def main(args: Array[String]): Unit = {
    //配置spark
    val spark = SparkSession.builder()
      .appName("SaprkSQLAtlasTestTableLineage")
      .master("local[*]")
      .config("hive.metastore.uris","thrift://CentOS:9083")
      .config("spark.sql.queryExecutionListeners","com.jd.atlas.listener.SparkAtlasEventTracker")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("FATAL")
    spark.sql("use spark_db");
    spark.sql("select concat(e.name,e.job) name_job,abs(salary) abs_salary,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO")
        .write.mode(SaveMode.Overwrite)
        .saveAsTable("t_results")
    Thread.sleep(1000)
    spark.stop()
  }
}
