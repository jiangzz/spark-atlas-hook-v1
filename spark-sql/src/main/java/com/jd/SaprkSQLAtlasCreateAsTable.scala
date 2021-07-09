package com.jd

import org.apache.spark.sql.SparkSession


/**
 * 測試CREATE AS
 */
object SaprkSQLAtlasCreateAsTable {
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
    spark.sql("drop table if exists t_results")
    spark.sql("create table t_results as select * from (select concat(e.name,e.job) name_job,abs(salary) abs_salary,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO)")

    Thread.sleep(1000)
    spark.stop()
  }
}
