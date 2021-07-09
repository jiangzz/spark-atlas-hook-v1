package com.jd

import org.apache.spark.sql.SparkSession


/**
 * 測試CREATE AS
 */
object SaprkSQLAtlasInserIntoSelect {
  def main(args: Array[String]): Unit = {
    //配置spark
    val spark = SparkSession.builder()
      .appName("SaprkSQLAtlasTestTableLineage")
      .master("yarn")
      .config("hive.metastore.uris","thrift://CentOS:9083")
      .enableHiveSupport()
      .getOrCreate()
    //import org.apache.spark.sql.functions.abs
    spark.sparkContext.setLogLevel("FATAL")
    spark.sql("use spark_db");
    spark.sql("drop table if exists t_results")
//    spark.sql("create table t_results as select concat(e.name,e.job) name_job,abs(salary) abs_salary,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO")
    spark.sql("from (select concat(e.name,e.job) name_job,abs(salary) abs_salary,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO) insert overwrite table t_emp_dim select *")

    spark.stop()
  }

  //from (select concat(e.name,e.job) name_job,abs(salary) abs_salary,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO) insert overwrite table t_emp_idm select name_job, abs_salary,manager,dname,loc
}
