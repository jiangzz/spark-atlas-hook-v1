package com.jd

import org.apache.spark.sql.{SaveMode, SparkSession}


object SaprkSQLHookDemo {
  def main(args: Array[String]): Unit = {
    //配置spark
    val spark = SparkSession.builder()
      .appName("SparkSQL")
      .master("local[6]")
      .config("hive.metastore.uris","thrift://CentOS:9083")
      .config("spark.sql.queryExecutionListeners","com.jd.atlas.listener.SparkAtlasEventTracker")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("FATAL")
    spark.sql("use spark_db");

    spark.sparkContext.textFile("file:///E:\\IdeaProject\\spark-atlas-hpook\\spark-sql\\src\\main\\resources\\user\\t_user")
      .map(_.split("\\s+"))
      .map(ts=>(ts(0).toInt,ts(1),ts(2).toBoolean,ts(3).toInt,ts(4).toDouble))
      .toDF("id","name","sex","age","salary")
      .createTempView("t_tmp_user")

    spark.sql("drop table if exists t_results");
    val t_employee=
      """
        |CREATE EXTERNAL TABLE t_employee(
        |    id INT comment '用户id',
        |    name STRING comment '雇员姓名',
        |    job STRING comment '雇员职位',
        |    manager INT comment '经理职位',
        |    hiredate TIMESTAMP  comment '雇佣日期',
        |    salary DECIMAL(7,2) comment '员工薪资'
        |    )comment '员工信息表'
        |    PARTITIONED BY (deptno INT comment '员工部门')
        |    ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
        |    LOCATION 'hdfs://CentOS:9000/hive/t_employee'
        |""".stripMargin
    val t_dept=
      """
        |CREATE TABLE t_dept(
        |    DEPTNO INT comment '部门编号',
        |    DNAME STRING comment '部门名称',
        |    LOC STRING comment '部门所在地理信息'
        |    ) comment '员工部门表'
        |row format delimited
        |fields terminated by ','
        |collection items terminated by '|'
        |map keys terminated by '>'
        |lines terminated by '\n'
        |stored as textfile
        |LOCATION 'hdfs://CentOS:9000/hive/t_dept'
        |""".stripMargin


    spark.sql("drop database if exists jdjr cascade")
    spark.sql("drop table if exists t_employee")
    spark.sql("drop table if exists t_dept")
    spark.sql("drop table if exists t_employee01")
    spark.sql("drop view if exists t_employ_v")
    spark.sql("drop table if exists  results")
    spark.sql("drop table if exists  t_user")
    spark.sql("drop table if exists  t_save_001")
    spark.sql(t_employee)
    spark.sql(t_dept)

    spark.sql("load data local inpath 'file:///E:/IdeaProject/spark-atlas-hpook/spark-sql/src/main/resources/employee/t_employee' overwrite into table t_employee partition(deptno='10')");
    spark.sql("load data local inpath 'file:///E:/IdeaProject/spark-atlas-hpook/spark-sql/src/main/resources/dept/t_dept' overwrite into table t_dept");
    spark.sql("create table t_results as select e.name,e.job,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO")
    spark.sql("create table t_employee01 like t_employee")
    spark.sql("create table t_user  as select * from t_tmp_user")
    spark.sql("alter table t_results rename to results")
    spark.sql("create view if not exists t_employ_v as select * from t_employee")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //常见三种插入
    spark.sql("INSERT INTO TABLE results select e.name,e.job,e.manager,d.* from t_employee e left join t_dept d on e.DEPTNO=d.DEPTNO")
    spark.sql(
      """
        |from t_employ_v
        |insert overwrite table t_employee partition(deptno='20') select id,name,job,manager,hiredate,salary where deptno='10'
        |insert overwrite table t_employee partition(deptno='30') select id,name,job,manager,hiredate,salary where deptno='30'
        |""".stripMargin)

    spark.sql("select * from t_employ_v where deptno='10'")
         .write.mode(SaveMode.Overwrite)
         .insertInto("t_employee")

    spark.sql("select * from t_employee where deptno='10'")
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("hdfs://CentOS:9000/results")

    spark.sql("select * from t_employ_v where deptno='10'")
      .write.mode(SaveMode.Overwrite)
      .saveAsTable("t_save_001")

    spark.sql("drop table if exists t_employee")
    spark.sql("drop table if exists t_dept")
    spark.sql("drop table if exists t_employee01")
    spark.sql("drop view if exists t_employ_v")
    spark.sql("drop table if exists  results")
    spark.sql("drop table if exists  t_user")
    spark.sql("drop table if exists  t_save_001")
    spark.stop()
  }
}
