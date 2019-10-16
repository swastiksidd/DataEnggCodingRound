package com.imaginea.dataengg.codinground.base

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestBaseSpark extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  implicit def spark: SparkSession = _spark

  val sparkConf = new SparkConf()
    .set("spark.hadoop.yarn.resourcemanager.address", "prinhyltphp0118:8050")
    .set("spark.hadoop.fs.defaultFS", "hdfs://prinhyltphp0118:8020")
    .set("spark.yarn.jars", "hdfs://prinhyltphp0118:8020/spark_yarn_jars/*.jar")
    .set("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*," +
      "$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*," +
      "$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")


  override def beforeAll() {
    _spark = SparkSession
      .builder
      .appName("DataPreprocessingValidationTest")
      //.config(sparkConf)
      //.master("yarn")
      .master("local[1]")
      .getOrCreate()


    _spark.sparkContext.setLogLevel("ERROR")
  }

}
