package com.imaginea.dataengg.codinground.schemavalidation

import com.imaginea.dataengg.codinground.base.TestBaseSpark
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.scalatest.{FlatSpec, Matchers}

class TestPipelineMain extends FlatSpec with Matchers with TestBaseSpark {

  getClass.getSimpleName + "Test" should "" in {

    spark.conf.set("spark.sql.codegen.wholeStage", "true")

    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    val data = Seq(
      ("1", "emp1", "aasa@asa.asa", "1967-04-02", "333-22-4444", "1"),
      ("2", "emp2", "asa@asasa..as", "1967-13-02", "333-22-4444", "1"),
      ("3", "", "asasaas.asoa", "1991-11-02", "333-22-44444", "2"),
      ("sds", "emp4", "jshhd@kkdd.com", "02-11-1991", "333-222-444", "10"),
      (null, null, "asas@asas.com", null, "333-22-4444", "asa")
    )

    import org.apache.spark.sql.functions.col
    val emp_df = spark.sparkContext.parallelize(data)
      .toDF("emp_id", "emp_name", "emp_email", "emp_dob", "emp_ssn", "emp_dept")
      //.withColumn("emp_dob", col("emp_dob").cast(DateType))
      //.withColumn("emp_id", col("emp_id").cast(IntegerType))

    val dept_df = spark
      .sparkContext
      .parallelize(List(
        ("1", "ADMIN", "", "A", "87.2"),
        ("2", "IT", "asas", "C", "23.4"),
        ("3", "hr", "asas", "A", ""),
        ("sxdsd", "DELIVERY", "asas", "B", "asas")
      ))
      .toDF("dept_id", "dept_name", "dept_address", "test_col", "npi")

    dept_df.show()
    emp_df.show()

    ///emp_df.join(emp_dept=dept_id),dept_df.count





  }


}
