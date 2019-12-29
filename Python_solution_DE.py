from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#### EMPLOYEE DATAFRAME AND VALIDATION REPORT ####
spark = SparkSession.builder.appName("Python Solution").getOrCreate()
emp_data = [
      ["1", "emp1", "aasa@asa.asa", "1967-04-02", "333-22-4444", "1"],
      ["2", "emp2", "asa@asasa..as", "1967-13-02", "333-22-4444", "1"],
      ["3", "", "asasaas.asoa", "1991-11-02", "333-22-44444", "2"],
      ["sds", "emp4", "jshhd@kkdd.com", "02-11-1991", "333-222-444", "10"],
      [None, None, "asas@asas.com", None, "333-22-4444", "asa"]
]

# Below exp : It will read and create a dataframe with the employee data. 
emp_df = spark.createDataFrame(emp_data,["emp_id", "emp_name", "emp_email","emp_dob", "emp_ssn", "emp_dept"])
# emp_df.show() # uncomment to see the data.

# Validation of fields using rlike functions. create one additional validation field for each column. Reason : It will give us more flexiblity.
# True : Value is right, False : Value is Wrong, Null will remain as it is.
emp_flag_df = emp_df.withColumn("emp_id_Valid",col("emp_id").rlike("^[0-9]*[0-9]$")) \
.withColumn("emp_name_Valid",col("emp_name").rlike("\w"))\
.withColumn("emp_email_Valid",col("emp_email").rlike("(\w|\d)+@(\w|\d)+\.(\w)+"))\
.withColumn("emp_dob_Valid",col("emp_dob").rlike("^(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$"))\
.withColumn("emp_ssn_Valid",col("emp_ssn").rlike("^[0-9]{3}-[0-9]{2}-[0-9]{4}$"))\
.withColumn("emp_dept_Valid",col("emp_dept").rlike("^[0-9]*[0-9]$"))
# emp_flag_df.show() # uncomment to see the data.

# Fetching only invalid Rows - required for validation report.
emp_invalid_recs = emp_flag_df.filter((~ col("emp_id_Valid")) | ( ~ col("emp_name_Valid")) | (~ col("emp_email_Valid")) | (~ col("emp_dob_Valid")) | ( ~ col("emp_ssn_Valid")) | (~ col("emp_dept_Valid")))
# .withColumn("Validation_message",col('Not Valid'))
# emp_invalid_recs.show()
emp_invalid_recs.createOrReplaceTempView("emp_invalid_recs")

emp_report = spark.sql("select * from (Select 'Employee' entityName, case when ! emp_id_Valid then 'emp_id' end fieldName,case when ! emp_id_Valid then emp_id end fieldValue, 'Validation Failed' validationMessage from emp_invalid_recs \
union all \
Select 'Employee' entityName, case when ! emp_name_Valid then 'emp_name' end,case when ! emp_name_Valid then emp_name end, 'Validation Failed' validationMessage from emp_invalid_recs \
union all \
Select 'Employee' entityName, case when ! emp_email_Valid then 'emp_email' end,case when ! emp_email_Valid then emp_email end, 'Validation Failed' validationMessage from emp_invalid_recs \
union all \
Select 'Employee' entityName, case when ! emp_dob_Valid then 'emp_dob' end,case when ! emp_dob_Valid then emp_dob end, 'Validation Failed' validationMessage from emp_invalid_recs \
union all \
Select 'Employee' entityName, case when ! emp_ssn_Valid then 'emp_ssn' end,case when ! emp_ssn_Valid then emp_ssn end, 'Validation Failed' validationMessage from emp_invalid_recs \
union all \
Select 'Employee' entityName, case when ! emp_dept_Valid then 'emp_dept' end,case when ! emp_dept_Valid then emp_dept end, 'Validation Failed' validationMessage from emp_invalid_recs) where fieldName is not null or fieldValue is not null"
                     )
# emp_report.show()
#### DEPARTMENT DATAFRAME AND VALIDATION REPORT ####

dept_data = [
      ["1", "ADMIN", "", "A", "87.2"],
      ["2", "IT", "asas", "C", "23.4"],
      ["3", "hr", "asas", "A", ""],
      ["sxdsd", "DELIVERY", "asas", "B", "asas"]
]

# Below exp : It will read and create a dataframe with the department data. 
dept_df = spark.createDataFrame(dept_data,["dept_id", "dept_name", "dept_address", "test_col", "npi"])
# dept_df.show() # uncomment to see the data.


# Validation of fields using rlike functions. create one additional validation field for each column. Reason : It will give us more flexiblity.
# True : Value is right, False : Value is Wrong, Null will remain as it is.
dept_flag_df = dept_df.withColumn("dept_id_flag",col("dept_id").rlike("^[0-9]*[0-9]$"))\
.withColumn("dept_name_flag",col("dept_name").rlike("^[A-Z]+$"))\
.withColumn("dept_address_flag",col("dept_address").rlike(".*{100}"))\
.withColumn("test_col_flag",col("test_col").rlike("^[ABD]$"))\
.withColumn("npi_flag",col("npi").rlike("^[0-9]+\.([0-9]+)$"))
# dept_flag_df.show() # uncomment to see the data.

# Fetching only invalid Rows - required for validation report.
dept_invalid_recs = dept_flag_df.filter((~ col("dept_id_flag")) | (~ col("dept_name_flag")) | (~ col("dept_address_flag")) | (~ col("test_col_flag")) | (~ col("npi_flag")))
# dept_invalid_recs.show()
dept_invalid_recs.createOrReplaceTempView("dept_invalid_recs")

dept_report = spark.sql("select * from(Select 'Department' entityName, case when ! dept_id_flag then 'dept_id' end fieldName,case when ! dept_id_flag then dept_id end fieldValue, 'Validation Failed' validationMessage from dept_invalid_recs \
union all \
Select 'Department' entityName, case when ! dept_name_flag then 'dept_name' end,case when ! dept_name_flag then dept_name end, 'Validation Failed' validationMessage from dept_invalid_recs \
union all \
Select 'Department' entityName, case when ! dept_address_flag then 'dept_address' end,case when ! dept_address_flag then dept_address end, 'Validation Failed' validationMessage from dept_invalid_recs \
union all \
Select 'Department' entityName, case when ! test_col_flag then 'test_col' end,case when ! test_col_flag then test_col end, 'Validation Failed' validationMessage from dept_invalid_recs \
union all \
Select 'Department' entityName, case when ! npi_flag then 'npi' end,case when ! npi_flag then npi end, 'Validation Failed' validationMessage from dept_invalid_recs ) where fieldName is not null or fieldValue is not null "
                     )

# dept_report.show() # uncomment to see the data.

print('### VALIDATION REPORT ###')
emp_report.unionAll(dept_report).show()

# Benefits of using Dataframe Approach.
# We can easily combine the output of employee and department further to analyze some more things.
# We can apply custom filter condition to tailored the result based on our requirements.
# In terms of performance gain we can cache the initial dataframe.
# Write the dataframe directly to Cloud or any other ecosystem.