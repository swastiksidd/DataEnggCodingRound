# DataEnggCodingRound

## This is a sbt Scala Project with boilerplate code for instantiating spark is already included. 

## Main File to Run:
[TestPipelineMain](src/test/scala/com/imaginea/dataengg/codinground/schemavalidation/TestPipelineMain.scala)

## It has two Raw Data DataFrames:

### Employee
#### Schema
"emp_id": Integer, 

"emp_name": Text, 

"emp_email": Text with email pattern(Take any standard format), 

"emp_dob": Formatted Date Text(YYYY-MM-DD), 

"emp_ssn": Text with format "AAA-GG-SSSS" where A,G,S~>[0-9],

"emp_dept": Integer


### Department
#### Schema
"dept_id": Integer,

"dept_name": All uppercase Text with no space,

"dept_address": Free Text with max length 100,

"test_col": Text should be among one of these(A, B, D),

"npi": Double


## Task

**Input**: The above DataFrames One By One

**Output**: Corresponding Validation Report Dataframe or Dataset in [Validation Report](src/main/scala/com/imaginea/dataengg/codinground/ValidationReport.scala) format

### Validations to consider:
- Data Types mentioned
- Formats if any
- Any other validations are welcome

## NOTE: The validation report will consist of the error instances only.
