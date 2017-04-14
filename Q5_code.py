# pyspark --packages com.databricks:spark-csv_2.10:1.2.0 Q3_code.py

# Import unix_timestamp to convert the datetime columns to 
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext,SQLContext
from pyspark.sql.functions import unix_timestamp
from subprocess import call
conf = SparkConf().setAppName("Q2")
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

### Load the data as spar dataframe using sqlContext
df = sqlContext.read.load('pxb161930/Police_Incidents.csv', format='com.databricks.spark.csv', header='true', inferSchema='true' , parserLib ='univocity')
## Data Cleaning
dl = ['Incident Number w/ Year', 'Watch', 'Call (911) Problem', 'Type of Incident', 'Penalty Class', 'Type of Location', 'Type of Property', 'Street Block', 'Street Direction', 'Street Name', 'Incident Address', 'City', 'X Coordinate', 'Y Coordinate', 'Reporting Area', 'Beat', 'Division', 'Sector', 'Council District', 'Target Area Action Grids', 'Community','Ending Date/Time', 'Map Date', 'Date of Report', 'Date incident created', 'Offense Entered Time', 'Offense Entered  Date/Time', 'Call Date Time','Call Dispatch Date Time', 'Complainant Age', 'Complainant Age at Offense', 'Complainant Home Address', 'Complainant Zip Code', 'Complainant City', 'Complainant Business Name', 'Complainant Business Address', 'Investigating Unit 1', 'Investigating Unit 2', 'Offense Status', 'Victim Injury Description', 'Victim Condition', 'RMS Code', 'Offense Code CC', 'CJIS Code', 'Penal Code', 'UCR Offense Name', 'Modus Operandi (MO)', 'Hate Crime', 'Gang Related Offense', 'Drug Related Incident']

## remove the unneccessary columns
df11 = df.select([column for column in df.columns if column not in dl])

#Commad to replace Spaces with _
newdf = df11.toDF(*(c.replace(' ', '_') for c in df11.columns)) 
ndf = newdf.toDF(*(c.replace('/', '') for c in newdf.columns))
sdf = ndf.toDF(*(c.replace('(', '') for c in ndf.columns))
adf = sdf.toDF(*(c.replace(')', '') for c in sdf.columns))
ddf = adf.toDF(*(c.replace('__', '_') for c in adf.columns))

sqlContext.sql("drop table if exists default.q5data");

# load the cleaned dataset as hive table
ddf.registerTempTable("q5data")
sqlContext.sql("create table q5table as select * from q5data");


## create CSV file fro Hive table

args = ["hive","-e" , "INSERT OVERWRITE LOCAL DIRECTORY '/root/q5data' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select * from q5table;"]
call(args)

sc.close()
