# pyspark --packages com.databricks:spark-csv_2.10:1.2.0 Q3_code.py

# Import unix_timestamp to convert the datetime columns to 

import os
import sys
import shutil
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

##
n = ddf.withColumn("Call_Cleared_Date_Time", unix_timestamp("Call_Cleared_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
a = n.withColumn("Starting_DateTime", unix_timestamp("Starting_DateTime", "MM/dd/yyyy HH:mm").cast("timestamp"))
nq = a.withColumn("Call_Received_Date_Time", unix_timestamp("Call_Received_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
nd = nq.withColumn("Update_Date", unix_timestamp("Update_Date", "MM/dd/yyyy HH:mm").cast("timestamp"))

sqlContext.sql("drop table if exists default.q3data");
sqlContext.sql("drop table if exists default.q3table");

# load the cleaned dataset as hive table
nd.registerTempTable("q3data")
sqlContext.sql("create table q3table as select * from q3data");

## queries for Q3
sqlContext.sql("select person_involvement_type,count(person_involvement_type),AVG(unix_timestamp(Call_Cleared_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from q3table group by person_involvement_type").show();

#race reported and the zipcode they are reporting from.
sqlContext.sql("select complainant_race, complainant_Gender, AVG(unix_timestamp(Call_Received_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from q3table group by complainant_race,complainant_Gender order by Avgresponsetime desc").show();


## create CSV file fro Hive table

args = ["hive","-e" , "INSERT OVERWRITE LOCAL DIRECTORY '/root/q3data_1' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select person_involvement_type,count(person_involvement_type),AVG(unix_timestamp(Call_Cleared_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from q3table group by person_involvement_type;"]

call(args)

with open('/root/q3data_1/q3_Respone_time.csv','wb') as w:
	with open('/root/q3data_1/000000_0','rb') as r:
		print("writing data to file for q3data1")
		shutil.copyfileobj(r ,w)
	r.close()
w.close()

#args1 = ["hdfs","dfs","-cat", "/root/q3data_1/000000_0" ,">" ,"/root/q3data_1/Q3_Response_time.csv"]



args2 = ["hive","-e" , "INSERT OVERWRITE LOCAL DIRECTORY '/root/q3data_2' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select complainant_race, complainant_Gender, AVG(unix_timestamp(Call_Received_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from q3table group by complainant_race,complainant_Gender order by Avgresponsetime desc;"]
#args3 = ["hdfs","dfs","-cat", "/root/q3data_2/000000_0" ,">" ,"/root/q3data_2/Q3_Complainant_table.csv"]

call(args2)

with open('/root/q3data_2/q3_Complainant_table.csv','wb') as i:
	with open('/root/q3data_2/000000_0','rb') as o:
		print("writing data to q3data2")
		shutil.copyfileobj( o,i)
	o.close()
i.close()


# Closing hive context
sqlContext.close()


sc.close()

