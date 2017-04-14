# pyspark --packages com.databricks:spark-csv_2.10:1.2.0 Q2_code.py

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
dl = ['Street Block', 'Street Direction','Beat', 'Division', 'Sector', 'Council District', 'Target Area Action Grids', 'Community', 'Complainant Race', 'Complainant Gender', 'Complainant Age', 'Complainant Age at Offense', 'Complainant Home Address', 'Complainant Zip Code', 'Complainant City', 'Complainant Business Name', 'Complainant Business Address', 'Investigating Unit 1', 'Investigating Unit 2', 'Offense Status', 'Victim Injury Description', 'Victim Condition', 'RMS Code', 'Offense Code CC', 'CJIS Code', 'Penal Code', 'UCR Offense Name', 'Modus Operandi (MO)', 'Hate Crime', 'Gang Related Offense']
## remove the unneccessary columns
df11 = df.select([column for column in df.columns if column not in dl])

#Commad to replace Spaces with _
newdf = df11.toDF(*(c.replace(' ', '_') for c in df11.columns)) 
ndf = newdf.toDF(*(c.replace('/', '') for c in newdf.columns))
sdf = ndf.toDF(*(c.replace('(', '') for c in ndf.columns))
adf = sdf.toDF(*(c.replace(')', '') for c in sdf.columns))
ddf = adf.toDF(*(c.replace('__', '_') for c in adf.columns))


# Convert timestamp
n = ddf.withColumn("Call_Received_Date_Time", unix_timestamp("Call_Received_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
a = n.withColumn("Starting_DateTime", unix_timestamp("Starting_DateTime", "MM/dd/yyyy HH:mm").cast("timestamp"))
b = a.withColumn("Ending_DateTime", unix_timestamp("Ending_DateTime", "MM/dd/yyyy HH:mm").cast("timestamp"))
s = b.withColumn("Call_Date_Time", unix_timestamp("Call_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
g = s.withColumn("Call_Cleared_Date_Time", unix_timestamp("Call_Cleared_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
newdf = g.withColumn("Call_Dispatch_Date_Time", unix_timestamp("Call_Dispatch_Date_Time", "MM/dd/yyyy HH:mm").cast("timestamp"))
nd = newdf.withColumn("Update_Date", unix_timestamp("Update_Date", "MM/dd/yyyy HH:mm").cast("timestamp"))

sqlContext.sql("drop table if exists default.mytable");

# load the cleaned dataset as hive table
nd.registerTempTable("q2data")
sqlContext.sql("create table mytable as select * from q2data");

# run the hive query 
sqlContext.sql("select e.zip_code, w.Avgresponsetime,e.duration, e.cduration  from (select m.zip_code, AVG(unix_timestamp(Call_Received_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from mytable m left join police_station_data p on m.zip_code = p.zip_code group by m.zip_code order by Avgresponsetime desc) w,(select a.zip_code, a.duration, count(a.duration) as cduration from (SELECT m.zip_code, CASE WHEN hour(Call_Received_Date_Time) <14 THEN 'Morning' WHEN hour(Call_Received_Date_Time) <17 THEN 'Afternoon' WHEN hour(Call_Received_Date_Time) <21 THEN 'Evening' ELSE 'Night' END as duration from mytable m left join police_station_data p on m.zip_code = p.zip_code) a group by a.zip_code,a.duration) e where w.zip_code=e.zip_code group by e.zip_code,w.Avgresponsetime,e.duration,e.cduration having e.cduration > 100").show();


args = ["hive","-e" , "INSERT OVERWRITE LOCAL DIRECTORY '/root/q2data' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select e.zip_code, w.Avgresponsetime,e.duration, e.cduration  from (select m.zip_code, AVG(unix_timestamp(Call_Received_Date_Time)-unix_timestamp(Starting_DateTime))/60/60 as Avgresponsetime from mytable m left join police_station_data p on m.zip_code = p.zip_code group by m.zip_code order by Avgresponsetime desc) w,(select a.zip_code, a.duration, count(a.duration) as cduration from (SELECT m.zip_code, CASE WHEN hour(Call_Received_Date_Time) <14 THEN 'Morning' WHEN hour(Call_Received_Date_Time) <17 THEN 'Afternoon' WHEN hour(Call_Received_Date_Time) <21 THEN 'Evening' ELSE 'Night' END as duration from mytable m left join police_station_data p on m.zip_code = p.zip_code) a group by a.zip_code,a.duration) e where w.zip_code=e.zip_code group by e.zip_code,w.Avgresponsetime,e.duration,e.cduration having e.cduration > 100;"]

call(args)

#args1 = ["cat", "000000_0" ,">" ,"Q2output.csv"]

with open('/root/q2data/q2_Avg_Response_time.csv','wb') as w:
	with open('/root/q2data/000000_0','rb') as r:
		print("writing data to file for q2data")
		shutil.copyfileobj(r ,w)
	r.close()
w.close()

#call(args1)


sc.close()
