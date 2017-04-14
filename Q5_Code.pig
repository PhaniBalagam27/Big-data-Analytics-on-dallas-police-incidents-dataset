Police = LOAD 'pxb161930/q5data.csv' using PigStorage(',');
B = LOAD 'pxb161930/Population.csv' using PigStorage(',');
Police_zip = foreach Police generate $0 as serviceid, $1 as zipcode;
STORE Police_zip  INTO 'pxb161930/pig_policezip' USING PigStorage (',');
Police_zip = LOAD 'pxb161930/pig_policezip' USING PigStorage (',') as (serviceid: chararray , zipcode:chararray);
Incidents_grp = group Police_zip by zipcode;
Incidents_cnt_zip = foreach Incidents_grp generate group , COUNT(Police_zip.serviceid) as crimerate;
Population = foreach B generate $0 as zipcode , $1 as population;
STORE Population INTO 'pxb161930/pig_pop' USING PigStorage (',');
Population = LOAD 'pxb161930/pig_pop' USING PigStorage (',') as (zipcode:chararray , population: double);
Police_Pop = JOIN Incidents_cnt_zip by group, Population by zipcode ;
pop_zip_crimerate = foreach Police_Pop generate (double)Incidents_cnt_zip::crimerate as crimerate , Population::population as pop ;
rel = GROUP pop_zip_crimerate ALL;
correlat = foreach rel GENERATE COR(pop_zip_crimerate.crimerate , pop_zip_crimerate.pop);
dump correlat;
