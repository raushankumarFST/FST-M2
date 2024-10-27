-- Load the CSV file
salesTable = LOAD 'hdfs:///user/raushan/sales.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Group data using the country column
GroupByCountry = GROUP salesTable BY Country;
-- Generate result format
CountByCountry = FOREACH GroupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
--delete the output folder
rmf hdfs:///user/raushan/pigresults_3;
-- Save result in HDFS folder
STORE CountByCountry INTO 'hdfs:///user/raushan/pigresults_3';