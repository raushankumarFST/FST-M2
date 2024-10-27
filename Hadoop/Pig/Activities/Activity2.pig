-- Load input file into pig
inputFile = LOAD 'hdfs:///user/raushan/input.txt' AS (lines:chararray);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) AS wordCount;
-- Delete the output folder
rmf hdfs:///user/raushan/pigresults;
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/raushan/pigresults';