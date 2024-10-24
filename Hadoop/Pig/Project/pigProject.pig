-- Load input file from HDFS
lines = LOAD 'hdfs:///user/raushan/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
lines = LOAD 'hdfs:///user/raushan/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
lines = LOAD 'hdfs:///user/raushan/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Group by
grouped_lines = GROUP lines BY name;

-- Count the number of lines spoken by each character
character_line_count = FOREACH grouped_lines GENERATE $0 AS character, COUNT($1) AS line_count;


-- Order the results in descending order
ordered_character_line_count = ORDER character_line_count BY line_count DESC;

-- Store the result
STORE ordered_character_line_count INTO 'hdfs:///user/raushan/pigProjectResult' USING PigStorage(',');