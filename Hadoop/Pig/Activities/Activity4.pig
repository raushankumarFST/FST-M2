-- Load input file into pig
inputFile = LOAD '/root/input.txt' AS (lines:chararray);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) AS wordCount;
-- Store the result in local
STORE cntd INTO '/root/pigresults_4';