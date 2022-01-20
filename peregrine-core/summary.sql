/* Summarize all operators */
"SELECT OperatorName, Subgraphs, DistinctSubgraphs, " +
    " (Subgraphs/DistinctSubgraphs) AS AverageRepeats" +
    " FROM ("+
        " SELECT OperatorName, COUNT(*) AS Subgraphs, COUNT(DISTINCT HashTag) AS DistinctSubgraphs" +
        " FROM repeated_table " +
        "GROUP BY OperatorName ) t";

/* Summarize repeat operators */
"SELECT OperatorName, Subgraphs, DistinctSubgraphs, " +
    "(Subgraphs/DistinctSubgraphs) AS AverageRepeats FROM ( " +
        "SELECT OperatorName, COUNT(DISTINCT JobGUID) AS Jobs, " +
        "COUNT(DISTINCT JobUser) AS Users, " +
        "COUNT(DISTINCT JobVirtualCluster) AS Accounts, " +
        "COUNT(*) AS Subgraphs, " +
        "COUNT(DISTINCT HashTag) AS DistinctSubgraphs, " +
        "COUNT(DISTINCT HashTagNonStrict) AS DistinctRecurringSubgraphs " +
        "FROM base_table GROUP BY OperatorName) Table";

/* Workload Analysis Summary */
SELECT COUNT(DISTINCT JobGUID) AS Queries, COUNT(DISTINCT Users) AS Users,
COUNT (HashTag) AS Subexpressions, COUNT (DISTINCT HashTag) AS DistinctSubexpressions
FROM name;




/* Operator Summary */
SELECT OperatorName, COUNT(HashTag)
FROM name
GROUP BY OperatorName;
HAVING COUNT(HashTag) > 1;

"SELECT OperatorName, COUNT(HashTag) " +
" FROM " +
" ( SELECT DISTINCT OperatorName, HashTag " +
        " FROM " + input.getName() + ", " +
        " ( SELECT HashTag AS RepHashTag " +
        " FROM " + input.getName() +
        " GROUP BY HashTag " +
        " HAVING COUNT(HashTag) > 1 ) AS RepTags " +
        " WHERE HashTag = RepTags.RepHashTag ) AS A "
" GROUP BY OperatorName "

SELECT DISTINCT OperatorName, HashTag
FROM name,
( SELECT HashTag AS RepHashTag
FROM name
GROUP BY HashTag
HAVING COUNT(HashTag) > 1 ) AS RepTags
WHERE name.HashTag = RepTags.RepHashTag



/* Selected views */
SELECT *
FROM name;


SELECT HashTag, COUNT(*) AS Freq
FROM name
GROUP BY HashTag \n
HAVING VS_Frequency > 1
WHERE HashTag = "Join"


SELECT temp.VS_Frequency, v.*
FROM
( SELECT HashTag, COUNT(*) AS VS_Frequency
FROM name
GROUP BY HashTag \n
HAVING VS_Frequency > 1
) AS temp
INNER JOIN
name AS v
ON v.HashTag = temp.HashTag
WHERE v.OperatorName = 'Join'

