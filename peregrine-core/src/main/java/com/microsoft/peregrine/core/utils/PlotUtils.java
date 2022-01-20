package com.microsoft.peregrine.core.utils;

import com.microsoft.peregrine.core.planir.preprocess.data.ir.DataFrameIR;

public class PlotUtils {
  public void writeSubexpressionAnalysis(DataFrameIR input) {
    String fileName = "subexp.txt";
    String[] header = { "Queries", "Users", "Subexpressions", "Duplicate Subexpressions" };
    String query = "SELECT COUNT(DISTINCT JobGUID) AS Queries, \nCOUNT (HashTag) AS Subexpressions, COUNT (DISTINCT HashTag) AS DistinctSubexpressions\nFROM " + input.getName();
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_subexp");
    output.persist();
    output.writeToFile(fileName);
  }
  
  public void writeOperatorAnalysis(DataFrameIR input) {
    String fileName = "opexp.txt";
    String[] header = { "Operator", "Frequency", "Repeats", "Repeat Occurrences" };
    String query = "SELECT OperatorName, Subgraphs, DistinctSubgraphs, (Subgraphs/DistinctSubgraphs) AS AverageRepeats FROM ( SELECT OperatorName, COUNT(DISTINCT JobGUID) AS Jobs, COUNT(DISTINCT JobUser) AS Users, COUNT(DISTINCT JobVirtualCluster) AS Accounts, COUNT(*) AS Subgraphs, COUNT(DISTINCT HashTag) AS DistinctSubgraphs, COUNT(DISTINCT HashTagNonStrict) AS DistinctRecurringSubgraphs FROM " + input.getName() + " GROUP BY OperatorName) Table";
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_opexp");
    output.persist();
    output.writeToFile(fileName);
    countRepeatedOperators(input);
  }
  
  public void countRepeatedOperators(DataFrameIR input) {
    String fileName = "opexp2.txt";
    String query = "SELECT OperatorName, COUNT(HashTag)  FROM  ( SELECT DISTINCT OperatorName, HashTag  FROM " + input.getName() + ",  ( SELECT HashTag AS RepHashTag  FROM " + input.getName() + " GROUP BY HashTag  HAVING COUNT(HashTag) > 1 ) AS RepTags  WHERE HashTag = RepTags.RepHashTag ) AS A  GROUP BY OperatorName ";
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_opexp2");
    output.persist();
    output.writeToFile(fileName);
    writeJoinAnalysis(input);
  }
  
  public void writeJoinAnalysis(DataFrameIR input) {
    String fileName = "join.txt";
    String query = "SELECT HFreq, COUNT(*) FROM (SELECT HashTag, COUNT(*) AS HFreq FROM (SELECT temp.VS_Frequency, v.*\nFROM\n( SELECT HashTag, COUNT(*) AS VS_Frequency\nFROM " + input.getName() + "\nGROUP BY HashTag \nHAVING VS_Frequency > 1\n) AS temp\nINNER JOIN\n" + input.getName() + " AS v\nON v.HashTag = temp.HashTag\nWHERE v.OperatorName = 'Join') AS T\nGROUP BY HashTag) AS T2 GROUP BY HFreq ORDER BY HFreq";
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_join");
    output.persist();
    output.writeToFile(fileName);
    output.show();
  }
  
  public void writeCandidateViews(DataFrameIR input) {
    String fileName = "candidate.txt";
    String query = "SELECT JobGUID, HashTag, HashTagNonStrict, OperatorName, Parameters FROM " + input.getName();
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_candidate");
    output.persist();
    output.writeToFile(fileName);
  }
  
  public void writeSelectedViews(DataFrameIR input) {
    String fileName = "select.txt";
    String query = " SELECT JobGUID, HashTag, HashTagNonStrict, Frequency, OperatorName, TreeLevel, Parameters FROM (SELECT JobGUID, HashTag, HashTagNonStrict, VS_Frequency AS Frequency, OperatorName, TreeLevel, Parameters, RANK() OVER(PARTITION BY HashTag ORDER BY PrimaryKey) AS S_Rank FROM " + input.getName() + " ) AS tmp WHERE S_Rank <= 1";
    DataFrameIR output = new DataFrameIR(input, query, input.getName() + "_candidate");
    output.persist();
    output.writeToFile(fileName);
  }
}
