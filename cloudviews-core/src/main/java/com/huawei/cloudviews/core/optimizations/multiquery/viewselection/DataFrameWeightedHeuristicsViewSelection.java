package com.huawei.cloudviews.core.optimizations.multiquery.viewselection;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.DataFrameIR;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.IR;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFrameWeightedHeuristicsViewSelection extends HeuristicsViewSelection<DataFrameIR> {
  public DataFrameWeightedHeuristicsViewSelection(Config conf) {
    super(conf);
  }
  
  public DataFrameIR addPrimaryKeys(DataFrameIR inputViews) {
    String query = "SELECT ROW_NUMBER() OVER(ORDER BY AppQueryID) AS PrimaryKey, * \n   FROM " + inputViews.getName();
    DataFrameIR viewIR = new DataFrameIR(inputViews, query, inputViews.getName() + "_uniqueids");
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  public DataFrameIR filterSpecificOperators(DataFrameIR inputViews) {
    int estimatedRowLength = 100;
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT *    FROM " + inputViews.getName() + "\n   WHERE (LogicalName LIKE 'Aggregate'\n   OR LogicalName LIKE 'Filter'\n   OR LogicalName LIKE 'GlobalLimit'\n   OR LogicalName LIKE 'Join'\n   OR LogicalName LIKE 'Project'\n   OR LogicalName LIKE 'Sort'\n   OR LogicalName LIKE 'Union') \n   AND ChildCount > 0\n   AND (cast(PRowCount AS BIGINT)*" + estimatedRowLength + "/cast(cast(1024.0 AS BIGINT)*cast(1024.0 AS BIGINT) AS BIGINT)) < " + this.maxSizeInMb + "\n   AND NOT (LogicalName LIKE 'Project' AND Parameters LIKE '%projectList%[],%')", inputViews.getName() + "_initial_filter");
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  public DataFrameIR weightedOperators(DataFrameIR inputViews) {
    String query = "SELECT *,    IF (LogicalName LIKE 'Join' OR LogicalName LIKE 'Sort' OR LogicalName LIKE 'Aggregate', " + this.extraWeight + ", 1) AS VS_Weight   FROM " + inputViews.getName();
    DataFrameIR viewIR = new DataFrameIR(inputViews, query, inputViews.getName() + "_weighted");
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  public DataFrameIR repeatedViews(DataFrameIR inputViews) {
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT v.*, temp.VS_Frequency     FROM \n    (\n    SELECT StrictSignature, COUNT(*) AS VS_Frequency\n    FROM " + inputViews.getName() + "\n    GROUP BY StrictSignature \n    HAVING VS_Frequency >= " + this.minRepeats + "\n    ) AS temp\n    INNER JOIN \n    " + inputViews.getName() + "  AS v\n    ON v.StrictSignature = temp.StrictSignature", inputViews.getName() + "_repeated");
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  public DataFrameIR filteredViews(DataFrameIR inputViews) {
    return inputViews;
  }
  
  public DataFrameIR perJobUniqueViews(DataFrameIR inputViews) {
    String query = "SELECT *\n   FROM (\n   SELECT *, RANK() OVER(PARTITION BY AppQueryID, StrictSignature ORDER BY VS_Weight DESC, PrimaryKey) AS VS_JobRank \n   FROM " + inputViews.getName() + ") AS tmp\n   WHERE VS_JobRank <= 1";
    DataFrameIR viewIR = new DataFrameIR(inputViews, query, inputViews.getName() + "_uniquePerJob");
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  public DataFrameIR scheduleAwareViews(DataFrameIR inputViews) {
    return inputViews;
  }
  
  public DataFrameIR topkViews(DataFrameIR inputViews) {
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT v.*, VS_Score     FROM \n    (\n    SELECT StrictSignature, count(*) * 1.0 * AVG(VS_Weight) / AVG(TreeLevel) AS VS_Score\n    FROM " + inputViews.getName() + "\n    GROUP BY StrictSignature \n    ORDER BY VS_Score DESC\n    LIMIT " + this.topK + "\n    ) AS temp\n    inner join \n    " + inputViews.getName() + "  as v\n    on v.StrictSignature = temp.StrictSignature", inputViews.getName() + "_top" + this.topK);
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR perQueryTopkViews(DataFrameIR inputViews) {
    String queryString = "SELECT *\n   FROM (\n   SELECT *, RANK() OVER(PARTITION BY AppQueryID ORDER BY VS_Score DESC, PrimaryKey) AS VS_QueryRank \n   FROM " + inputViews.getName() + ") AS tmp\n   WHERE VS_QueryRank <= " + this.perQueryTopK;
    DataFrameIR viewIR = new DataFrameIR(inputViews, queryString, inputViews.getName() + "_perQueryTop" + this.perQueryTopK);
    viewIR.persist();
    viewIR.show();
    return viewIR;
  }
  
  protected List<Pair<String, String>> getSignatureProps(DataFrameIR annotationInput) {
    Dataset<Row> dataset = annotationInput.query("SELECT DISTINCT NonStrictSignature FROM " + annotationInput.getName());
    List<Pair<String, String>> annotationProps = new ArrayList<>();
    for (Row row : dataset.collectAsList())
      annotationProps.add(new ImmutablePair(row.get(0).toString(), null)); 
    return annotationProps;
  }
}
