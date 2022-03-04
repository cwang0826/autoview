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

public class DataFrameHeuristicsViewSelection extends HeuristicsViewSelection<DataFrameIR> {
  public DataFrameHeuristicsViewSelection(Config conf) {
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
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT *    FROM " + inputViews.getName() + "\n   WHERE OperatorName NOT LIKE 'LogicalRelation'\n   AND OperatorName NOT LIKE 'LocalRelation'\n   AND OperatorName NOT LIKE 'LocalLimit'\n   AND OperatorName NOT LIKE 'RowDataSourceScanExec'\n   AND OperatorName NOT LIKE 'HiveTableScanExec'\n   AND OperatorName NOT LIKE 'ExternalRDDScanExec'\n   AND OperatorName NOT LIKE 'InMemoryTableScanExec'\n   AND OperatorName NOT LIKE 'FileSourceScanExec'\n   AND OperatorName NOT LIKE 'LocalTableScanExec'\n   AND OperatorName NOT LIKE 'SerializeFromObjectExec'\n   AND OperatorName NOT LIKE 'MapElementsExec'\n   AND OperatorName NOT LIKE 'DeserializeToObjectExec'\n   AND OperatorName NOT LIKE 'BatchEvalPythonExec'\n   AND OperatorName NOT LIKE 'SubqueryAlias'\n   AND OperatorName NOT LIKE 'HiveTableRelation'\n   AND OperatorName NOT LIKE 'SetDatabaseCommand'\n   AND OperatorName NOT LIKE 'CreateViewCommand'\n   AND OperatorName NOT LIKE 'DataWritingCommandExec'\n   AND ChildCount > 0\n   AND NOT (OperatorName LIKE 'Project' AND Parameters LIKE '%projectList%[],%')", inputViews.getName() + "_initial_filter");
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR weightedOperators(DataFrameIR inputViews) {
    return inputViews;
  }
  
  public DataFrameIR repeatedViews(DataFrameIR inputViews) {
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT v.*, temp.VS_Frequency     FROM \n    (\n    SELECT StrictSignature, COUNT(*) AS VS_Frequency\n    FROM " + inputViews.getName() + "\n    GROUP BY StrictSignature \n    HAVING VS_Frequency > 1\n    ) AS temp\n    INNER JOIN \n    " + inputViews.getName() + "  AS v\n    ON v.StrictSignature = temp.StrictSignature", inputViews.getName() + "_repeated");
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR filteredViews(DataFrameIR inputViews) {
    return inputViews;
  }
  
  public DataFrameIR perJobUniqueViews(DataFrameIR inputViews) {
    String query = "SELECT *\n   FROM (\n   SELECT *, RANK() OVER(PARTITION BY AppQueryID, StrictSignature ORDER BY PSerialTime DESC, PrimaryKey) AS VS_JobRank \n   FROM " + inputViews.getName() + ") AS tmp\n   WHERE VS_JobRank <= 1";
    DataFrameIR viewIR = new DataFrameIR(inputViews, query, inputViews.getName() + "_uniquePerJob");
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR scheduleAwareViews(DataFrameIR inputViews) {
    return inputViews;
  }
  
  public DataFrameIR topkViews(DataFrameIR inputViews) {
    DataFrameIR viewIR = new DataFrameIR(inputViews, "SELECT v.*, VS_Score     FROM \n    (\n    SELECT StrictSignature, count(*) * if (AVG(PSerialTime) > 0, AVG(PSerialTime), 1) AS VS_Score\n    FROM " + inputViews.getName() + "\n    GROUP BY StrictSignature \n    ORDER BY VS_Score DESC\n    LIMIT " + this.topK + "\n    ) AS temp\n    inner join \n    " + inputViews.getName() + "  as v\n    on v.StrictSignature = temp.StrictSignature", inputViews.getName() + "_top" + this.topK);
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR perQueryTopkViews(DataFrameIR inputViews) {
    String queryString = "SELECT *\n   FROM (\n   SELECT *, RANK() OVER(PARTITION BY AppQueryID ORDER BY VS_Score DESC, PrimaryKey) AS VS_QueryRank \n   FROM " + inputViews.getName() + ") AS tmp\n   WHERE VS_QueryRank <= " + this.perQueryTopK;
    DataFrameIR viewIR = new DataFrameIR(inputViews, queryString, inputViews.getName() + "_perQueryTop" + this.perQueryTopK);
    viewIR.persist();
    return viewIR;
  }
  
  public DataFrameIR run(DataFrameIR dataFrameIR) {
    return super.run(dataFrameIR);
  }
  
  protected List<Pair<String, String>> getSignatureProps(DataFrameIR annotationInput) {
    Dataset<Row> dataset = annotationInput.query("SELECT DISTINCT NonStrictSignature FROM " + annotationInput.getName());
    List<Pair<String, String>> annotationProps = new ArrayList<>();
    for (Row row : dataset.collectAsList())
      annotationProps.add(new ImmutablePair(row.get(0).toString(), null)); 
    return annotationProps;
  }
}
