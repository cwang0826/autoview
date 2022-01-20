package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.connectors.kusto.KustoCli;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SparkWorkloadParser {
  protected Config conf;
  
  public SparkWorkloadParser(Config conf) {
    this.conf = conf;
  }
  
  public Workload fetchLogs() {
    String source = this.conf.get("workload_source");
    switch (source) {
      case "kusto":
        return fetchKusto(10000);
      case "cosmos":
        return fetchCosmos();
      case "applog":
        return fetchAppLog();
    } 
    throw new RuntimeException("Unknown data fetchLogs source: " + source);
  }
  
  private Workload fetchKusto(int limit) {
    String query = "CSparkSqlExecutionEvents | where PreciseTimeStamp > ago(1d) | project ApplicationId,PreciseTimeStamp,StartTime,EndTime,PhysicalPlanDescription,SparkPlanInfo,Role,Tenant | where PhysicalPlanDescription!=\\\"\\\" | limit " + limit + " | join (CSparkStageTaskAccumulables | project MetricId,MetricName,Value,ApplicationId | summarize makelist(MetricId),makelist(MetricName),makelist(Value) by ApplicationId) on ApplicationId";
    String outputPath = this.conf.get("Kusto_outputFile");
    (new KustoCli()).run(this.conf
        .get("Kusto_datasource"), this.conf
        .get("Kusto_database"), query, outputPath);
    return (Workload)(new KustoWorkloadParser()).parse(Paths.get(outputPath, new String[0]));
  }
  
  private Workload fetchCosmos() {
    String cosmos_file = this.conf.get("applog_file");
    Path filepath = Paths.get(cosmos_file, new String[0]);
    CosmosWorkloadParser workloadParser = new CosmosWorkloadParser();
    return workloadParser.parse(filepath);
  }
  
  private Workload fetchAppLog() {
    String directory = this.conf.get("applog_directory");
    Path dirPath = Paths.get(directory, new String[0]);
    ApplicationLogWorkloadParser workloadParser = new ApplicationLogWorkloadParser();
    return workloadParser.parse(dirPath);
  }
}
