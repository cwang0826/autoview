package com.huawei.cloudviews.spark.planir.parsers;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
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
      case "applog":
        return fetchAppLog();
    } 
    throw new RuntimeException("Unknown data fetchLogs source: " + source);
  }
  
  private Workload fetchAppLog() {
    String directory = this.conf.get("applog_directory");
    Path dirPath = Paths.get(directory, new String[0]);
    ApplicationLogWorkloadParser workloadParser = new ApplicationLogWorkloadParser();
    return workloadParser.parse(dirPath);
  }
}
