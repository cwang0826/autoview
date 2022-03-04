package com.huawei.cloudviews.spark.tasks;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.IEnumerator;
import com.huawei.cloudviews.core.tasks.WorkloadParserTask;
import com.huawei.cloudviews.spark.planir.enumerators.SparkViewEnumerator;
import com.huawei.cloudviews.spark.planir.parsers.SparkWorkloadParser;

public class SparkWorkloadParserTask extends WorkloadParserTask {
  protected Workload getWorkload(Config conf) {
    return (new SparkWorkloadParser(conf)).fetchLogs();
  }
  
  protected IEnumerator getEnumerator(Workload workload) {
    return (IEnumerator)new SparkViewEnumerator(workload);
  }
}
