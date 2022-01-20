package com.microsoft.peregrine.spark.tasks;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import com.microsoft.peregrine.core.planir.preprocess.enumerators.IEnumerator;
import com.microsoft.peregrine.core.tasks.WorkloadParserTask;
import com.microsoft.peregrine.spark.planir.enumerators.SparkViewEnumerator;
import com.microsoft.peregrine.spark.planir.parsers.SparkWorkloadParser;

public class SparkWorkloadParserTask extends WorkloadParserTask {
  protected Workload getWorkload(Config conf) {
    return (new SparkWorkloadParser(conf)).fetchLogs();
  }
  
  protected IEnumerator getEnumerator(Workload workload) {
    return (IEnumerator)new SparkViewEnumerator(workload);
  }
}
