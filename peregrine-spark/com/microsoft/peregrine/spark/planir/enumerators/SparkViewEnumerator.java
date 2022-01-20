package com.microsoft.peregrine.spark.planir.enumerators;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import com.microsoft.peregrine.core.planir.preprocess.enumerators.ViewEnumerator;
import com.microsoft.peregrine.spark.planir.parsers.metricflow.RowCountFlow;
import com.microsoft.peregrine.spark.planir.parsers.metricflow.SerialTimeCalculator;
import com.microsoft.peregrine.spark.planir.parsers.metricflow.StageTimeDistributor;

public class SparkViewEnumerator extends ViewEnumerator {
  public SparkViewEnumerator(Workload workload) {
    super(workload, new SparkMetricAttributeAssociation());
  }
  
  public void fillMissing(Operator root) {
    RowCountFlow rcFlow = new RowCountFlow();
    rcFlow.propagate(root.postOrderTraversal());
    StageTimeDistributor timeDistributor = new StageTimeDistributor();
    timeDistributor.propagate(root.postOrderTraversal());
    SerialTimeCalculator it = new SerialTimeCalculator();
    it.propagate(root.postOrderTraversal());
  }
}
