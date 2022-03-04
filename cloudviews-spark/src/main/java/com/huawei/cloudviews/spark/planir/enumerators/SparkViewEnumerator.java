package com.huawei.cloudviews.spark.planir.enumerators;

import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.ViewEnumerator;
import com.huawei.cloudviews.spark.planir.parsers.metricflow.RowCountFlow;
import com.huawei.cloudviews.spark.planir.parsers.metricflow.SerialTimeCalculator;
import com.huawei.cloudviews.spark.planir.parsers.metricflow.StageTimeDistributor;

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
