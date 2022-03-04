package com.huawei.cloudviews.spark.planir.enumerators;

import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.ViewEnumerator;
import java.util.Iterator;

public class SparkLogicalViewEnumerator extends ViewEnumerator {
  public SparkLogicalViewEnumerator(Workload workload) {
    super(workload, new SparkMetricAttributeAssociation());
  }
  
  protected void fillMissing(Operator root) {}
  
  public Iterator<View> enumerate() {
    return enumerateLogicalPlan();
  }
}
