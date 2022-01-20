package com.microsoft.peregrine.spark.planir.enumerators;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import com.microsoft.peregrine.core.planir.preprocess.entities.View;
import com.microsoft.peregrine.core.planir.preprocess.enumerators.ViewEnumerator;
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
