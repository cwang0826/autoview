package com.microsoft.peregrine.spark.planir.parsers.metricflow;

import com.microsoft.peregrine.core.planir.parsers.entities.Metric;
import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.preprocess.entities.View;
import com.microsoft.peregrine.spark.planir.enumerators.SparkMetricAttributeAssociation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SerialTimeCalculator {
  public void propagate(List<Operator> postOrder) {
    SparkMetricAttributeAssociation metricAttributes = new SparkMetricAttributeAssociation();
    Set<String> exclusiveTimeMetrics = metricAttributes.getMetricNames(View.EXCLUSIVE_TIME);
    Set<String> propagatedExclusiveTimeMetrics = metricAttributes.getMetricNames(View.PROP_EXCLUSIVE_TIME);
    Set<String> allExclusiveTimeMetrics = new HashSet<>();
    allExclusiveTimeMetrics.addAll(exclusiveTimeMetrics);
    allExclusiveTimeMetrics.addAll(propagatedExclusiveTimeMetrics);
    String serialTimeMetricName = metricAttributes.getDefaultMetricName(View.PROP_SERIAL_TIME);
    for (int i = 0; i < postOrder.size(); i++) {
      Operator op = postOrder.get(i);
      long inclusiveTime = 0L;
      for (Operator child : op.getChildren()) {
        Metric inclusiveTimeMetric = getSerialTimeMetric(child, serialTimeMetricName);
        if (inclusiveTimeMetric != null)
          inclusiveTime += inclusiveTimeMetric.getValue(); 
      } 
      if (!op.name.contains("WholeStageCodegen")) {
        Metric exclusiveTimeMetric = getExclusiveTimeMetric(op, allExclusiveTimeMetrics);
        if (exclusiveTimeMetric != null)
          inclusiveTime += exclusiveTimeMetric.getValue(); 
      } 
      Metric newMetric = new Metric(-1L, serialTimeMetricName, null, inclusiveTime);
      op.addMetric(newMetric);
    } 
  }
  
  private Metric getSerialTimeMetric(Operator op, String serialTimeMetricName) {
    for (Metric m : op.getMetrics()) {
      if (m.getName().equalsIgnoreCase(serialTimeMetricName))
        return m; 
    } 
    return null;
  }
  
  public Metric getExclusiveTimeMetric(Operator op, Set<String> allExclusiveTimeMetrics) {
    for (Metric m : op.getMetrics()) {
      if (allExclusiveTimeMetrics.contains(m.getName()))
        return m; 
    } 
    return null;
  }
}
