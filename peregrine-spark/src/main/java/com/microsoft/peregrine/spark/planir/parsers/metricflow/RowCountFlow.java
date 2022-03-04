package com.huawei.cloudviews.spark.planir.parsers.metricflow;

import com.huawei.cloudviews.core.planir.parsers.entities.Metric;
import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.spark.planir.enumerators.SparkMetricAttributeAssociation;
import java.util.List;
import java.util.Set;

public class RowCountFlow {
  public void propagate(List<Operator> postOrder) {
    SparkMetricAttributeAssociation metricAttributes = new SparkMetricAttributeAssociation();
    Set<String> rowCountMetrics = metricAttributes.getMetricNames(View.ROW_COUNT);
    String propagatedMetricName = metricAttributes.getDefaultMetricName(View.PROP_ROW_COUNT);
    Metric currentRowLength = null;
    for (int i = 0; i < postOrder.size(); i++) {
      boolean containsRowCount = false;
      Operator op = postOrder.get(i);
      List<Metric> metrics = op.getMetrics();
      for (Metric m : metrics) {
        String name = m.getName();
        if (rowCountMetrics.contains(name)) {
          currentRowLength = m;
          containsRowCount = true;
          break;
        } 
      } 
      if (!containsRowCount && currentRowLength != null) {
        Metric newMetric = new Metric(currentRowLength.getId(), propagatedMetricName, null, currentRowLength.getValue());
        op.addMetric(newMetric);
      } 
    } 
  }
}
