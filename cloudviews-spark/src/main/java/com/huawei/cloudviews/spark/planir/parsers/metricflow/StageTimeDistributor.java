package com.huawei.cloudviews.spark.planir.parsers.metricflow;

import com.huawei.cloudviews.core.planir.parsers.entities.Metric;
import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.spark.planir.enumerators.SparkMetricAttributeAssociation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StageTimeDistributor {
  private static List<String> barriers;
  
  private static Set<String> exclusiveTimeMetrics;
  
  private static String propExclTimeMetricName;
  
  public StageTimeDistributor() {
    if (barriers == null)
      barriers = getBarrierList(); 
    SparkMetricAttributeAssociation metricAttributes = new SparkMetricAttributeAssociation();
    if (exclusiveTimeMetrics == null)
      exclusiveTimeMetrics = metricAttributes.getMetricNames(View.EXCLUSIVE_TIME); 
    if (propExclTimeMetricName == null)
      propExclTimeMetricName = metricAttributes.getDefaultMetricName(View.PROP_EXCLUSIVE_TIME); 
  }
  
  public void propagate(List<Operator> postOrder) {
    for (int i = 0; i < postOrder.size(); i++) {
      Operator op = postOrder.get(i);
      if (op.name.contains("WholeStageCodegen")) {
        Set<Operator> opsInStage = new HashSet<>();
        getStageOperators(op, opsInStage);
        opsInStage.remove(op);
        List<Operator> opsList = new ArrayList<>(opsInStage);
        List<Boolean> timeVector = hasTimeVector(opsList);
        Metric codegenTimeMetric = getTimeMetric(op);
        divideTime(opsList, timeVector, codegenTimeMetric);
      } 
    } 
  }
  
  public void divideTime(List<Operator> opsList, List<Boolean> timeVector, Metric codegenTimeMetric) {
    long totalTime = codegenTimeMetric.getValue();
    long accountedTime = 0L;
    int missingTimeOpCount = 0;
    for (int i = 0; i < opsList.size(); i++) {
      Operator op = opsList.get(i);
      boolean hasTime = ((Boolean)timeVector.get(i)).booleanValue();
      if (hasTime) {
        Metric metric = getTimeMetric(op);
        long time = metric.getValue();
        accountedTime += time;
      } else {
        missingTimeOpCount++;
      } 
    } 
    if (missingTimeOpCount <= 0)
      return; 
    long remainingTime = Math.max(0L, totalTime - accountedTime);
    double fraction = 1.0D / missingTimeOpCount;
    Metric m = new Metric(-1L, propExclTimeMetricName, null, remainingTime, fraction);
    for (int j = 0; j < opsList.size(); j++) {
      Operator operator = opsList.get(j);
      boolean hasTime = ((Boolean)timeVector.get(j)).booleanValue();
      if (!hasTime)
        operator.addMetric(m); 
    } 
  }
  
  public List<Boolean> hasTimeVector(List<Operator> operators) {
    List<Boolean> timeVector = new ArrayList<>();
    Set<String> timeMetrics = exclusiveTimeMetrics;
    for (Operator op : operators) {
      boolean hasTime = false;
      for (Metric m : op.getMetrics()) {
        if (timeMetrics.contains(m.getName())) {
          hasTime = true;
          break;
        } 
      } 
      timeVector.add(Boolean.valueOf(hasTime));
    } 
    return timeVector;
  }
  
  public void getStageOperators(Operator op, Set<Operator> discovered) {
    discovered.add(op);
    for (Operator child : op.getChildren()) {
      if (!discovered.contains(child) && 
        !endOfStage(child))
        getStageOperators(child, discovered); 
    } 
  }
  
  private boolean endOfStage(Operator op) {
    List<String> barrier = getBarrierList();
    for (String b : barrier) {
      if (op.name.contains(b))
        return true; 
    } 
    return false;
  }
  
  private Metric getTimeMetric(Operator op) {
    Set<String> timeMetrics = exclusiveTimeMetrics;
    for (Metric m : op.getMetrics()) {
      if (timeMetrics.contains(m.getName()))
        return m; 
    } 
    return null;
  }
  
  private List<String> getBarrierList() {
    List<String> barrier = new ArrayList<>();
    barrier.add("WholeStageCodegen");
    barrier.add("InputAdapter");
    barrier.add("Subquery");
    barrier.add("ReusedExchange");
    return barrier;
  }
}
