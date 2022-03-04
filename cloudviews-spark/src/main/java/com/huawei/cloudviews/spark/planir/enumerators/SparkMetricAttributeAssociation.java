package com.huawei.cloudviews.spark.planir.enumerators;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.MetricAttributeAssociation;
import java.util.HashSet;
import java.util.Set;

public class SparkMetricAttributeAssociation implements MetricAttributeAssociation {
  private static Multimap<String, String> attrToMetrics;
  
  private static Multimap<String, String> metricToAttrs;
  
  public SparkMetricAttributeAssociation() {
    if (attrToMetrics == null || metricToAttrs == null) {
      attrToMetrics = HashMultimap.create();
      metricToAttrs = HashMultimap.create();
      initializeMaps();
      addPropagatedMetrics();
    } 
  }
  
  public Set<String> getMetricNames(String attribute) {
    return new HashSet<>(attrToMetrics.get(attribute));
  }
  
  public Set<String> getAttributeNames(String metricName) {
    return new HashSet<>(metricToAttrs.get(metricName));
  }
  
  public String getDefaultMetricName(String attribute) {
    if (attribute.equals(View.PROP_ROW_COUNT))
      return "propagated row length"; 
    if (attribute.equals(View.PROP_EXCLUSIVE_TIME))
      return "calculated exclusive time"; 
    if (attribute.equals(View.PROP_SERIAL_TIME))
      return "calculated serial time"; 
    return null;
  }
  
  private void initializeMaps() {
    add(View.BYTES, "bytes of written output");
    add(View.BYTES, "data size total (min, med, max)");
    add(View.BYTES, "data size (bytes)");
    add(View.ROW_COUNT, "number of output rows");
    add(View.EXCLUSIVE_TIME, "duration total (min, med, max)");
    add(View.EXCLUSIVE_TIME, "aggregate time total (min, med, max)");
    add(View.EXCLUSIVE_TIME, "scan time total (min, med, max)");
    add(View.EXCLUSIVE_TIME, "sort time total (min, med, max)");
    add(View.MAX_MEMORY, "peak memory total (min, med, max)");
  }
  
  private void addPropagatedMetrics() {
    if (attrToMetrics.size() <= 0 || metricToAttrs.size() <= 0)
      throw new RuntimeException("Add actual metrics first."); 
    add(View.PROP_ROW_COUNT, "propagated row length");
    add(View.PROP_ROW_COUNT, getMetricNames(View.ROW_COUNT));
    add(View.PROP_EXCLUSIVE_TIME, "calculated exclusive time");
    add(View.PROP_EXCLUSIVE_TIME, getMetricNames(View.EXCLUSIVE_TIME));
    add(View.PROP_SERIAL_TIME, "calculated serial time");
  }
  
  private void add(String attribute, Set<String> metricNames) {
    for (String metricName : metricNames)
      add(attribute, metricName); 
  }
  
  private void add(String attribute, String metricName) {
    attrToMetrics.put(attribute, metricName);
    metricToAttrs.put(metricName, attribute);
  }
}
