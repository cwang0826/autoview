package com.microsoft.peregrine.core.planir.parsers.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Application {
  List<Query> queries = new ArrayList<>();
  
  List<Metric> metrics = new ArrayList<>();
  
  Map<Long, Metric> metricsMap = new HashMap<>();
  
  public Metadata metadata = new Metadata();
  
  public void addQuery(Query query) {
    this.queries.add(query);
  }
  
  public List<Query> getQueries() {
    return this.queries;
  }
  
  public void addMetrics(List<Metric> metrics) {
    for (Metric m : metrics)
      addMetric(m); 
  }
  
  public void addMetric(Metric metric) {
    this.metrics.add(metric);
    this.metricsMap.put(Long.valueOf(metric.getId()), metric);
  }
  
  public Metric getMetric(int Id) {
    return this.metricsMap.containsKey(Integer.valueOf(Id)) ? this.metricsMap.get(Integer.valueOf(Id)) : null;
  }
  
  public Map<Long, Metric> getMetricMap() {
    return this.metricsMap;
  }
  
  public Metadata getMetadata() {
    return this.metadata;
  }
}
