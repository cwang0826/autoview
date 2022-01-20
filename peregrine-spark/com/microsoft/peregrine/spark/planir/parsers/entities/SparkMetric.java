package com.microsoft.peregrine.spark.planir.parsers.entities;

import com.microsoft.peregrine.core.planir.parsers.entities.Metric;
import org.json.simple.JSONObject;

public class SparkMetric extends Metric {
  public SparkMetric(JSONObject jsonMetric) {
    super(((Long)jsonMetric
        .get("accumulatorId")).longValue(), (String)jsonMetric
        .get("name"), (String)jsonMetric
        .get("metricType"), 0L);
  }
}
