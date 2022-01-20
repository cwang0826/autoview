package com.microsoft.peregrine.spark.planir.parsers.entities;

import com.microsoft.peregrine.core.planir.parsers.entities.Metric;
import org.json.simple.JSONObject;

public class SparkAccumulable extends Metric {
  public SparkAccumulable(JSONObject jsonMetric) {
    super(
        (jsonMetric.get("ID") instanceof Long) ? ((Long)jsonMetric.get("ID")).longValue() : Long.parseLong((String)jsonMetric.get("ID")), (String)jsonMetric
        .get("Name"), null, 
        
        (jsonMetric.get("Value") instanceof Long) ? ((Long)jsonMetric.get("Value")).longValue() : Long.parseLong((String)jsonMetric.get("Value")));
  }
  
  public SparkAccumulable(long id, long value) {
    super(id, "", null, value);
  }
}
