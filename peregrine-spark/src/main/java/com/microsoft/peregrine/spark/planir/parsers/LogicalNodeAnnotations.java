package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.planir.preprocess.entities.View;
import org.json.simple.JSONObject;

public class LogicalNodeAnnotations {
  private String signature;
  
  private long inputCardinality;
  
  private String inputDataset;
  
  private long avgRowLength;
  
  public LogicalNodeAnnotations(JSONObject jsonObject) {
    setSignature(getStringValue("signature", jsonObject));
    setInputCardinality(getLongValue(View.INPUT_CARD, jsonObject));
    setInputDataset(getStringValue(View.INPUT_DATASET, jsonObject));
    setAvgRowLength(getLongValue(View.AVG_ROW_LEN, jsonObject));
  }
  
  public String getStringValue(String key, JSONObject jsonObject) {
    if (jsonObject.containsKey(key))
      return (String)jsonObject.get(key); 
    return "";
  }
  
  public long getLongValue(String key, JSONObject jsonObject) {
    if (jsonObject.containsKey(key))
      return ((Long)jsonObject.get(key)).longValue(); 
    return -1L;
  }
  
  public String getSignature() {
    return this.signature;
  }
  
  public void setSignature(String signature) {
    this.signature = signature;
  }
  
  public long getInputCardinality() {
    return this.inputCardinality;
  }
  
  public void setInputCardinality(long inputCardinality) {
    this.inputCardinality = inputCardinality;
  }
  
  public String getInputDataset() {
    return this.inputDataset;
  }
  
  public void setInputDataset(String inputDataset) {
    this.inputDataset = inputDataset;
  }
  
  public long getAvgRowLength() {
    return this.avgRowLength;
  }
  
  public void setAvgRowLength(long avgRowLength) {
    this.avgRowLength = avgRowLength;
  }
}
