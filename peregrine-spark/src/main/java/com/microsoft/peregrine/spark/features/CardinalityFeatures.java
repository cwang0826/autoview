package com.huawei.cloudviews.spark.features;

import com.huawei.cloudviews.core.features.IFeatures;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.mllib.linalg.Vector;

public class CardinalityFeatures implements IFeatures {
  private long inputCardinality = -1L;
  
  private String inputDataset = "";
  
  private long avgRowLength = -1L;
  
  private String appName = "";
  
  private List<Long> cardinalityList;
  
  private List<String> datasetList;
  
  public CardinalityFeatures() {
    this.cardinalityList = new ArrayList<>();
    this.datasetList = new ArrayList<>();
  }
  
  public CardinalityFeatures setAppName(String name) {
    this.appName = name;
    return this;
  }
  
  public CardinalityFeatures setAvgRowLength(long value) {
    this.avgRowLength = value;
    return this;
  }
  
  public void addInputCardinality(long value) {
    this.cardinalityList.add(Long.valueOf(value));
  }
  
  public void addInputDataset(String name) {
    this.datasetList.add(name);
  }
  
  public Vector getFeatureVector() {
    CardinalityVector vector = new CardinalityVector();
    vector.setAppName(this.appName);
    vector.setInputDataset(getInputDataset());
    double inputCard = getInputCardinality();
    vector.setInputCard(inputCard);
    vector.setAvgRowLength(getAvgRowLength());
    vector.setInputCardSquared((inputCard >= 0.0D) ? Math.pow(inputCard, 2.0D) : -1.0D);
    vector.setInputCardSqrt((inputCard >= 0.0D) ? Math.sqrt(inputCard) : -1.0D);
    vector.setInputCardLog((inputCard >= 0.0D) ? Math.log(inputCard) : -1.0D);
    return vector.getFeatureVector();
  }
  
  public long getInputCardinality() {
    if (this.inputCardinality <= 0L && this.cardinalityList.size() > 0)
      this.inputCardinality = ((Long)this.cardinalityList.stream().reduce(Long.valueOf(0L), (x, y) -> Long.valueOf(x.longValue() + y.longValue()))).longValue(); 
    return this.inputCardinality;
  }
  
  public String getInputDataset() {
    if (this.inputDataset == "")
      this.inputDataset = String.join(",", (Iterable)this.datasetList); 
    return this.inputDataset;
  }
  
  public long getAvgRowLength() {
    return this.avgRowLength;
  }
  
  public String toString() {
    StringBuilder out = new StringBuilder();
    out.append("JobName=" + this.appName);
    out.append("; InputCardinality=" + getInputCardinality());
    out.append("; InputDataset=" + getInputDataset());
    out.append("; AvgRowLength=" + getAvgRowLength());
    return out.toString();
  }
}
