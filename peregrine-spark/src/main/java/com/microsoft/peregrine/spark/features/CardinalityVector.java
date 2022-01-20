package com.microsoft.peregrine.spark.features;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class CardinalityVector {
  private double inputCard;
  
  private String inputDataset;
  
  private double avgRowLength;
  
  private String appName;
  
  private double inputCardSquared;
  
  private double inputCardSqrt;
  
  private double inputCardLog;
  
  public Vector getFeatureVector() {
    double[] numFeatures = getNumericalFeatures();
    Vector feature = Vectors.dense(numFeatures);
    return feature;
  }
  
  private double[] getNumericalFeatures() {
    List<Double> numFeatures = new ArrayList<>();
    numFeatures.add(Double.valueOf(this.inputCard));
    numFeatures.add(Double.valueOf(this.inputCardSquared));
    numFeatures.add(Double.valueOf(this.inputCardSqrt));
    numFeatures.add(Double.valueOf(this.inputCardLog));
    numFeatures.add(Double.valueOf(stringToDouble(this.inputDataset)));
    numFeatures.add(Double.valueOf(this.avgRowLength));
    numFeatures.add(Double.valueOf(stringToDouble(this.appName)));
    return numFeatures.stream().mapToDouble(Double::valueOf).toArray();
  }
  
  public double stringToDouble(String s) {
    return s.hashCode();
  }
  
  public CardinalityVector setInputCard(double inputCard) {
    this.inputCard = inputCard;
    return this;
  }
  
  public CardinalityVector setInputDataset(String inputDataset) {
    this.inputDataset = inputDataset;
    return this;
  }
  
  public CardinalityVector setAvgRowLength(double avgRowLength) {
    this.avgRowLength = avgRowLength;
    return this;
  }
  
  public CardinalityVector setAppName(String appName) {
    this.appName = appName;
    return this;
  }
  
  public CardinalityVector setInputCardSquared(double inputCardSquared) {
    this.inputCardSquared = inputCardSquared;
    return this;
  }
  
  public CardinalityVector setInputCardSqrt(double inputCardSqrt) {
    this.inputCardSqrt = inputCardSqrt;
    return this;
  }
  
  public CardinalityVector setInputCardLog(double inputCardLog) {
    this.inputCardLog = inputCardLog;
    return this;
  }
}
