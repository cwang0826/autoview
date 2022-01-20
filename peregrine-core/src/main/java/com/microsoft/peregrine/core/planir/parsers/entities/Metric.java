package com.microsoft.peregrine.core.planir.parsers.entities;

import org.apache.log4j.Logger;

public class Metric {
  private long id;
  
  private String name;
  
  private String type;
  
  private long value;
  
  private double fraction = 1.0D;
  
  public static int totalMetricCount = 0;
  
  public Metric(long id, String name, String type, long value) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.value = value;
    totalMetricCount++;
  }
  
  public Metric(long id, String name, String type, long value, double fraction) {
    this(id, name, type, value);
    this.fraction = fraction;
  }
  
  public Metric(Metric other) {
    this(other.id, other.name, other.type, other.value, other.fraction);
  }
  
  public void increment() {
    this.value++;
  }
  
  public void update(long newValue) {
    this.value = newValue;
  }
  
  public void print(Logger logger) {
    logger.debug(this.name + ": " + this.type + " = " + this.value);
  }
  
  public String toString() {
    return "Metric:" + this.name + ":" + this.type + "=" + this.value;
  }
  
  public long getId() {
    return this.id;
  }
  
  public String getType() {
    return this.type;
  }
  
  public String getName() {
    return this.name;
  }
  
  public long getValue() {
    long valueContribution = (long)(this.fraction * this.value);
    return valueContribution;
  }
  
  public long getEntireValue() {
    return this.value;
  }
  
  public void setValue(long newValue) {
    this.value = newValue;
  }
}
