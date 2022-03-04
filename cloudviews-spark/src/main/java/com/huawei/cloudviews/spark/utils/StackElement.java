package com.huawei.cloudviews.spark.utils;

import com.huawei.cloudviews.spark.planir.parsers.entities.SparkOpJson;

class StackElement {
  private SparkOpJson operator;
  
  private int numChildren;
  
  private int assignedChildren;
  
  public StackElement(SparkOpJson operator) {
    this.operator = operator;
    this.numChildren = operator.numChildren;
    this.assignedChildren = 0;
  }
  
  public boolean hasUnassignedChildren() {
    return (this.numChildren - this.assignedChildren > 0);
  }
  
  public void incrementAssignment() {
    this.assignedChildren++;
  }
  
  public SparkOpJson getOperator() {
    return this.operator;
  }
}
