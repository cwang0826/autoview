package com.microsoft.peregrine.core.planir.parsers.entities;

import java.util.ArrayList;
import java.util.List;

public abstract class Operator {
  public int indent;
  
  public int nodeId;
  
  public Operator parent;
  
  public List<Operator> children;
  
  public String name;
  
  public String opInfo;
  
  public List<String> additionalPreds;
  
  private List<Metric> metrics;
  
  private String _hts = null, _ht = null;
  
  private String linkedLogical = "";
  
  private long inputCardinality = -1L;
  
  private String inputDataset = "";
  
  private long avgRowLength = -1L;
  
  public Operator(String opInfo) {
    this.children = new ArrayList<>();
    this.metrics = new ArrayList<>();
    parseOpInfo(opInfo);
  }
  
  protected abstract void parseOpInfo(String paramString);
  
  public String HT() {
    if (this._ht == null)
      this._ht = ""; 
    return this._ht;
  }
  
  public String HTS() {
    if (this._hts == null)
      this._hts = ""; 
    return this._hts;
  }
  
  public String getLinkedLogicalName() {
    return this.linkedLogical;
  }
  
  public void setLogicalProperties(Operator from) {
    setHT(from.HT());
    setHTS(from.HTS());
    setLogicalName(from.name);
    setInputCardinality(from.inputCardinality);
    setInputDataset(from.inputDataset);
    setAvgRowLength(from.avgRowLength);
  }
  
  public void setHTS(String hts) {
    this._hts = hts;
  }
  
  public void setHT(String ht) {
    this._ht = ht;
  }
  
  public void setLogicalName(String logicalName) {
    this.linkedLogical = logicalName;
  }
  
  public void setInputCardinality(long inputCardinality) {
    this.inputCardinality = inputCardinality;
  }
  
  public void setInputDataset(String inputDataset) {
    this.inputDataset = inputDataset;
  }
  
  public void setAvgRowLength(long avgRowLength) {
    this.avgRowLength = avgRowLength;
  }
  
  public long getInputCardinality() {
    return this.inputCardinality;
  }
  
  public String getInputDataset() {
    return this.inputDataset;
  }
  
  public long getAvgRowLength() {
    return this.avgRowLength;
  }
  
  public void addChild(Operator child) {
    this.children.add(child);
    child.parent = this;
  }
  
  public List<Operator> getChildren() {
    return this.children;
  }
  
  public void addMetric(Metric metric) {
    this.metrics.add(metric);
  }
  
  public List<Metric> getMetrics() {
    return this.metrics;
  }
  
  public void addAdditionalPredicates(String predicate) {
    if (this.additionalPreds == null)
      this.additionalPreds = new ArrayList<>(); 
    this.additionalPreds.add(predicate);
  }
  
  public void setIdentifier(int nodeId) {
    this.nodeId = nodeId;
  }
  
  public static class RootOp extends Operator {
    public RootOp() {
      super("Root ");
    }
    
    protected void parseOpInfo(String opInfo) {
      this.name = opInfo;
    }
  }
  
  public String toString() {
    return this.name;
  }
  
  public String getMetricsString() {
    String metricStr = "";
    for (Metric m : this.metrics)
      metricStr = metricStr + m; 
    return (metricStr.length() > 0) ? metricStr : "NA";
  }
  
  public void printPreorder(int level) {
    for (int i = 0; i < level; i++)
      System.out.print(" "); 
    System.out.print(level + ". " + this + " (" + HTS() + ", " + getMetricsString() + ")");
    System.out.print("[");
    for (Operator child : this.children) {
      System.out.print(child + " (" + child.HTS() + ")");
      System.out.print(",");
    } 
    System.out.println("]");
    level++;
    for (Operator child : this.children)
      child.printPreorder(level); 
  }
  
  public List<Operator> postOrderTraversal() {
    return postOrderTraversal(new ArrayList<>());
  }
  
  public List<Operator> postOrderTraversal(List<Operator> postOrder) {
    List<Operator> children = this.children;
    Operator left = null, right = null;
    if (children.size() > 0) {
      left = children.get(0);
      left.postOrderTraversal(postOrder);
    } 
    if (children.size() > 1) {
      right = children.get(1);
      right.postOrderTraversal(postOrder);
    } 
    postOrder.add(this);
    return postOrder;
  }
}
