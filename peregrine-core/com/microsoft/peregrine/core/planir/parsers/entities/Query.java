package com.microsoft.peregrine.core.planir.parsers.entities;

public class Query {
  public Plan ParsedPlan;
  
  public Plan AnalyzedPlan;
  
  public Plan OptimizedPlan;
  
  public Plan PhysicalPlan;
  
  private long queryId;
  
  public void setQueryId(long queryId) {
    this.queryId = queryId;
  }
  
  public long getQueryId() {
    return this.queryId;
  }
}
