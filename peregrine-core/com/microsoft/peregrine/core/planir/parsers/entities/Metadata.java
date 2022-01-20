package com.microsoft.peregrine.core.planir.parsers.entities;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;

public class Metadata {
  public String AppID;
  
  public String AppName;
  
  public String UserName;
  
  public String ClusterName;
  
  public String Subscription;
  
  public long AppSubmitTime;
  
  public long AppStartTime;
  
  public long AppEndTime;
  
  Map<Long, QueryMetadata> queryMetadataMap = new HashMap<>();
  
  public QueryMetadata getQueryMetadata(long queryID) {
    if (!this.queryMetadataMap.containsKey(Long.valueOf(queryID))) {
      QueryMetadata queryMetadata = new QueryMetadata();
      queryMetadata.QueryID = queryID;
      this.queryMetadataMap.put(Long.valueOf(queryID), queryMetadata);
    } 
    return this.queryMetadataMap.get(Long.valueOf(queryID));
  }
  
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
