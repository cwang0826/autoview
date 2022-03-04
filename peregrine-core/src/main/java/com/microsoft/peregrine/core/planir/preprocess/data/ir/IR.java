package com.huawei.cloudviews.core.planir.preprocess.data.ir;

public interface IR {
  Object query(String paramString);
  
  void persist();
  
  String getName();
  
  long getRowCount();
  
  void summarize(String paramString);
  
  void show();
}
