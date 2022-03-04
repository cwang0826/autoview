package com.huawei.cloudviews.core.planir.preprocess.data.formats;

public interface IFormat {
  String getName();
  
  void writeAll(Object paramObject);
  
  void showAll();
  
  Object query(String paramString);
  
  void cleanUp();
}
