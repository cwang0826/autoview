package com.huawei.cloudviews.core.config.legacy;

public interface Config {
  String get(String paramString);
  
  String getOrDefault(String paramString1, String paramString2);
  
  boolean isEmpty();
}
