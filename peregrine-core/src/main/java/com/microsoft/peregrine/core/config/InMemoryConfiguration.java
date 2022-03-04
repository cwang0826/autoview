package com.huawei.cloudviews.core.config;

import com.huawei.cloudviews.core.config.legacy.Config;
import java.util.HashMap;
import java.util.Map;

public class InMemoryConfiguration implements Config {
  Map<String, String> props = new HashMap<>();
  
  public InMemoryConfiguration(Map<String, String> argumentMap) {
    for (String key : argumentMap.keySet())
      this.props.put(key, argumentMap.get(key)); 
  }
  
  public String get(String key) {
    return this.props.get(key);
  }
  
  public String getOrDefault(String key, String defaultValue) {
    return (this.props.get(key) == null) ? this.props.get(key) : defaultValue;
  }
  
  public boolean isEmpty() {
    return this.props.isEmpty();
  }
}
