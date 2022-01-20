package com.microsoft.peregrine.core.config.legacy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyConfig implements Config {
  protected static Map<String, Config> cache = new HashMap<>();
  
  private Properties props;
  
  public static Config getInstance(String propertyPackage) {
    if (!cache.containsKey(propertyPackage))
      cache.put(propertyPackage, new PropertyConfig(propertyPackage)); 
    return cache.get(propertyPackage);
  }
  
  private PropertyConfig(String propertyPackage) {
    this.props = new Properties();
    try {
      this.props.load(PropertyConfig.class.getClassLoader().getResourceAsStream(propertyPackage));
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize properties package: " + propertyPackage);
    } 
  }
  
  public String get(String key) {
    return this.props.getProperty(key);
  }
  
  public String getOrDefault(String key, String defaultValue) {
    String value = this.props.getProperty(key);
    return (value != null) ? value : defaultValue;
  }
  
  public boolean isEmpty() {
    return this.props.isEmpty();
  }
}
