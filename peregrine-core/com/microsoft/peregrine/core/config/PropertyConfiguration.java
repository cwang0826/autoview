package com.microsoft.peregrine.core.config;

import com.microsoft.peregrine.core.config.legacy.Config;
import java.io.File;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class PropertyConfiguration implements Config {
  private Properties properties;
  
  private String propertyFile;
  
  public PropertyConfiguration(String propertyFile) {
    this(propertyFile, new HashSet<>());
  }
  
  public PropertyConfiguration(String propertyFile, Set<String> pathFiles) {
    if (pathFiles.contains(propertyFile))
      throw new RuntimeException("Cyclic dependency detected for " + propertyFile); 
    pathFiles.add(propertyFile);
    this.propertyFile = propertyFile;
    this.properties = load(propertyFile, pathFiles);
  }
  
  public void print() {
    for (String key : this.properties.stringPropertyNames())
      System.out.println(key + ": " + this.properties.getProperty(key)); 
  }
  
  private Properties load(String baseFile, Set<String> pathFiles) {
    if (Files.notExists((new File(baseFile)).toPath(), new java.nio.file.LinkOption[0]))
      return new Properties(); 
    PropertyConfigurationHelper helper = new PropertyConfigurationHelper();
    Properties baseProperties = helper.getProperties(baseFile);
    Properties importedProperties = helper.getImportedProperties(baseFile, baseProperties, pathFiles);
    Properties mergedProperties = helper.merge(baseProperties, importedProperties);
    return mergedProperties;
  }
  
  public String getFileName() {
    return this.propertyFile;
  }
  
  public Properties getProperties() {
    return this.properties;
  }
  
  public String get(String key) {
    return this.properties.getProperty(key);
  }
  
  public String getOrDefault(String key, String defaultValue) {
    String value = this.properties.getProperty(key);
    return (value != null) ? value : defaultValue;
  }
  
  public boolean isEmpty() {
    return this.properties.isEmpty();
  }
}
