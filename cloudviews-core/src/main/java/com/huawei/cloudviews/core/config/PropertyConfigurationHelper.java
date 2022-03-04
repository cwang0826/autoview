package com.huawei.cloudviews.core.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyConfigurationHelper {
  public Properties getProperties(String propertyFileName) {
    Properties props = new Properties();
    try (InputStream inputStream = new FileInputStream(propertyFileName)) {
      props.load(inputStream);
    } catch (FileNotFoundException e) {
      return props;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
    return props;
  }
  
  public Properties getImportedProperties(String baseFile, Properties baseProperties, Set<String> pathFiles) {
    Properties workingSet = new Properties();
    List<String> importFileNames = getImportFileNames(baseFile);
    for (String fileName : importFileNames) {
      String newFileName = substituteBaseDir(fileName, baseProperties);
      Properties current = (new PropertyConfiguration(newFileName, pathFiles)).getProperties();
      pathFiles.remove(newFileName);
      workingSet = merge(workingSet, current, baseProperties);
    } 
    return workingSet;
  }
  
  private String substituteBaseDir(String varString, Properties baseProperties) {
    Pattern varRegex = Pattern.compile("\\$\\{(.+?)\\}");
    Matcher matcher = varRegex.matcher(varString);
    String newFileName = varString;
    while (matcher.find()) {
      String key = matcher.group(1);
      String value = baseProperties.getProperty(key);
      newFileName = newFileName.replace("${" + key + "}", value);
    } 
    return newFileName;
  }
  
  private Properties merge(Properties workingSet, Properties current, Properties baseProperties) {
    for (String key : current.stringPropertyNames()) {
      if (workingSet.getProperty(key) == null) {
        workingSet.setProperty(key, current.getProperty(key));
        continue;
      } 
      if (baseProperties.getProperty(key) == null) {
        String workingSetValue = workingSet.getProperty(key);
        String currentValue = current.getProperty(key);
        if (!workingSetValue.equalsIgnoreCase(currentValue))
          throw new RuntimeException("Property '" + key + "' exists in multiple property files. Please override this property in main configuration file."); 
      } 
    } 
    return workingSet;
  }
  
  public Properties merge(Properties baseProperties, Properties importedProperties) {
    Properties merged = new Properties();
    for (String key : importedProperties.stringPropertyNames())
      merged.setProperty(key, importedProperties.getProperty(key)); 
    for (String key : baseProperties.stringPropertyNames())
      merged.setProperty(key, baseProperties.getProperty(key)); 
    return merged;
  }
  
  private List<String> getImportFileNames(String propertyFile) {
    List<String> fileNames = new ArrayList<>();
    try(FileReader fileReader = new FileReader(new File(propertyFile)); 
        BufferedReader br = new BufferedReader(fileReader)) {
      String line = null;
      while ((line = br.readLine()) != null) {
        String fileName = getFileName(line);
        if (fileName != null)
          fileNames.add(fileName); 
      } 
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
    return fileNames;
  }
  
  private String getFileName(String line) {
    Pattern fileNameRegex = Pattern.compile("[#]\\s*import\\s+(.*)$");
    Matcher matcher = fileNameRegex.matcher(line);
    if (matcher.find())
      return matcher.group(1); 
    return null;
  }
}
