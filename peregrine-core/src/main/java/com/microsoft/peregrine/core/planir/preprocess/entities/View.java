package com.huawei.cloudviews.core.planir.preprocess.entities;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class View {
  public static String APP_ID = "AppID";
  
  public static String APP_NAME = "AppName";
  
  public static String NORM_APP_NAME = "NormAppName";
  
  public static String USERNAME = "UserName";
  
  public static String CLUSTER_NAME = "ClusterName";
  
  public static String SUBSCRIPTION = "Subscription";
  
  public static String APP_SUBMIT_TIME = "AppSubmitTime";
  
  public static String APP_START_TIME = "AppStartTime";
  
  public static String APP_END_TIME = "AppEndTime";
  
  public static String APP_WALL_CLOCK_TIME = "AppWallClockTime";
  
  public static String QUERY_ID = "QueryID";
  
  public static String APP_QUERY_ID = "AppQueryID";
  
  public static String QUERY_START_TIME = "QueryStartTime";
  
  public static String QUERY_END_TIME = "QueryEndTime";
  
  public static String QUERY_WALL_CLOCK_TIME = "QueryWallClockTime";
  
  public static String OPERATOR_NAME = "OperatorName";
  
  public static String OPERATOR_ID = "OperatorID";
  
  public static String TREE_LEVEL = "TreeLevel";
  
  public static String PARENT_ID = "ParentID";
  
  public static String CHILD_COUNT = "ChildCount";
  
  public static String LOGICAL_NAME = "LogicalName";
  
  public static String STRICT_SIGNATURE = "StrictSignature";
  
  public static String NON_STRICT_SIGNATURE = "NonStrictSignature";
  
  public static String PROP_ROW_COUNT = "PRowCount";
  
  public static String PROP_EXCLUSIVE_TIME = "PExclusiveTime";
  
  public static String PROP_SERIAL_TIME = "PSerialTime";
  
  public static String PARAMETERS = "Parameters";
  
  public static String BYTES = "Bytes";
  
  public static String ROW_COUNT = "RowCount";
  
  public static String EXCLUSIVE_TIME = "ExclusiveTime";
  
  public static String MAX_MEMORY = "MaxMemory";
  
  public static String INPUT_CARD = "InputCard";
  
  public static String AVG_ROW_LEN = "AvgRowLength";
  
  public static String INPUT_DATASET = "InputDataset";
  
  public static String delimiter = "|";
  
  public static String defaultDateFormat = "MM/dd/yyyy HH:mm:ss a";
  
  public static DateFormat df = new SimpleDateFormat(defaultDateFormat);
  
  private Map<String, Object> attrMap = new HashMap<>();
  
  private static List<String> header;
  
  private static Set<String> longAttributes;
  
  private static Set<String> doubleAttributes;
  
  public View() {
    if (header == null)
      header = getHeader(); 
    if (longAttributes == null)
      longAttributes = getLongAttributes(); 
    if (doubleAttributes == null)
      doubleAttributes = getDoubleAttributes(); 
  }
  
  public void set(String key, Object value) {
    this.attrMap.put(key, value);
  }
  
  public long getAsLong(String key) {
    if (this.attrMap.containsKey(key))
      return ((Long)this.attrMap.get(key)).longValue(); 
    return 0L;
  }
  
  public String getAsString(String key) {
    if (this.attrMap.containsKey(key))
      return (String)this.attrMap.get(key); 
    return "";
  }
  
  public double getAsDouble(String key) {
    if (this.attrMap.containsKey(key))
      return ((Double)this.attrMap.get(key)).doubleValue(); 
    return 0.0D;
  }
  
  public boolean isLongAttribute(String key) {
    return longAttributes.contains(key);
  }
  
  public boolean isDoubleAttribute(String key) {
    return doubleAttributes.contains(key);
  }
  
  private Set<String> getLongAttributes() {
    Set<String> longValues = new HashSet<>();
    longValues.add(APP_SUBMIT_TIME);
    longValues.add(APP_START_TIME);
    longValues.add(APP_END_TIME);
    longValues.add(APP_WALL_CLOCK_TIME);
    longValues.add(QUERY_ID);
    longValues.add(QUERY_START_TIME);
    longValues.add(QUERY_END_TIME);
    longValues.add(QUERY_WALL_CLOCK_TIME);
    longValues.add(OPERATOR_ID);
    longValues.add(TREE_LEVEL);
    longValues.add(PARENT_ID);
    longValues.add(CHILD_COUNT);
    longValues.add(PROP_ROW_COUNT);
    longValues.add(PROP_EXCLUSIVE_TIME);
    longValues.add(PROP_SERIAL_TIME);
    longValues.add(BYTES);
    longValues.add(ROW_COUNT);
    longValues.add(EXCLUSIVE_TIME);
    longValues.add(MAX_MEMORY);
    longValues.add(INPUT_CARD);
    longValues.add(AVG_ROW_LEN);
    return longValues;
  }
  
  private Set<String> getDoubleAttributes() {
    Set<String> doubleAttributes = new HashSet<>();
    return doubleAttributes;
  }
  
  public List<String> getHeader() {
    List<String> lst = new ArrayList<>();
    lst.add(APP_ID);
    lst.add(APP_NAME);
    lst.add(NORM_APP_NAME);
    lst.add(USERNAME);
    lst.add(CLUSTER_NAME);
    lst.add(SUBSCRIPTION);
    lst.add(APP_SUBMIT_TIME);
    lst.add(APP_START_TIME);
    lst.add(APP_END_TIME);
    lst.add(APP_WALL_CLOCK_TIME);
    lst.add(QUERY_ID);
    lst.add(APP_QUERY_ID);
    lst.add(QUERY_START_TIME);
    lst.add(QUERY_END_TIME);
    lst.add(QUERY_WALL_CLOCK_TIME);
    lst.add(OPERATOR_NAME);
    lst.add(OPERATOR_ID);
    lst.add(TREE_LEVEL);
    lst.add(PARENT_ID);
    lst.add(CHILD_COUNT);
    lst.add(LOGICAL_NAME);
    lst.add(STRICT_SIGNATURE);
    lst.add(NON_STRICT_SIGNATURE);
    lst.add(PROP_ROW_COUNT);
    lst.add(PROP_EXCLUSIVE_TIME);
    lst.add(PROP_SERIAL_TIME);
    lst.add(PARAMETERS);
    lst.add(BYTES);
    lst.add(ROW_COUNT);
    lst.add(EXCLUSIVE_TIME);
    lst.add(MAX_MEMORY);
    lst.add(INPUT_CARD);
    lst.add(AVG_ROW_LEN);
    lst.add(INPUT_DATASET);
    return lst;
  }
  
  public String getHeaderString() {
    return String.join(delimiter, (Iterable)header);
  }
  
  public List<String> getRecord() {
    List<String> attributes = header;
    List<String> lst = new ArrayList<>();
    for (String attribute : attributes) {
      if (isLongAttribute(attribute)) {
        lst.add("" + getAsLong(attribute));
        continue;
      } 
      if (isDoubleAttribute(attribute)) {
        lst.add("" + getAsDouble(attribute));
        continue;
      } 
      lst.add(getAsString(attribute));
    } 
    return lst;
  }
  
  public String toRecordString() {
    return String.join(delimiter, (Iterable)getRecord());
  }
}
