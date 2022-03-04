package com.huawei.cloudviews.spark.utils;

import com.huawei.cloudviews.core.tasks.TaskRunner;

public class SparkTaskRunner extends TaskRunner {
  protected String getFullyQualifiedTaskName(String taskName) {
    if (taskName.startsWith("Spark"))
      return "com.huawei.cloudviews.spark.tasks." + taskName; 
    return "com.huawei.cloudviews.core.tasks." + taskName;
  }
  
  public static void main(String[] args) {
    try {
      System.out.println((new SparkTaskRunner()).run(args));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
}
