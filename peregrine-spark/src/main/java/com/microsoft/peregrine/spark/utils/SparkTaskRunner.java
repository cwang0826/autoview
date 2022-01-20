package com.microsoft.peregrine.spark.utils;

import com.microsoft.peregrine.core.tasks.TaskRunner;

public class SparkTaskRunner extends TaskRunner {
  protected String getFullyQualifiedTaskName(String taskName) {
    if (taskName.startsWith("Spark"))
      return "com.microsoft.peregrine.spark.tasks." + taskName; 
    return "com.microsoft.peregrine.core.tasks." + taskName;
  }
  
  public static void main(String[] args) {
    try {
      System.out.println((new SparkTaskRunner()).run(args));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
}
