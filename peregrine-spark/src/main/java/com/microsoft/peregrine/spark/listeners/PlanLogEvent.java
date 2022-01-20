package com.microsoft.peregrine.spark.listeners;

import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

class PlanLogEvent extends SparkListenerSQLExecutionStart {
  public PlanLogEvent(long executionId, String description, String details, String physicalPlanDescription, SparkPlanInfo sparkPlanInfo, long time) {
    super(executionId, description, details, physicalPlanDescription, sparkPlanInfo, time);
  }
}
