package com.huawei.cloudviews.spark.planir.parsers;

import com.huawei.cloudviews.core.planir.parsers.ApplicationParser;
import com.huawei.cloudviews.core.planir.parsers.entities.Application;
import com.huawei.cloudviews.core.planir.parsers.entities.Query;
import com.huawei.cloudviews.spark.planir.parsers.entities.SparkAccumulable;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationLogParser extends ApplicationParser {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationLogParser.class);
  
  protected Application application = new Application();
  
  protected SparkQueryParser queryParser = new SparkQueryParser();
  
  private String fileName = "";
  
  private static int failedQueries;
  
  public ApplicationLogParser(String fileName) {
    this();
    this.fileName = fileName;
  }
  
  public Application parseReader(BufferedReader reader) {
    Map<String, Integer> eventCounters = new HashMap<>();
    try {
      int lineCounter = 0;
      String currentLine;
      while ((currentLine = reader.readLine()) != null) {
        String identifier = this.fileName + ":" + lineCounter;
        String eventType = parseWorkloadLine(identifier, currentLine);
        eventCounters.merge(eventType, Integer.valueOf(1), (x, y) -> Integer.valueOf(x.intValue() + y.intValue()));
        lineCounter++;
      } 
      String applicationId = (this.application.getMetadata()).AppID;
      logger.info("Parsed application ID " + applicationId);
      logger.info("Metrics for application ID " + applicationId + this.queryParser.getQueryMetrics());
    } catch (Exception ex) {
      String applicationId = (this.application.getMetadata()).AppID;
      logger.error("Error in parsing application " + applicationId, ex);
      logger.error("Partial results for application " + applicationId + this.queryParser.getQueryMetrics());
    } 
    return this.application;
  }
  
  public String parseWorkloadLine(String identifier, String line) {
    try {
      long queryStartId, queryEndId;
      Query q;
      JSONArray jsonArray, accumUpdates;
      JSONObject json = (JSONObject)(new JSONParser()).parse(line);
      String eventType = (String)json.get("Event");
      switch (eventType) {
        case "SparkListenerApplicationStart":
          this.application.metadata.AppID = (String)json.get("App ID");
          this.application.metadata.AppName = (String)json.get("App Name");
          this.application.metadata.UserName = (String)json.get("User");
          this.application.metadata.AppStartTime = ((Long)json.get("Timestamp")).longValue();
          break;
        case "SparkListenerApplicationEnd":
          this.application.metadata.AppEndTime = ((Long)json.get("Timestamp")).longValue();
          break;
        case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
          queryStartId = ((Long)json.get("executionId")).longValue();
          (this.application.metadata.getQueryMetadata(queryStartId)).QueryStartTime = ((Long)json.get("time")).longValue();
          break;
        case "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
          queryEndId = ((Long)json.get("executionId")).longValue();
          (this.application.metadata.getQueryMetadata(queryEndId)).QueryEndTime = ((Long)json.get("time")).longValue();
          break;
        case "com.huawei.cloudviews.spark.listeners.PlanLogEvent":
        case "com.huawei.cloudviews.spark.extensions.listeners.PlanLogEvent":
          q = this.queryParser.parse(json);
          if (q != null)
            this.application.addQuery(q); 
          break;
        case "SparkListenerStageCompleted":
          jsonArray = (JSONArray)((JSONObject)json.get("Stage Info")).get("Accumulables");
          if (jsonArray != null)
            for (Object aJson : jsonArray) {
              SparkAccumulable aMetric = new SparkAccumulable((JSONObject)aJson);
              this.application.addMetric(aMetric);
            }  
          break;
        case "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
          accumUpdates = (JSONArray)json.get("accumUpdates");
          if (accumUpdates != null)
            for (Object keyValPair : accumUpdates) {
              JSONArray keyValPairArray = (JSONArray)keyValPair;
              if (keyValPairArray.size() == 2) {
                long id = ((Long)keyValPairArray.get(0)).longValue();
                Object updateObject = keyValPairArray.get(1);
                long updateValue = (updateObject instanceof Long) ? ((Long)updateObject).longValue() : Long.parseLong((String)updateObject);
                SparkAccumulable aMetric = new SparkAccumulable(id, updateValue);
                this.application.addMetric(aMetric);
              } 
            }  
          break;
      } 
      return eventType;
    } catch (Exception e) {
      logger.error("Error in parsing: " + line.substring(0, Math.min(line.length(), 100)), e.getMessage());
      System.out.println("Error " + ++failedQueries + " " + this.fileName);
      return "";
    } 
  }
  
  public ApplicationLogParser() {}
}
