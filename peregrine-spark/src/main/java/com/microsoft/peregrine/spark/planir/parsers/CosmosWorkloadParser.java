package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.planir.parsers.FileParser;
import com.microsoft.peregrine.core.planir.parsers.entities.Application;
import com.microsoft.peregrine.core.planir.parsers.entities.Query;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import com.microsoft.peregrine.spark.planir.parsers.entities.SparkAccumulable;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosmosWorkloadParser extends FileParser<Workload> {
  private static final Logger logger = LoggerFactory.getLogger(CosmosWorkloadParser.class);
  
  private SparkQueryParser queryParser = new SparkQueryParser();
  
  private DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
  
  public Workload parse(Path filePath) {
    try {
      return parseReader(new BufferedReader(new FileReader(filePath.toString())));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } 
  }
  
  public Workload parseReader(BufferedReader reader) {
    return new Workload(new ApplicationIterator(reader));
  }
  
  class ApplicationIterator implements Iterator<Application> {
    Iterator<CSVRecord> iterator;
    
    ApplicationIterator(BufferedReader reader) {
      try {
        this.iterator = (new CSVParser(reader, CSVFormat.newFormat('|').withFirstRecordAsHeader())).iterator();
      } catch (IOException e) {
        e.printStackTrace();
      } 
    }
    
    public boolean hasNext() {
      if (this.iterator.hasNext())
        return true; 
      CosmosWorkloadParser.logger.info(CosmosWorkloadParser.this.queryParser.getQueryMetrics());
      return false;
    }
    
    public Application next() {
      CSVRecord r = this.iterator.next();
      Application application = new Application();
      try {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("physicalPlanDescription", r.get("PhysicalPlanDescription"));
        jsonObject.put("sparkPlanInfo", (new JSONParser()).parse(r.get("SparkPlanInfo")));
        jsonObject.put("executionId", Long.valueOf(Long.parseLong(r.get("QueryId"))));
        Query q = CosmosWorkloadParser.this.queryParser.parse(jsonObject);
        if (q != null)
          application.addQuery(q); 
        JSONObject metricsObject = (JSONObject)(new JSONParser()).parse(r.get("Metrics"));
        JSONArray jsonArray = (JSONArray)metricsObject.get("Metrics");
        if (jsonArray != null)
          for (Object aJson : jsonArray)
            application.addMetric(new SparkAccumulable((JSONObject)aJson));  
        application.metadata.AppID = r.get("ApplicationId");
        application.metadata.ClusterName = r.get("JobVirtualCluster");
        application.metadata.Subscription = r.get("UserSubscriptionId");
        try {
          application.metadata.AppSubmitTime = CosmosWorkloadParser.this.df.parse(r.get("SubmitTime")).getTime();
          application.metadata.AppStartTime = CosmosWorkloadParser.this.df.parse(r.get("StartTime")).getTime();
          application.metadata.AppEndTime = CosmosWorkloadParser.this.df.parse(r.get("EndTime")).getTime();
        } catch (ParseException parseException) {}
        return application;
      } catch (ParseException e) {
        throw new RuntimeException(e.getMessage());
      } 
    }
  }
}
