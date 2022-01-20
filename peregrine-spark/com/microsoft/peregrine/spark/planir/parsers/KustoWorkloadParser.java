package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.planir.parsers.FileParser;
import com.microsoft.peregrine.core.planir.parsers.entities.Application;
import com.microsoft.peregrine.core.planir.parsers.entities.Metadata;
import com.microsoft.peregrine.core.planir.parsers.entities.Metric;
import com.microsoft.peregrine.core.planir.parsers.entities.Query;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KustoWorkloadParser extends FileParser<Workload> {
  SparkQueryParser queryParser = new SparkQueryParser();
  
  public Workload parseReader(BufferedReader reader) {
    return new Workload(new ApplicationIterator(reader));
  }
  
  class ApplicationIterator implements Iterator<Application> {
    Iterator<CSVRecord> iterator;
    
    ApplicationIterator(BufferedReader reader) {
      try {
        this.iterator = (new CSVParser(reader, CSVFormat.newFormat(',').withQuote('"').withFirstRecordAsHeader())).iterator();
      } catch (IOException e) {
        e.printStackTrace();
      } 
    }
    
    public boolean hasNext() {
      if (this.iterator.hasNext())
        return true; 
      System.out.println(KustoWorkloadParser.this.queryParser.getQueryMetrics());
      return false;
    }
    
    public Application next() {
      CSVRecord r = this.iterator.next();
      Application application = new Application();
      Query q = KustoWorkloadParser.this.queryParser.parsePlan(r.get("PhysicalPlanDescription"));
      application.addQuery(q);
      try {
        if (q.PhysicalPlan != null && !r.get("SparkPlanInfo").equalsIgnoreCase("null"))
          KustoWorkloadParser.this.queryParser.decorateQueryWithMetrics(q, (JSONObject)(new JSONParser()).parse(r.get("SparkPlanInfo"))); 
      } catch (ParseException e) {
        System.out.println("Could not parse planinfo string: " + e.getMessage());
      } 
      application.addMetrics(
          getMetrics(r
            .get("list_MetricId"), r
            .get("list_MetricName"), r
            .get("list_Value")));
      application.metadata = new Metadata();
      return application;
    }
    
    private List<Metric> getMetrics(String metricIds, String metricNames, String metricValues) {
      List<Metric> metrics = new ArrayList<>();
      try {
        JSONArray ids = (JSONArray)(new JSONParser()).parse(metricIds);
        JSONArray names = (JSONArray)(new JSONParser()).parse(metricNames);
        JSONArray values = (JSONArray)(new JSONParser()).parse(metricValues);
        if (ids.size() != names.size() || names.size() != values.size())
          throw new RuntimeException("Metrics data mismatch"); 
        for (int i = 0; i < ids.size(); i++) {
          try {
            long value = Long.parseLong((String)values.get(i));
            metrics.add(new Metric(
                  Long.parseLong((String)ids.get(i)), null, (String)names
                  
                  .get(i), value));
          } catch (NumberFormatException numberFormatException) {}
        } 
        return metrics;
      } catch (ParseException e) {
        throw new RuntimeException("Error parsing the CSV: " + e.getMessage());
      } 
    }
  }
}
