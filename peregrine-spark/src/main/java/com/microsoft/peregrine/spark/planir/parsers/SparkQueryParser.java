package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.planir.parsers.PlanParser;
import com.microsoft.peregrine.core.planir.parsers.QueryParser;
import com.microsoft.peregrine.core.planir.parsers.entities.Metric;
import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.parsers.entities.Plan;
import com.microsoft.peregrine.core.planir.parsers.entities.Query;
import com.microsoft.peregrine.spark.planir.parsers.entities.SparkMetric;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

public class SparkQueryParser extends QueryParser<JSONObject> {
  private static final Logger logger = LoggerFactory.getLogger(SparkQueryParser.class);
  
  public Query parse(JSONObject jsonObject) {
    String planString = (String)jsonObject.get("physicalPlanDescription");
    if (planString == null || planString.length() <= 0)
      return null; 
    long negativePlanLogId = ((Long)jsonObject.get("executionId")).longValue();
    long queryLogId = (negativePlanLogId < 0L) ? Math.max(0L, -(negativePlanLogId + 1L)) : negativePlanLogId;
    try {
      Query query = parsePlan(planString);
      query.setQueryId(queryLogId);
      List<LogicalNodeAnnotations> annotations = getAnnotations(planString);
      if (annotations != null)
        decorateQueryWithAnnotations(query.OptimizedPlan, annotations); 
      if (query.PhysicalPlan != null)
        decorateQueryWithMetrics(query, (JSONObject)jsonObject.get("sparkPlanInfo")); 
      this.decoratedQueries.increment();
      return query;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } 
  }
  
  private void decorateQueryWithAnnotations(Plan plan, List<LogicalNodeAnnotations> annotations) {
    List<Operator> postorder = plan.tree.postOrderTraversal();
    int index = preorder(plan.tree, annotations, 0);
    int numLinkedSignatures = index + 1;
    if (annotations.size() != numLinkedSignatures) {
      String errMsg = " Error in linking annotations with operators. # Annotations: " + annotations.size() + " and # Logical Operators " + plan.tree.postOrderTraversal().size();
      System.out.println(errMsg);
      throw new RuntimeException("Error in linking annotations and logical nodes");
    } 
  }
  
  private int preorder(Operator tree, List<LogicalNodeAnnotations> annotations, int index) {
    setAnnotation(tree, annotations.get(index));
    List<Operator> children = tree.children;
    for (Operator child : children) {
      index++;
      index = preorder(child, annotations, index);
    } 
    return index;
  }
  
  private void setAnnotation(Operator operator, LogicalNodeAnnotations annotation) {
    operator.setInputCardinality(annotation.getInputCardinality());
    operator.setAvgRowLength(annotation.getAvgRowLength());
    operator.setInputDataset(annotation.getInputDataset());
    String signature = annotation.getSignature();
    setSignature(operator, signature);
  }
  
  private void setSignature(Operator operator, String signature) {
    String hts = extractPattern(signature, HTS_PATTERN);
    operator.setHTS(hts);
    String ht = extractPattern(signature, HT_PATTERN);
    operator.setHT(ht);
  }
  
  private static Pattern HTS_PATTERN = Pattern.compile(".*HTS:(\\d+).*");
  
  private static Pattern HT_PATTERN = Pattern.compile(".*HT:(\\d+).*");
  
  private String extractPattern(String hay, Pattern needle) {
    Matcher matcher = needle.matcher(hay);
    if (matcher.find())
      return matcher.group(1); 
    return null;
  }
  
  private List<LogicalNodeAnnotations> getAnnotations(String planString) {
    String[] tokens = planString.split("==");
    for (int i = 0; i < tokens.length; i++) {
      String jsonString;
      List<LogicalNodeAnnotations> annotations;
      switch (tokens[i].trim()) {
        case "Peregrine Signature":
          jsonString = tokens[++i].trim();
          annotations = extractAnnotationsFromJson(jsonString);
          return annotations;
      } 
    } 
    return null;
  }
  
  private List<LogicalNodeAnnotations> extractAnnotationsFromJson(String jsonString) {
    JSONParser parser = new JSONParser();
    JSONArray jsonArray = null;
    try {
      jsonArray = (JSONArray)parser.parse(jsonString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    } 
    List<LogicalNodeAnnotations> annotations = new ArrayList<>();
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = (JSONObject)jsonArray.get(i);
      LogicalNodeAnnotations logicalAnnotation = new LogicalNodeAnnotations(jsonObject);
      annotations.add(logicalAnnotation);
    } 
    return annotations;
  }
  
  public Query parsePlan(String planString) {
    this.inputQueries.increment();
    Query query = new Query();
    PlanParser parser = new SparkPlanParserJson();
    String[] tokens = planString.split("==");
    for (int i = 0; i < tokens.length; i++) {
      switch (tokens[i].trim()) {
        case "Parsed Logical Plan":
          query.ParsedPlan = parser.parse(tokens[++i].trim());
          break;
        case "Analyzed Logical Plan":
          query.AnalyzedPlan = parser.parse(tokens[++i].trim());
          break;
        case "Optimized Logical Plan":
          query.OptimizedPlan = parser.parse(tokens[++i].trim());
          break;
        case "Physical Plan":
          query.PhysicalPlan = parser.parse(tokens[++i].trim());
          break;
      } 
    } 
    if (query.OptimizedPlan != null && query.PhysicalPlan != null)
      this.parsedQueries.increment(); 
    return query;
  }
  
  public void decorateQueryWithMetrics(Query query, JSONObject jsonObject) {
    try {
      LinkMetricsRecursive(query.PhysicalPlan.tree, jsonObject);
      LinkLogicalPlanRecursive(query.OptimizedPlan.tree, query.PhysicalPlan.tree);
    } catch (Exception e) {
      logger.error("Error in linking metrics with plan", e);
      System.out.println("Error in linking metrics with plan: " + e.getMessage());
      throw new RuntimeException(e);
    } 
  }
  
  private void LinkMetricsRecursive(Operator op, JSONObject planInfo) {
    String planInfoNodeName = (String)planInfo.get("nodeName") + "Exec";
    if (op.name.equalsIgnoreCase(planInfoNodeName) || (op.opInfo != null && ((String)planInfo
      .get("simpleString")).contains(op.opInfo)) || (op.opInfo != null && op.opInfo
      .contains((String)planInfo.get("simpleString"))) || (op.opInfo != null && op.name
      .contains(planInfoNodeName)) || (op.opInfo != null && op.name
      .contains("Scan") && planInfoNodeName.contains("Scan")) || (op.name
      .contains("CommandExec") && planInfoNodeName.contains("Execute") && planInfoNodeName
      .contains("Command"))) {
      Iterator var4 = ((JSONArray)planInfo.get("metrics")).iterator();
      while(var4.hasNext()) {
	Object o = var4.next();
        Metric metric = new SparkMetric((JSONObject)o);
        op.addMetric(metric);
      } 
      JSONArray childPlanInfo = (JSONArray)planInfo.get("children");
      JSONArray copyChildPlanInfo = new JSONArray();
      for (ListIterator<JSONObject> it = childPlanInfo.listIterator(); it.hasNext(); ) {
        JSONObject obj = it.next();
        if (!obj.get("nodeName").toString().contains("Subquery"))
          copyChildPlanInfo.add(obj); 
      } 
      if (copyChildPlanInfo.size() < childPlanInfo.size())
        childPlanInfo = copyChildPlanInfo; 
      if (childPlanInfo.size() == op.children.size()) {
        for (int i = 0; i < childPlanInfo.size(); i++)
          LinkMetricsRecursive(op.children.get(i), (JSONObject)childPlanInfo.get(i)); 
      } else {
        if (childPlanInfo.size() == 0 || op.children.size() == 0)
          return; 
        throw new RuntimeException("Failed to link metrics: child count does not match!");
      } 
    } else if (planInfoNodeName.contains("WholeStageCodegen") || planInfoNodeName.contains("InputAdapter") || planInfoNodeName.contains("Subquery")) {
      LinkMetricsRecursive(op, (JSONObject)((JSONArray)planInfo.get("children")).get(0));
    } else {
      throw new RuntimeException("Failed to link metrics: node names do not match");
    } 
  }
  
  @Deprecated
  private void LinkLogicalPlanRecursive(Operator op1, Operator op2) {
    if (op2.name.contains(op1.name) || ((op1.name
      .contains("Relation") || op1.name.contains("RDD")) && op2.name.contains("Scan")) || (op1.name
      .contains("Scan") && op2.name.contains("Relation")) || (op1.name
      .contains("Repartition") && op2.name.contains("Coalesce")) || (op1.name
      .contains("Limit") && op2.name.contains("Limit")) || (op1.name
      .contains("RepartitionByExpression") && op2.name.contains("Exchange")) || (op1.name
      .contains("TypedFilter") && op2.name.contains("Filter")) || (op1.name
      .contains("Relation") && op2.name.contains("ReusedExchange")) || (op1.name
      .contains("InsertInto") && op2.name.contains("DataWriting")) || (op1.name
      .contains("CreateDataSource") && op2.name.contains("DataWriting")) || (op1.name
      .contains("CreateHiveTable") && op2.name.contains("DataWriting")) || (op1.name
      .contains("Join") && op2.name.contains("CartesianProductExec")) || (op1.name
      .contains("Command") && op2.name.contains("Command"))) {
      op2.setLogicalProperties(op1);
      if (op1.children.size() != op2.children.size()) {
        if (op1.children.size() == 1 && op2.name.contains("Sort") && op2.name.contains("Join")) {
          op1 = op1.children.get(0);
          LinkLogicalPlanRecursive(op1, op2);
        } else {
          while (op2.children.size() == 1 && (op2.name
            .contains("Sort") || op2.name.contains("Exchange")))
            op2 = op2.children.get(0); 
          if (op1.children.size() == op2.children.size()) {
            for (int i = 0; i < op1.children.size(); i++)
              LinkLogicalPlanRecursive(op1.children.get(i), op2.children.get(i)); 
          } else {
            if (op1.children.size() == 0 || op2.children.size() == 0)
              return; 
            throw new RuntimeException("Failed to link logical signatures: child count does not match!");
          } 
        } 
      } else {
        for (int i = 0; i < op1.children.size(); i++)
          LinkLogicalPlanRecursive(op1.children.get(i), op2.children.get(i)); 
      } 
    } else if (op2.name
      .contains("TakeOrderedAndProject") || op2.name
      .contains("WholeStageCodegen") || op2.name
      .contains("InputAdapter") || (op2.name
      .contains("Exchange") && !op2.name.equals("ReusedExchange")) || (op2.name
      .contains("Aggregate") && op2.opInfo != null && op2.opInfo.contains("partial")) || op2.name
      .contains("CreateViewCommand") || op2.name
      .contains("SaveIntoDataSourceCommand") || op2.name
      .contains("StateStoreSave") || op2.name
      .contains("StateStoreRestore") || op2.name
      .contains("CoalesceExec") || op2.name
      .contains("GlobalLimitExec") || op2.name
      .contains("LocalLimitExec") || (op1.name

      
      .contains("Join") && op2.name.contains("Aggregate"))) {
      if (op2.children.size() == 0)
        return; 
      LinkLogicalPlanRecursive(op1, op2.children.get(0));
    } else if (op1.name
      .contains("GlobalLimit") || op1.name
      .contains("LocalLimit") || op1.name
      .contains("Repartition") || op1.name
      .contains("ResolvedHint")) {
      LinkLogicalPlanRecursive(op1.children.get(0), op2);
    } else if (op1.name.contains("Project")) {
      LinkLogicalPlanRecursive(op1.children.get(0), op2);
    } else if (op2.name.contains("Project")) {
      LinkLogicalPlanRecursive(op1, op2.children.get(0));
    } else if (op1.name.contains("Sort")) {
      LinkLogicalPlanRecursive(op1.children.get(0), op2);
    } else if (op2.name.contains("Sort")) {
      LinkLogicalPlanRecursive(op1, op2.children.get(0));
    } else if (op1.name.contains("Union")) {
      LinkLogicalPlanRecursive(op1.children.get(0), op2);
    } else if (op2.name.contains("Union") || op2.name
      .contains("Aggregate") || op2.name
      .contains("InMemoryRelation") || op2.name
      .contains("BatchEvalPython")) {
      LinkLogicalPlanRecursive(op1, op2.children.get(0));
    } else if (op1.name.equals("Filter")) {
      LinkLogicalPlanRecursive(op1.children.get(0), op2);
    } else {
      throw new RuntimeException("Failed to link logical signatures: node names do not match!");
    } 
  }
}
