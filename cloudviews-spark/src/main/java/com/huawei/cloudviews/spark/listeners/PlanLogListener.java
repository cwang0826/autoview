package com.huawei.cloudviews.spark.listeners;

import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.spark.extensions.rules.ComputationReuse;
import com.huawei.cloudviews.spark.features.CardinalityFeatures;
import com.huawei.cloudviews.spark.features.LogicalPlanFeaturize;
import com.huawei.cloudviews.spark.signature.LogicalPlanSignature;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

public class PlanLogListener implements QueryExecutionListener {
  private static final Logger logger = LoggerFactory.getLogger(PlanLogListener.class);
  
  private AtomicLong nextPlanLogId = new AtomicLong(-1L);
  
  private static boolean LEARN_CARD_ENABLED = false;
  
  public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
    try {
      long startTime = System.currentTimeMillis();
      long negativePlanLogId = this.nextPlanLogId.getAndDecrement();
      String planDescription = logPlans(qe);
      postPlans(planDescription, qe, startTime, negativePlanLogId);
    } catch (Error e) {
      logger.error("Unable to log JSON plan - " + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      logger.error("Unable to log JSON plan - " + e.getMessage());
      e.printStackTrace();
    } 
  }
  
  protected void postPlans(String planDescription, QueryExecution qe, long startTime, long planLogId) {
    SparkContext sc = qe.sparkSession().sparkContext();
    PlanLogEvent startEvent = new PlanLogEvent(planLogId, "CloudviewsPlanLogEvent", "Spark SQL Plans with annotations.", planDescription, SparkPlanInfo.fromSparkPlan(qe.executedPlan()), startTime);
    sc.listenerBus().post((SparkListenerEvent)startEvent);
    SparkListenerSQLExecutionEnd endEvent = new SparkListenerSQLExecutionEnd(planLogId, System.currentTimeMillis());
    sc.listenerBus().post((SparkListenerEvent)endEvent);
  }
  
  protected String logPlans(QueryExecution qe) {
    StringBuilder sb = new StringBuilder();
    logPreOptimization(sb, qe);
    logPostOptimization(sb, qe, true);
    String planDescription = sb.toString();
    return planDescription;
  }
  
  private void logPreOptimization(StringBuilder sb, QueryExecution qe) {
    try {
      sb.append("== Parsed Logical Plan ==");
      sb.append(qe.logical().prettyJson());
      sb.append("== Analyzed Logical Plan ==");
      sb.append(qe.analyzed().prettyJson());
    } catch (OutOfMemoryError e) {
      sb.delete(0, sb.length());
      sb.trimToSize();
      System.gc();
      logDebugInfo(sb, qe.analyzed(), "Analyzed");
    } 
  }
  
  private void logPostOptimization(StringBuilder sb, QueryExecution qe, boolean firstAttempt) {
    try {
      sb.append("== Optimized Logical Plan ==");
      sb.append(qe.optimizedPlan().prettyJson());
      sb.append("== Cloudviews Signature ==");
      List<String> signatures = getSignatures(qe.optimizedPlan());
      SparkContext context = qe.sparkSession().sparkContext();
      List<CardinalityFeatures> cardinalityFeatures = new ArrayList<>();
      if (LEARN_CARD_ENABLED)
        cardinalityFeatures = getCardinalityFeatures(qe.optimizedPlan(), context); 
      sb.append(getJsonAnnotations(signatures, cardinalityFeatures));
      sb.append("== Physical Plan ==");
      sb.append(qe.executedPlan().prettyJson());
    } catch (OutOfMemoryError e) {
      sb.delete(0, sb.length());
      sb.trimToSize();
      System.gc();
      if (firstAttempt) {
        logDebugInfo(sb, qe.analyzed(), "Analyzed");
        logPostOptimization(sb, qe, false);
      } else {
        logDebugInfo(sb, qe.optimizedPlan(), "Optimized");
      } 
    } 
  }
  
  private void logDebugInfo(StringBuilder sb, LogicalPlan lp, String planType) {
    int nodeCount = getNodeCount(lp);
    Runtime runtime = Runtime.getRuntime();
    int bytesInMb = 1048576;
    long maxMemMb = runtime.maxMemory() / bytesInMb;
    long totalMemMb = runtime.totalMemory() / bytesInMb;
    long freeMemMb = runtime.freeMemory() / bytesInMb;
    sb.append("== Failure Metrics ==");
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("Plan type", planType);
    jsonObject.put("Plan nodes", Integer.valueOf(nodeCount));
    jsonObject.put("Maximum memory mb", Long.valueOf(maxMemMb));
    jsonObject.put("Total memory mb", Long.valueOf(totalMemMb));
    jsonObject.put("Free memory mb", Long.valueOf(freeMemMb));
    sb.append(jsonObject.toJSONString());
  }
  
  private int getNodeCount(LogicalPlan lp) {
    AbstractFunction1<LogicalPlan, Integer> abstractFunction1 = new AbstractFunction1<LogicalPlan, Integer>() {
        public Integer apply(LogicalPlan node) {
          return Integer.valueOf(1);
        }
      };
    int nodeCount = lp.map((Function1)abstractFunction1).length();
    return nodeCount;
  }
  
  private List<String> getSignatures(LogicalPlan lp) {
    AbstractFunction1<LogicalPlan, String> abstractFunction1 = new AbstractFunction1<LogicalPlan, String>() {
        public String apply(LogicalPlan node) {
          if (node instanceof LogicalRDD) {
            LogicalRDD logicalRDD = (LogicalRDD)node;
            String rddName = logicalRDD.rdd().name();
            if (rddName.startsWith(ComputationReuse.reuseRddPrefix)) {
              int signStartIndex = rddName.indexOf(ComputationReuse.reuseRddPrefix) + ComputationReuse.reuseRddPrefix.length();
              return rddName.substring(signStartIndex);
            } 
          } 
          return LogicalPlanSignature.getSignatures(node);
        }
      };
    List<String> signatures = (List<String>)JavaConverters.seqAsJavaListConverter(lp.map((Function1)abstractFunction1)).asJava();
    return signatures;
  }
  
  private String getJsonAnnotations(List<String> signatures, List<CardinalityFeatures> cardinalityFeatures) {
    JSONArray array = new JSONArray();
    for (int i = 0; i < signatures.size(); i++) {
      JSONObject logicalObj = new JSONObject();
      logicalObj.put("signature", signatures.get(i));
      if (LEARN_CARD_ENABLED) {
        CardinalityFeatures features = cardinalityFeatures.get(i);
        logicalObj.put(View.INPUT_CARD, Long.valueOf(features.getInputCardinality()));
        logicalObj.put(View.AVG_ROW_LEN, Long.valueOf(features.getAvgRowLength()));
        logicalObj.put(View.INPUT_DATASET, features.getInputDataset());
      } 
      array.add(logicalObj);
    } 
    return array.toJSONString();
  }
  
  private List<CardinalityFeatures> getCardinalityFeatures(LogicalPlan lp, final SparkContext context) {
    AbstractFunction1<LogicalPlan, CardinalityFeatures> abstractFunction1 = new AbstractFunction1<LogicalPlan, CardinalityFeatures>() {
        public CardinalityFeatures apply(LogicalPlan node) {
          LogicalPlanFeaturize featurizer = new LogicalPlanFeaturize();
          return featurizer.featurize(node, context);
        }
      };
    List<CardinalityFeatures> features = (List<CardinalityFeatures>)JavaConverters.seqAsJavaListConverter(lp.map((Function1)abstractFunction1)).asJava();
    return features;
  }
  
  public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
}
