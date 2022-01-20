package com.microsoft.peregrine.spark.extensions.rules;

import com.microsoft.peregrine.core.connectors.spark.SparkSQL;
import com.microsoft.peregrine.core.feedback.AbstractFeedback;
import com.microsoft.peregrine.core.feedback.FeedbackFile;
import com.microsoft.peregrine.core.feedback.IFeedback;
import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import com.microsoft.peregrine.spark.signature.LogicalPlanSignature;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.PartialFunction;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

public abstract class AbstractRule extends Rule<LogicalPlan> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractRule.class);
  
  private static boolean PROCESSING_RULE = false;
  
  protected SparkSQL sparkSQL;
  
  protected static Set<String> processedSignatures = new HashSet<>();
  
  private static String feedbackTypeKey = "peregrine.feedback.type";
  
  private static String feedbackParamsKey = "peregrine.feedback.params";
  
  protected static String feedbackLocationKey = "peregrine.feedback.location";
  
  private static String feedbackRefreshFlag = "peregrine.feedback.refresh";
  
  protected static String materializeFormatKey = "peregrine.materialize.format";
  
  protected static IFeedback feedbackLoader;
  
  public AbstractRule(SparkSession spark) {
    this.sparkSQL = new SparkSQL(spark);
    if (getFeedbackRefreshFlag(this.sparkSQL) || feedbackLoader == null) {
      String feedbackType = this.sparkSQL.getProperty(feedbackTypeKey);
      String feedbackParams = this.sparkSQL.getProperty(feedbackParamsKey);
      String feedbackLocation = this.sparkSQL.getProperty(feedbackLocationKey);
      if (feedbackType != null && feedbackParams != null) {
        feedbackLoader = AbstractFeedback.getInstance(feedbackType);
        feedbackLoader.initialize(FeedbackFile.getInputStream(feedbackParams, feedbackLocation));
      } 
    } 
  }
  
  public static boolean getFeedbackRefreshFlag(SparkSQL sparkSQL) {
    return (sparkSQL.getProperty(feedbackRefreshFlag) != null && sparkSQL
      .getProperty(feedbackRefreshFlag).equalsIgnoreCase("true"));
  }
  
  public static void setFeedbackType(SparkSQL sparkSQL, String feedbackType) {
    sparkSQL.setProperty(feedbackTypeKey, feedbackType);
  }
  
  public static void setFeedbackParams(SparkSQL sparkSQL, String feedbackParams) {
    sparkSQL.setProperty(feedbackParamsKey, feedbackParams);
  }
  
  public static void setFeedbackLocation(SparkSQL sparkSQL, String feedbackLocation) {
    sparkSQL.setProperty(feedbackLocationKey, feedbackLocation);
  }
  
  public static void setFeedbackRefreshFlag(SparkSQL sparkSQL, boolean flag) {
    sparkSQL.setProperty(feedbackRefreshFlag, "" + flag);
  }
  
  protected synchronized boolean signatureMatch(LogicalPlan p) {
    if (feedbackLoader == null)
      return false; 
    String h = LogicalPlanSignature.HT(p);
    List<Annotation> annotations = feedbackLoader.get(h);
    if (annotations == null)
      return false; 
    for (Annotation annotation : annotations) {
      if (isApplicable(annotation, p))
        return true; 
    } 
    return false;
  }
  
  public synchronized LogicalPlan apply(LogicalPlan plan) {
    if (PROCESSING_RULE)
      return plan; 
    if (signatureMatch(plan))
      return modifySubPlan(plan); 
    return (LogicalPlan)plan.transform((PartialFunction)new ModifyPlanFunc());
  }
  
  public class ModifyPlanFunc extends AbstractPartialFunction<LogicalPlan, LogicalPlan> {
    public synchronized boolean isDefinedAt(LogicalPlan p) {
      if (p.children() == null)
        return false; 
      List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(p.children()).asJava();
      for (LogicalPlan child : children) {
        if (!AbstractRule.PROCESSING_RULE && AbstractRule.this.signatureMatch(child)) {
          AbstractRule.PROCESSING_RULE = true;
          return true;
        } 
      } 
      return false;
    }
    
    public synchronized LogicalPlan apply(LogicalPlan v1) {
      List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(v1.children()).asJava();
      List<LogicalPlan> modifiedChildren = new ArrayList<>();
      for (LogicalPlan child : children) {
        if (AbstractRule.this.signatureMatch(child)) {
          LogicalPlan modifiedChild = AbstractRule.this.modifySubPlan(child);
          modifiedChildren.add(modifiedChild);
          AbstractRule.logger.info("[PeregrineRule] {} rule applied on HTS {}", AbstractRule.this.getRuleClassName(), LogicalPlanSignature.HTS(child));
          continue;
        } 
        modifiedChildren.add(child);
      } 
      Seq<LogicalPlan> modifiedChildrenSeq = ((Iterator)JavaConverters.asScalaIteratorConverter(modifiedChildren.iterator()).asScala()).toSeq();
      LogicalPlan lp = (LogicalPlan)v1.withNewChildren(modifiedChildrenSeq);
      AbstractRule.PROCESSING_RULE = false;
      return lp;
    }
  }
  
  protected String getRuleClassName() {
    return getClass().getSimpleName();
  }
  
  protected abstract int getRuleId();
  
  protected abstract boolean isApplicable(Annotation paramAnnotation, LogicalPlan paramLogicalPlan);
  
  protected abstract LogicalPlan modifySubPlan(LogicalPlan paramLogicalPlan);
}
