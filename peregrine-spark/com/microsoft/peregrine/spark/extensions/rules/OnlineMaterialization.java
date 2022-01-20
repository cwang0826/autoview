package com.microsoft.peregrine.spark.extensions.rules;

import com.microsoft.peregrine.core.connectors.spark.SparkSQL;
import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import com.microsoft.peregrine.core.feedback.annotations.AnnotationAction;
import com.microsoft.peregrine.core.feedback.annotations.AnnotationType;
import com.microsoft.peregrine.core.utils.FSUtils;
import com.microsoft.peregrine.spark.signature.LogicalPlanSignature;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

public class OnlineMaterialization extends AbstractRule {
  private static final Logger logger = LoggerFactory.getLogger(OnlineMaterialization.class);
  
  private String materializePathString;
  
  private String materializeFormat;
  
  private String lockPathString;
  
  protected int getRuleId() {
    return 0;
  }
  
  public OnlineMaterialization(SparkSession spark) {
    super(spark);
    this.materializeFormat = this.sparkSQL.getProperty(materializeFormatKey);
  }
  
  public static void setMaterializeFormat(SparkSQL sparkSQL, String format) {
    sparkSQL.setProperty(materializeFormatKey, format);
  }
  
  protected synchronized boolean isApplicable(Annotation annotation, LogicalPlan plan) {
    try {
      String h = LogicalPlanSignature.HT(plan);
      if (processedSignatures.contains(h))
        return false; 
      String hts = LogicalPlanSignature.HTS(plan);
      if (annotation.getType() == AnnotationType.Materialize && ((AnnotationAction.Persist)annotation
        .getAction()).viewToCreate(hts)) {
        String ht = LogicalPlanSignature.HT(plan);
        AnnotationAction.Persist action = (AnnotationAction.Persist)annotation.getAction();
        this.materializePathString = action.getPath(hts);
        this.lockPathString = action.getLockPath(hts);
        return true;
      } 
    } catch (Exception e) {
      logger.error("[PeregrineRule] Exception in checking materialization rule ", e);
    } 
    return false;
  }
  
  protected synchronized LogicalPlan modifySubPlan(LogicalPlan p) {
    try {
      String h = LogicalPlanSignature.HT(p);
      if (processedSignatures.contains(h))
        return p; 
      processedSignatures.add(h);
      if (!FSUtils.lock(this.lockPathString, -1L))
        return p; 
      Dataset<Row> cachedDataset = this.sparkSQL.createDataFrame(p);
      Dataset<Row> sanitizedDataset = sanitizeSchemaForMaterialization(cachedDataset);
      sanitizedDataset.write().format(this.materializeFormat).save(this.materializePathString);
      FSUtils.delete(this.lockPathString);
    } catch (Exception e) {
      logger.error("[PeregrineRule] Exception in materialization on HTS: {} , HT: {}.", LogicalPlanSignature.HTS(p), LogicalPlanSignature.HT(p));
      logger.error("[PeregrineRule] Exception in materialization of output columns {}.", p.output());
      logger.error("[PeregrineRule] Exception in materialization ", e);
    } 
    return p;
  }
  
  private Dataset<Row> sanitizeSchemaForMaterialization(Dataset<Row> input) {
    int columnCount = (input.columns()).length;
    ArrayBuffer arrayBuffer = new ArrayBuffer();
    for (int i = 0; i < columnCount; i++) {
      String newName = "column" + i;
      arrayBuffer.$plus$eq(newName);
    } 
    return input.toDF((Seq)arrayBuffer);
  }
}
