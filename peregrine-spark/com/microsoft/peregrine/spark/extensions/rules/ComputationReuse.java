package com.microsoft.peregrine.spark.extensions.rules;

import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import com.microsoft.peregrine.core.feedback.annotations.AnnotationAction;
import com.microsoft.peregrine.core.feedback.annotations.AnnotationType;
import com.microsoft.peregrine.spark.signature.LogicalPlanSignature;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.physical.Partitioning;
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.RDDConversions;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

public class ComputationReuse extends AbstractRule {
  public static String reuseRddPrefix = "ReuseRDD:";
  
  private String reusePathString;
  
  private String format;
  
  private final String csvDelimiter = ",";
  
  protected int getRuleId() {
    return 1;
  }
  
  public ComputationReuse(SparkSession spark) {
    super(spark);
    this.format = this.sparkSQL.getProperty(materializeFormatKey);
  }
  
  protected synchronized boolean isApplicable(Annotation annotation, LogicalPlan plan) {
    String hts = LogicalPlanSignature.HTS(plan);
    if (annotation.getType() == AnnotationType.Reuse && annotation
      .htsMatch(hts)) {
      String ht = LogicalPlanSignature.HT(plan);
      AnnotationAction.Read action = (AnnotationAction.Read)annotation.getAction();
      this.reusePathString = action.getPath(hts);
      return true;
    } 
    return false;
  }
  
  protected AnnotationType getApplicableAnnotationType() {
    return AnnotationType.Reuse;
  }
  
  protected synchronized LogicalPlan modifySubPlan(LogicalPlan p) {
    LogicalPlan newPlan = getPlanUsingRdd(p);
    return newPlan;
  }
  
  private LogicalPlan getPlanUsingRdd(LogicalPlan p) {
    StructType sanitizedSchema = sanitizeSchemaForRead(p.schema());
    Dataset<Row> view = this.sparkSQL.createDataFrame(this.reusePathString, ",", sanitizedSchema, this.format);
    RDD<Row> viewRdd = view.rdd();
    ArrayBuffer arrayBuffer = new ArrayBuffer();
    Iterator<StructField> schemaIterator = view.schema().iterator();
    while (schemaIterator.hasNext()) {
      StructField field = (StructField)schemaIterator.next();
      arrayBuffer.$plus$eq(field.dataType());
    } 
    RDD<InternalRow> catalystRDD = RDDConversions.rowToRowRdd(viewRdd, (Seq)arrayBuffer);
    String reuseSignature = LogicalPlanSignature.getSignatures(p);
    catalystRDD.setName(reuseRddPrefix + reuseSignature);
    LogicalRDD viewWithConsistentAttributes = new LogicalRDD(p.output(), catalystRDD, (Partitioning)new UnknownPartitioning(0), null, false, this.sparkSQL.getCurrentSparkSession());
    return (LogicalPlan)viewWithConsistentAttributes;
  }
  
  private StructType sanitizeSchemaForRead(StructType planSchema) {
    List<StructField> newFields = new ArrayList<>();
    Iterator<StructField> planSchemaIterator = planSchema.iterator();
    int index = 0;
    while (planSchemaIterator.hasNext()) {
      StructField sf = (StructField)planSchemaIterator.next();
      String newName = "column" + index;
      newFields.add(new StructField(newName, sf.dataType(), sf.nullable(), sf.metadata()));
      index++;
    } 
    StructType sanitizedSchema = DataTypes.createStructType(newFields);
    return sanitizedSchema;
  }
  
  private LogicalPlan getPlanUsingLocalRelation(LogicalPlan p) {
    Dataset<Row> view = this.sparkSQL.createDataFrame(this.reusePathString, ",", p.schema(), this.format);
    AttributeSet planAttributes = p.outputSet();
    return (LogicalPlan)LocalRelation.fromExternalRows(planAttributes.toSeq(), (
        (Iterator)JavaConverters.asScalaIteratorConverter(view.toLocalIterator()).asScala()).toSeq());
  }
  
  private void getPlanUsingHadoopFs(LogicalPlan p) {
    HadoopFsRelation hadoopFsRelation = null;
    LogicalRelation logicalRelation = null;
  }
}
