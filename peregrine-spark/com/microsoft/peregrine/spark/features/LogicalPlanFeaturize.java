package com.microsoft.peregrine.spark.features;

import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.AttributeMap;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.ListMap;
import scala.collection.immutable.Map;
import scala.math.BigInt;

public class LogicalPlanFeaturize {
  public CardinalityFeatures featurize(LogicalPlan node, SparkContext context) {
    CardinalityFeatures features = new CardinalityFeatures();
    features.setAppName(context.appName());
    BigInt out = EstimationUtils.getSizePerRow(node.output(), new AttributeMap((Map)new ListMap()));
    features.setAvgRowLength(out.longValue());
    Seq<LogicalPlan> leaves = node.collectLeaves();
    List<LogicalPlan> jLeaves = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(leaves).asJava();
    for (LogicalPlan leaf : jLeaves) {
      CatalogTable catalogTable = null;
      if (LogicalRelation.class.isAssignableFrom(leaf.getClass())) {
        LogicalRelation relation = (LogicalRelation)leaf;
        Option<CatalogTable> option = relation.catalogTable();
        if (!option.isEmpty())
          catalogTable = (CatalogTable)option.get(); 
      } else if (HiveTableRelation.class.isAssignableFrom(leaf.getClass())) {
        HiveTableRelation relation = (HiveTableRelation)leaf;
        catalogTable = relation.tableMeta();
      } 
      if (catalogTable != null)
        setCatalogFeatures(features, catalogTable); 
    } 
    return features;
  }
  
  private void setCatalogFeatures(CardinalityFeatures features, CatalogTable catalogTable) {
    String name = catalogTable.identifier().identifier();
    features.addInputDataset(name);
    Option<CatalogStatistics> catOption = catalogTable.stats();
    if (!catOption.isEmpty()) {
      CatalogStatistics stats = (CatalogStatistics)catOption.get();
      Option<BigInt> rcOption = stats.rowCount();
      if (!rcOption.isEmpty()) {
        long inputCard = ((BigInt)rcOption.get()).longValue();
        features.addInputCardinality(inputCard);
      } else {
        features.addInputCardinality(0L);
      } 
    } 
  }
}
