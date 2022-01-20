package com.microsoft.peregrine.spark.planir.parsers.linker.lexical;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class LexicalEquivalence {
  private static Multimap<String, String> equivalent;
  
  public LexicalEquivalence() {
    if (equivalent == null) {
      equivalent = (Multimap<String, String>)HashMultimap.create();
      init();
    } 
  }
  
  public boolean areEquivalent(String op1, String op2) {
    return (equivalent.containsEntry(op1, op2) || equivalent.containsEntry(op2, op1));
  }
  
  private void init() {
    add("Aggregate", "HashAggregateExec");
    add("AppendColumns", "AppendColumnsExec");
    add("AppendColumns", "AppendColumnsWithObject");
    add("CoGroup", "CoGroupExec");
    add("DataWritingCommand", "DataWritingCommandExec");
    add("DeserializeToObject", "DeserializeToObjectExec");
    add("Expand", "ExpandExec");
    add("ExternalRDD", "ExternalRDDScanExec");
    add("Filter", "FilterExec");
    add("FlatMapGroupsInPandas", "FlatMapGroupsInPandasExec");
    add("FlatMapGroupsInR", "FlatMapGroupsInRExec");
    add("FlatMapGroupsInRWithArrow", "FlatMapGroupsInRWithArrowExec");
    add("FlatMapGroupsWithState", "MapGroupsExec");
    add("Generate", "GenerateExec");
    add("GlobalLimit", "CollectLimitExec");
    add("GlobalLimit", "GlobalLimitExec");
    add("HiveTableRelation", "HiveTableScanExec");
    add("Join", "BroadcastHashJoinExec");
    add("Join", "BroadcastNestedLoopJoinExec");
    add("Join", "CartesianProductExec");
    add("Join", "ShuffledHashJoinExec");
    add("Join", "SortMergeJoinExec");
    add("Limit", "CollectLimitExec");
    add("Limit", "TakeOrderedAndProjectExec");
    add("LocalLimit", "CollectLimitExec");
    add("LocalLimit", "LocalLimitExec");
    add("LocalRelation", "LocalTableScanExec");
    add("LogicalRDD", "RDDScanExec");
    add("LogicalRelation", "FileSourceScanExec");
    add("MapElements", "MapElementsExec");
    add("MapGroups", "MapGroupsExec");
    add("MapPartitions", "MapPartitionsExec");
    add("MapPartitionsInRWithArrow", "MapPartitionsInRWithArrowExec");
    add("MemoryPlan", "LocalTableScanExec");
    add("MemoryPlanV2", "LocalTableScanExec");
    add("OneRowRelation", "RDDScanExec");
    add("Project", "ProjectExec");
    add("Range", "RangeExec");
    add("Repartition", "ShuffleExchangeExec");
    add("Repartition", "CoalesceExec");
    add("RepartitionByExpression", "ShuffleExchangeExec");
    add("RunnableCommand", "ExecutedCommandExec");
    add("Sample", "SampleExec");
    add("SerializeFromObject", "SerializeFromObjectExec");
    add("Sort", "SortExec");
    add("TypedFilter", "FilterExec");
    add("Union", "UnionExec");
  }
  
  private void add(String op1, String op2) {
    equivalent.put(op1, op2);
    equivalent.put(op2, op1);
  }
}
