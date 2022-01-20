package com.microsoft.peregrine.spark.planir.parsers;

import com.microsoft.peregrine.core.planir.parsers.PlanParser;
import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.planir.parsers.entities.Plan;
import com.microsoft.peregrine.spark.planir.parsers.entities.SparkOp;

public class SparkPlanParser extends PlanParser<String> {
  public Plan parse(String planString) {
    Plan p = null;
    if (!planString.startsWith("Execute InsertInto") && 
      !planString.startsWith("InsertInto")) {
      p = new Plan();
      p.tree = (Operator)new Operator.RootOp();
      String[] opLines = planString.trim().split("\n");
      getOpTree(opLines, 0, -1, p.tree);
      assert p.tree.HTS() != null;
    } 
    return p;
  }
  
  protected Operator getOp(String opString) {
    return new SparkOp(opString);
  }
}
