package com.microsoft.peregrine.spark.planir.parsers.entities;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkOp extends Operator {
  private static String numberSymbol = "^\\d+\\W+";
  
  private static String serialNumber = "\\*\\(\\d+\\)";
  
  public SparkOp(String opInfo) {
    super(opInfo);
  }
  
  protected void parseOpInfo(String opInfo) {
    if (opInfo == null || opInfo.isEmpty()) {
      this.name = opInfo;
      return;
    } 
    this.children = new ArrayList();
    String[] tokens = opInfo.split(" ");
    int i = 0;
    this.name = tokens[i++];
    if (this.name.equals("Execute") && i < tokens.length)
      this.name += " " + tokens[i++]; 
    if ((this.name.matches(serialNumber) || this.name.matches(numberSymbol)) && i < tokens.length)
      this.name = tokens[i++]; 
    this.name = this.name.split("\\(")[0].split("\\[")[0];
    if (this.name.charAt(0) == '*')
      this.name = this.name.split("\\*")[1]; 
    if (this.name == null || this.name.isEmpty())
      throw new RuntimeException("Empty name"); 
    if (i < tokens.length) {
      List<String> seg = Arrays.<String>asList(tokens).subList(i, tokens.length);
      this.opInfo = String.join(" ", (Iterable)seg);
      ParseRemaining();
    } 
  }
  
  private void ParseRemaining() {}
}
