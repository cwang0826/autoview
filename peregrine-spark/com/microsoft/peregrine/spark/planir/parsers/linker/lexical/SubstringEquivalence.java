package com.microsoft.peregrine.spark.planir.parsers.linker.lexical;

import java.util.ArrayList;
import java.util.List;

public class SubstringEquivalence {
  private static List<String> affixes;
  
  public SubstringEquivalence() {
    if (affixes == null) {
      affixes = new ArrayList<>();
      init();
    } 
  }
  
  public boolean areContained(String op1, String op2) {
    return (op1.contains(op2) || op2.contains(op1));
  }
  
  public String scrub(String input) {
    String output = input;
    for (String affix : affixes)
      output = output.replaceAll(affix, ""); 
    return new String(output);
  }
  
  private void init() {
    add("Exec");
    add("Broadcast");
    add("Collect");
    add("Command");
    add("Exec");
    add("Global");
    add("Hash");
    add("Local");
    add("Relation");
    add("Scan");
  }
  
  private void add(String op) {
    affixes.add(op);
  }
}
