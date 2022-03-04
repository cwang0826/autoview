package com.huawei.cloudviews.spark.planir.parsers.linker.lexical;

import com.huawei.cloudviews.core.planir.parsers.entities.Operator;

public class LexicalSimilarity {
  LexicalEquivalence le = new LexicalEquivalence();
  
  SubstringEquivalence se = new SubstringEquivalence();
  
  OverlapEquivalence oe = new OverlapEquivalence();
  
  final double SIMILAR = 1.0D;
  
  public double getLexicalSimilarityScore(Operator original, Operator transform) {
    String oName = original.name;
    String tName = transform.name;
    return getLexicalSimilarityScore(oName, tName);
  }
  
  public double getLexicalSimilarityScore(String original, String transform) {
    if (this.le.areEquivalent(original, transform))
      return 1.0D; 
    String oScrubbed = this.se.scrub(original);
    String tScrubbed = this.se.scrub(transform);
    if (this.se.areContained(oScrubbed, tScrubbed))
      return 1.0D; 
    return this.oe.getOverlapSimilarityScore(oScrubbed, tScrubbed);
  }
}
