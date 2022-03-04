package com.huawei.cloudviews.spark.planir.parsers.linker.lexical;

public class OverlapEquivalence {
  public double getOverlapSimilarityScore(String op1, String op2) {
    int maxPossibleOverlap = Math.min(op1.length(), op2.length());
    int longestOverlap = 0;
    for (int length = maxPossibleOverlap; length > 0; length--) {
      for (int i = 0; i + length <= op1.length(); i++) {
        String substr = op1.substring(i, i + length);
        if (op2.contains(substr)) {
          longestOverlap = length;
          break;
        } 
      } 
      if (longestOverlap == length)
        break; 
    } 
    return longestOverlap * 1.0D / maxPossibleOverlap;
  }
}
