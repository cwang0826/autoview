package com.microsoft.peregrine.spark.planir.parsers.linker;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.spark.planir.parsers.linker.lexical.LexicalSimilarity;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PlanLinker {
  private final double weight = 0.5D;
  
  private final double delta = 0.05D;
  
  private final double highThreshold = 0.8D;
  
  private final double lowThreshold = 0.1D;
  
  private final double matchThreshold = 0.5D;
  
  private List<Operator> originalPost;
  
  private List<Operator> transformPost;
  
  private double[][] lsim;
  
  private double[][] ssim;
  
  private double[][] wsim;
  
  private Map<Integer, Integer> finalMatches;
  
  public PlanLinker(Operator original, Operator transform) {
    this.originalPost = postOrderTraversal(original);
    this.transformPost = postOrderTraversal(transform);
    int m = this.originalPost.size();
    int n = this.transformPost.size();
    this.lsim = new double[m][n];
    this.wsim = new double[m][n];
    this.ssim = new double[m][n];
  }
  
  public void run() {
    lexical();
    similar();
    saveFinalMatches();
  }
  
  public void printMatches() {
    for (Iterator<Integer> iterator = this.finalMatches.keySet().iterator(); iterator.hasNext(); ) {
      int i = ((Integer)iterator.next()).intValue();
      int j = ((Integer)this.finalMatches.get(Integer.valueOf(i))).intValue();
      String oName = ((Operator)this.originalPost.get(i)).name;
      String str1 = ((Operator)this.transformPost.get(j)).name;
    } 
  }
  
  public void postProcessMatches(IPostLinker function) {
    for (Iterator<Integer> iterator = this.finalMatches.keySet().iterator(); iterator.hasNext(); ) {
      int i = ((Integer)iterator.next()).intValue();
      int j = ((Integer)this.finalMatches.get(Integer.valueOf(i))).intValue();
      Operator o = this.originalPost.get(i);
      Operator t = this.transformPost.get(j);
      function.apply(o, t);
    } 
  }
  
  private void saveFinalMatches() {
    Operator oRoot = this.originalPost.get(this.originalPost.size() - 1);
    Operator tRoot = this.transformPost.get(this.transformPost.size() - 1);
    this.finalMatches = findStrongMatches(oRoot, tRoot);
  }
  
  private void lexical() {
    LexicalSimilarity ls = new LexicalSimilarity();
    for (int i = 0; i < this.originalPost.size(); i++) {
      Operator o = this.originalPost.get(i);
      for (int j = 0; j < this.transformPost.size(); j++) {
        Operator t = this.transformPost.get(j);
        this.lsim[i][j] = ls.getLexicalSimilarityScore(o, t);
        if ((o.children == null || o.children.isEmpty()) && (
          t.children == null || t.children.isEmpty())) {
          this.ssim[i][j] = this.lsim[i][j];
          this.wsim[i][j] = this.lsim[i][j];
        } 
      } 
    } 
  }
  
  private void similar() {
    for (int i = 0; i < this.originalPost.size(); i++) {
      Operator o = this.originalPost.get(i);
      for (int j = 0; j < this.transformPost.size(); j++) {
        Operator t = this.transformPost.get(j);
        Map<Integer, Integer> matches = findStrongMatches(o, t);
        this.ssim[i][j] = computeSsim(matches);
        this.wsim[i][j] = 0.5D * this.ssim[i][j] + 0.5D * this.lsim[i][j];
        if (this.wsim[i][j] > 0.8D) {
          changeWsim(matches, 0.05D);
        } else if (this.wsim[i][j] < 0.1D) {
          changeWsim(matches, -0.05D);
        } 
      } 
    } 
  }
  
  private void changeWsim(Map<Integer, Integer> matches, double delta) {
    for (Iterator<Integer> iterator = matches.keySet().iterator(); iterator.hasNext(); ) {
      int i = ((Integer)iterator.next()).intValue();
      int j = ((Integer)matches.get(Integer.valueOf(i))).intValue();
      this.wsim[i][j] = this.wsim[i][j] + delta;
    } 
  }
  
  private double computeSsim(Map<Integer, Integer> matches) {
    double ssim = 0.0D;
    if (matches.isEmpty())
      return 0.0D; 
    for (Iterator<Integer> iterator = matches.keySet().iterator(); iterator.hasNext(); ) {
      int i = ((Integer)iterator.next()).intValue();
      int j = ((Integer)matches.get(Integer.valueOf(i))).intValue();
      ssim += this.wsim[i][j];
    } 
    return ssim / matches.size();
  }
  
  private Map<Integer, Integer> findStrongMatches(Operator original, Operator transform) {
    Map<Integer, Integer> matches = new HashMap<>();
    List<Operator> originalPost = postOrderTraversal(original);
    List<Operator> transformPost = postOrderTraversal(transform);
    int min_index = 0;
    for (int i = 0; i < originalPost.size(); i++) {
      double max_wsim = 0.0D;
      for (int j = min_index; j < transformPost.size(); j++) {
        if (this.wsim[i][j] > 0.5D && this.wsim[i][j] > max_wsim) {
          max_wsim = this.wsim[i][j];
          min_index = j;
          matches.put(Integer.valueOf(i), Integer.valueOf(j));
        } 
      } 
    } 
    return matches;
  }
  
  private List<Operator> postOrderTraversal(Operator root) {
    return root.postOrderTraversal();
  }
}
