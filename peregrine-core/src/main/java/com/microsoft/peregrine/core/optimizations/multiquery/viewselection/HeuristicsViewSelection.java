package com.huawei.cloudviews.core.optimizations.multiquery.viewselection;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.DataFrameIR;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.IR;
import com.huawei.cloudviews.core.utils.PlotUtils;

public abstract class HeuristicsViewSelection<E extends IR> extends ViewSelection<E> {
  private boolean verboseMode = false;
  
  protected final int topK;
  
  protected final int perQueryTopK;
  
  protected final long maxSizeInMb;
  
  protected final int minRepeats;
  
  protected final int extraWeight;
  
  public HeuristicsViewSelection(Config conf) {
    super(conf);
    this.topK = Integer.parseInt(conf.getOrDefault("View_Max_Count", "1000"));
    this.perQueryTopK = Integer.parseInt(conf.getOrDefault("View_Per_Query_Max_Count", "1"));
    this.maxSizeInMb = Integer.parseInt(conf.getOrDefault("View_Per_View_Max_Size_Mb", "400"));
    this.minRepeats = Integer.parseInt(conf.getOrDefault("View_Min_Repeats", "2"));
    this.extraWeight = Integer.parseInt(conf.getOrDefault("View_Extra_Weight", "5"));
  }
  
  public abstract E addPrimaryKeys(E paramE);
  
  public abstract E filterSpecificOperators(E paramE);
  
  public abstract E repeatedViews(E paramE);
  
  public abstract E weightedOperators(E paramE);
  
  public abstract E filteredViews(E paramE);
  
  public abstract E perJobUniqueViews(E paramE);
  
  public abstract E scheduleAwareViews(E paramE);
  
  public abstract E topkViews(E paramE);
  
  public abstract E perQueryTopkViews(E paramE);
  
  public E run(E e) {
    if (this.verboseMode) {
      (new PlotUtils()).writeSubexpressionAnalysis((DataFrameIR)e);
      (new PlotUtils()).writeOperatorAnalysis((DataFrameIR)e);
    } 
    this.selectedViews = addPrimaryKeys(e);
    if (this.verboseMode)
      (new PlotUtils()).writeCandidateViews((DataFrameIR)this.selectedViews); 
    this.selectedViews = weightedOperators(this.selectedViews);
    this.selectedViews = filterSpecificOperators(this.selectedViews);
    this.selectedViews = repeatedViews(this.selectedViews);
    this.selectedViews = filteredViews(this.selectedViews);
    this.selectedViews = perJobUniqueViews(this.selectedViews);
    this.selectedViews = scheduleAwareViews(this.selectedViews);
    this.selectedViews = topkViews(this.selectedViews);
    this.selectedViews = perQueryTopkViews(this.selectedViews);
    this.selectedViews.show();
    if (this.verboseMode)
      (new PlotUtils()).writeSelectedViews((DataFrameIR)this.selectedViews); 
    return this.selectedViews;
  }
}
