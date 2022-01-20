package com.microsoft.peregrine.core.tasks;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.connectors.spark.SparkSQL;
import com.microsoft.peregrine.core.feedback.AbstractFeedback;
import com.microsoft.peregrine.core.feedback.FeedbackFile;
import com.microsoft.peregrine.core.feedback.IFeedback;
import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import com.microsoft.peregrine.core.optimizations.multiquery.viewselection.DataFrameWeightedHeuristicsViewSelection;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.DataFrameIR;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.FileIR;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.IR;
import java.util.List;

public class ViewSelectionTask implements FileBasedTask {
  public String getName() {
    return getClass().getSimpleName();
  }
  
  public FileIR execute(Config conf, FileIR input) {
    DataFrameWeightedHeuristicsViewSelection opt = new DataFrameWeightedHeuristicsViewSelection(conf);
    DataFrameIR dfInput = new DataFrameIR(new SparkSQL(), input, conf.get("IR_tablename"));
    dfInput.persist();
    DataFrameIR selectedViewsIR = opt.run(dfInput);
    List<Annotation> annotations = opt.getAnnotations(conf.get("ComputeReuse_materializePath"));
    IFeedback feedback = AbstractFeedback.getInstance(conf.get("FeedbackType"));
    feedback.update(annotations, FeedbackFile.getOutputStream(conf.get("ComputeReuse_feedbackPath"), "local"));
    selectedViewsIR.writeToFile(conf.get("View_Selection_IR"));
    FileIR fileIR = new FileIR(conf.get("View_Selection_IR"), conf.get("IR_delimiter"));
    return fileIR;
  }
}
