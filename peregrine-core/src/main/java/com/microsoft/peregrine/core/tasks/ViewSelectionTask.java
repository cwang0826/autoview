package com.huawei.cloudviews.core.tasks;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.connectors.spark.SparkSQL;
import com.huawei.cloudviews.core.feedback.AbstractFeedback;
import com.huawei.cloudviews.core.feedback.FeedbackFile;
import com.huawei.cloudviews.core.feedback.IFeedback;
import com.huawei.cloudviews.core.feedback.annotations.Annotation;
import com.huawei.cloudviews.core.optimizations.multiquery.viewselection.DataFrameWeightedHeuristicsViewSelection;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.DataFrameIR;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.FileIR;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.IR;
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
