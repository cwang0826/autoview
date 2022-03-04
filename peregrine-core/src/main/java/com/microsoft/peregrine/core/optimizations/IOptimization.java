package com.huawei.cloudviews.core.optimizations;

import com.huawei.cloudviews.core.feedback.annotations.Annotation;
import java.util.List;

public interface IOptimization<Input extends com.huawei.cloudviews.core.planir.preprocess.data.ir.IR, Output> {
  Input run(Input paramInput);
  
  List<Annotation> getAnnotations(String paramString);
  
  Output getResult();
}
