package com.microsoft.peregrine.core.optimizations;

import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import java.util.List;

public interface IOptimization<Input extends com.microsoft.peregrine.core.planir.preprocess.data.ir.IR, Output> {
  Input run(Input paramInput);
  
  List<Annotation> getAnnotations(String paramString);
  
  Output getResult();
}
