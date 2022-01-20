package com.microsoft.peregrine.core.tasks;

import com.microsoft.peregrine.core.config.legacy.Config;

public interface Task<Input extends com.microsoft.peregrine.core.planir.preprocess.data.ir.IR, Output extends com.microsoft.peregrine.core.planir.preprocess.data.ir.IR> {
  String getName();
  
  Output execute(Config paramConfig, Input paramInput);
}
