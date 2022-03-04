package com.huawei.cloudviews.core.tasks;

import com.huawei.cloudviews.core.config.legacy.Config;

public interface Task<Input extends com.huawei.cloudviews.core.planir.preprocess.data.ir.IR, Output extends com.huawei.cloudviews.core.planir.preprocess.data.ir.IR> {
  String getName();
  
  Output execute(Config paramConfig, Input paramInput);
}
