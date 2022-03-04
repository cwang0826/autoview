package com.huawei.cloudviews.spark.planir.parsers.linker;

import com.huawei.cloudviews.core.planir.parsers.entities.Operator;

@FunctionalInterface
public interface IPostLinker {
  void apply(Operator paramOperator1, Operator paramOperator2);
}
