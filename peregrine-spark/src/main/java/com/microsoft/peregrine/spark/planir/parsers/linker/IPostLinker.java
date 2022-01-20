package com.microsoft.peregrine.spark.planir.parsers.linker;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;

@FunctionalInterface
public interface IPostLinker {
  void apply(Operator paramOperator1, Operator paramOperator2);
}
