package com.huawei.cloudviews.core.planir.parsers;

public interface IParser<Input, Entity> {
  Entity parse(Input paramInput);
}
