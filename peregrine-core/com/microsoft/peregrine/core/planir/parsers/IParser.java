package com.microsoft.peregrine.core.planir.parsers;

public interface IParser<Input, Entity> {
  Entity parse(Input paramInput);
}
