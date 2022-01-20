package com.microsoft.peregrine.core.planir.preprocess.enumerators;

import java.util.Iterator;

public interface IEnumerator<E> {
  Iterator<E> enumerate();
}
