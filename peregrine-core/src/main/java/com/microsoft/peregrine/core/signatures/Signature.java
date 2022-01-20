package com.microsoft.peregrine.core.signatures;

public interface Signature<E> {
  String getSignature(E paramE);
  
  String getLocalSignature(E paramE);
}
