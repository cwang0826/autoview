package com.huawei.cloudviews.core.signatures;

public interface Signature<E> {
  String getSignature(E paramE);
  
  String getLocalSignature(E paramE);
}
