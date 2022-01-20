package com.microsoft.peregrine.core.signatures;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import com.microsoft.peregrine.core.signatures.hash.SignHash64;

public abstract class OperatorSignature implements Signature<Operator> {
  private static OperatorSignature _hts;
  
  private static OperatorSignature _ht;
  
  public String getSignature(Operator op) {
    String sig = getLocalSignature(op);
    for (Operator child : op.children)
      sig = SignHash64.Compute(sig, getSignature(child), 0L); 
    return sig;
  }
  
  public static String HTS(final Operator op) {
    if (_hts == null)
      _hts = new OperatorSignature() {
          public String getLocalSignature(Operator node) {
            return SignHash64.Compute(SignHash64.Compute(op.name), op.opInfo);
          }
        }; 
    return _hts.getSignature(op);
  }
  
  public static String HT(final Operator op) {
    if (_ht == null)
      _ht = new OperatorSignature() {
          public String getLocalSignature(Operator node) {
            return SignHash64.Compute(op.name);
          }
        }; 
    return _ht.getSignature(op);
  }
}
