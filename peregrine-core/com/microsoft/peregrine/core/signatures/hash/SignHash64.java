package com.microsoft.peregrine.core.signatures.hash;

public class SignHash64 {
  public static String Compute(String base, String pString, long uSeed) {
    SignHash32.Result r = SignHash32.Compute2(base + pString, (int)uSeed, (int)(uSeed >> 32L));
    return Long.toUnsignedString(r.hash1 | r.hash2 << 32L);
  }
  
  public static String Compute(String base, String pString) {
    if (pString == null)
      return base; 
    if (base == null)
      return Compute("", pString, 0L); 
    return Compute(base, pString, 0L);
  }
  
  public static String Compute(String pString) {
    return Compute(null, pString);
  }
}
