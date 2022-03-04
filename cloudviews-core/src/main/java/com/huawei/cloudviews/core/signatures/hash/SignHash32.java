package com.huawei.cloudviews.core.signatures.hash;

import java.nio.charset.Charset;

public class SignHash32 {
  static class Result {
    int hash1;
    
    int hash2;
  }
  
  static Result Compute2(String pString, int uSeed1, int uSeed2) {
    return Compute2(pString.getBytes(Charset.forName("UTF-8")), uSeed1, uSeed2);
  }
  
  static Result Compute2(byte[] pData, int uSeed1, int uSeed2) {
    Result r = new Result();
    int c = -559038737 + pData.length + uSeed1, b = c, a = b;
    c += uSeed2;
    int index = 0, size = pData.length;
    while (size > 12) {
      a += pData[index] + (pData[index + 1] << 8) + (pData[index + 2] << 16) + (pData[index + 3] << 24);
      b += pData[index + 4] + (pData[index + 5] << 8) + (pData[index + 6] << 16) + (pData[index + 7] << 24);
      c += pData[index + 8] + (pData[index + 9] << 8) + (pData[index + 10] << 16) + (pData[index + 11] << 24);
      a -= c;
      a ^= c << 4 | c >> 28;
      c += b;
      b -= a;
      b ^= a << 6 | a >> 26;
      a += c;
      c -= b;
      c ^= b << 8 | b >> 24;
      b += a;
      a -= c;
      a ^= c << 16 | c >> 16;
      c += b;
      b -= a;
      b ^= a << 19 | a >> 13;
      a += c;
      c -= b;
      c ^= b << 4 | b >> 28;
      b += a;
      index += 12;
      size -= 12;
    } 
    switch (size) {
      case 12:
        c += pData[index + 11] << 24;
      case 11:
        c += pData[index + 10] << 16;
      case 10:
        c += pData[index + 9] << 8;
      case 9:
        c += pData[index + 8];
      case 8:
        b += pData[index + 7] << 24;
      case 7:
        b += pData[index + 6] << 16;
      case 6:
        b += pData[index + 5] << 8;
      case 5:
        b += pData[index + 4];
      case 4:
        a += pData[index + 3] << 24;
      case 3:
        a += pData[index + 2] << 16;
      case 2:
        a += pData[index + 1] << 8;
      case 1:
        a += pData[index];
        break;
      case 0:
        r.hash1 = c;
        r.hash2 = b;
        return r;
    } 
    c ^= b;
    c -= b << 14 | b >> 18;
    a ^= c;
    a -= c << 11 | c >> 21;
    b ^= a;
    b -= a << 25 | a >> 7;
    c ^= b;
    c -= b << 16 | b >> 16;
    a ^= c;
    a -= c << 4 | c >> 28;
    b ^= a;
    b -= a << 14 | a >> 18;
    c ^= b;
    c -= b << 24 | b >> 8;
    r.hash1 = c;
    r.hash2 = b;
    return r;
  }
}
