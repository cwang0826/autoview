package com.microsoft.peregrine.core.feedback.annotations;

import com.microsoft.peregrine.core.utils.FSUtils;
import java.io.File;
import java.nio.file.Paths;

public abstract class AnnotationAction {
  protected String params;
  
  public AnnotationAction(String params) {
    this.params = params;
  }
  
  public String toString() {
    return this.params;
  }
  
  public static class Read extends AnnotationAction {
    public Read(String params) {
      super(params);
    }
    
    public String getPath(String hts) {
      return this.params + File.separator + hts;
    }
  }
  
  public static class Persist extends AnnotationAction {
    private static final long expiryInMillis = 900000L;
    
    public Persist(String params) {
      super(params);
    }
    
    public boolean viewToCreate(String hts) {
      if (FSUtils.exists(getPath(hts)))
        return false; 
      return true;
    }
    
    public String getLockPath(String hts) {
      return Paths.get(this.params, new String[] { hts + ".lock" }).toString();
    }
    
    public String getPath(String hts) {
      return this.params + File.separator + hts;
    }
  }
}
