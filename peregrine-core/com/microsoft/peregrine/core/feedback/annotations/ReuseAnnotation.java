package com.microsoft.peregrine.core.feedback.annotations;

import com.microsoft.peregrine.core.utils.FSUtils;

public class ReuseAnnotation extends Annotation {
  public ReuseAnnotation(String recurringSignature, String payload) {
    super(recurringSignature, new AnnotationAction.Read(payload));
    this.priority = Annotation.AnnotationPriority.HIGHEST;
  }
  
  public AnnotationType getType() {
    return AnnotationType.Reuse;
  }
  
  public boolean htsMatch(String hts) {
    if (!FSUtils.exists(((AnnotationAction.Read)this.action).getPath(hts)))
      return false; 
    return true;
  }
}
