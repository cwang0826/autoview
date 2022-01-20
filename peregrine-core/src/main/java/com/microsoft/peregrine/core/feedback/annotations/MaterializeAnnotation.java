package com.microsoft.peregrine.core.feedback.annotations;

public class MaterializeAnnotation extends Annotation {
  public MaterializeAnnotation(String recurringSignature, String payload) {
    super(recurringSignature, new AnnotationAction.Persist(payload));
    this.priority = Annotation.AnnotationPriority.OPPORTUNISTIC;
  }
  
  public AnnotationType getType() {
    return AnnotationType.Materialize;
  }
  
  public boolean htsMatch(String hts) {
    throw new UnsupportedOperationException();
  }
}
