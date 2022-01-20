package com.microsoft.peregrine.core.feedback.annotations;

public abstract class Annotation {
  protected String recurringSignature;
  
  protected AnnotationAction action;
  
  protected enum AnnotationPriority {
    HIGHEST(1),
    OPPORTUNISTIC(100);
    
    private final int value;
    
    AnnotationPriority(int value) {
      this.value = value;
    }
    
    public int getValue() {
      return this.value;
    }
  }
  
  protected AnnotationPriority priority = AnnotationPriority.OPPORTUNISTIC;
  
  public Annotation(String recurringSignature, AnnotationAction action) {
    this.recurringSignature = recurringSignature;
    this.action = action;
  }
  
  public String getRecurringSignature() {
    return this.recurringSignature;
  }
  
  public AnnotationAction getAction() {
    return this.action;
  }
  
  public abstract AnnotationType getType();
  
  public static Annotation getInstance(String annotationString) {
    String recurringSignature;
    AnnotationType type;
    String[] tokens = annotationString.split(" ");
    if (tokens.length != 3)
      throw new RuntimeException("Expected 3 fields in an annotation string. Invalid annotation string: " + annotationString); 
    try {
      recurringSignature = tokens[0];
    } catch (NumberFormatException e) {
      throw new RuntimeException("Cannot parse the signature: " + tokens[0]);
    } 
    String annotationType = tokens[1];
    String payload = tokens[2];
    try {
      type = AnnotationType.valueOf(annotationType);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Cannot parse the annotation type: " + annotationType);
    } 
    switch (type) {
      case Materialize:
        return new MaterializeAnnotation(recurringSignature, payload);
      case Reuse:
        return new ReuseAnnotation(recurringSignature, payload);
    } 
    throw new RuntimeException("Failed to created annotation instance!");
  }
  
  public boolean htMatch(String ht) {
    return this.recurringSignature.equals(ht);
  }
  
  public abstract boolean htsMatch(String paramString);
  
  public String toString() {
    return this.recurringSignature + " " + getType().toString() + " " + getAction().toString();
  }
  
  public int getPriority() {
    return this.priority.getValue();
  }
}
