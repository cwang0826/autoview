package com.microsoft.peregrine.core.feedback;

import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class FeedbackService extends AbstractFeedback {
  public void initialize(InputStream input) {
    throw new UnsupportedOperationException();
  }
  
  public void update(List<Annotation> annotations, OutputStream out) {
    throw new UnsupportedOperationException();
  }
  
  public List<Annotation> get(String identifier) {
    throw new UnsupportedOperationException();
  }
}
