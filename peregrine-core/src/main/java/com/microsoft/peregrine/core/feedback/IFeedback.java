package com.microsoft.peregrine.core.feedback;

import com.microsoft.peregrine.core.feedback.annotations.Annotation;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IFeedback {
  void initialize(InputStream paramInputStream);
  
  void update(List<Annotation> paramList, OutputStream paramOutputStream);
  
  List<Annotation> get(String paramString);
}
