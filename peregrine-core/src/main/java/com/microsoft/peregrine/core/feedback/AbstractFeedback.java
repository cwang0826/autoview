package com.microsoft.peregrine.core.feedback;

public abstract class AbstractFeedback implements IFeedback {
  public static IFeedback getInstance(String feedbackType) {
    IFeedback feedback;
    switch (feedbackType) {
      case "file":
        feedback = new FeedbackFile();
        return feedback;
      case "service":
        feedback = new FeedbackService();
        return feedback;
    } 
    throw new RuntimeException("Invalid feedback type: " + feedbackType + ". Options are file|service");
  }
}
