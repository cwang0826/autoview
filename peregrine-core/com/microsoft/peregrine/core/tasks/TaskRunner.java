package com.microsoft.peregrine.core.tasks;

import com.microsoft.peregrine.core.config.PropertyConfiguration;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.FileIR;

public class TaskRunner {
  private int paramIdx = 0;
  
  private boolean checkNextParam(String[] params) {
    return (this.paramIdx < params.length);
  }
  
  private String getNextParam(String[] params, String paramName) {
    if (checkNextParam(params))
      return params[this.paramIdx++]; 
    throw new RuntimeException("Parameter " + paramName + " missing at index " + this.paramIdx);
  }
  
  protected String getFullyQualifiedTaskName(String taskName) {
    return taskName;
  }
  
  public String run(String[] args) throws Exception {
    String taskName = getNextParam(args, "Task Name");
    String propertyFilename = getNextParam(args, "Property Filename");
    String inputIRFilename = null;
    if (checkNextParam(args))
      inputIRFilename = getNextParam(args, "IR Filename"); 
    PropertyConfiguration conf = new PropertyConfiguration(propertyFilename);
    FileIR inputIR = new FileIR(inputIRFilename, conf.get("IR_delimiter"));
    Class<?> clazz = Class.forName(getFullyQualifiedTaskName(taskName));
    FileBasedTask task = (FileBasedTask)clazz.newInstance();
    FileIR outputIR = task.execute(conf, inputIR);
    return outputIR.getName();
  }
}
