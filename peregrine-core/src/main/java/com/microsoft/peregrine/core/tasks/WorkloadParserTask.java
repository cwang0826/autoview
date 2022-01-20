package com.microsoft.peregrine.core.tasks;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.planir.parsers.entities.Workload;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.FileIR;
import com.microsoft.peregrine.core.planir.preprocess.data.ir.IR;
import com.microsoft.peregrine.core.planir.preprocess.enumerators.IEnumerator;

public abstract class WorkloadParserTask implements FileBasedTask {
  public String getName() {
    return getClass().getSimpleName();
  }
  
  public FileIR execute(Config conf, FileIR inputIR) {
    return execute(conf);
  }
  
  public FileIR execute(Config conf) {
    Workload workload = getWorkload(conf);
    FileIR currentIR = new FileIR(getEnumerator(workload), conf.get("IR_filename"), conf.get("IR_delimiter"));
    currentIR.persist();
    return currentIR;
  }
  
  protected abstract Workload getWorkload(Config paramConfig);
  
  protected abstract IEnumerator getEnumerator(Workload paramWorkload);
}
