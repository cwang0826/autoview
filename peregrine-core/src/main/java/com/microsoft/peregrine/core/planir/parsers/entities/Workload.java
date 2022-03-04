package com.huawei.cloudviews.core.planir.parsers.entities;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Workload {
  Iterator<Application> applications;
  
  public Workload(Application application) {
    List<Application> lst = new ArrayList<>();
    lst.add(application);
    this.applications = lst.iterator();
  }
  
  public Workload(Iterator<Application> applications) {
    this.applications = applications;
  }
  
  public Iterator<Application> getApplications() {
    return this.applications;
  }
}
