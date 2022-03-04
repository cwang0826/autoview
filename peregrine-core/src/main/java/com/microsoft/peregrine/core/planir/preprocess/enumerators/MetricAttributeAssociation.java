package com.huawei.cloudviews.core.planir.preprocess.enumerators;

import java.util.Set;

public interface MetricAttributeAssociation {
  Set<String> getMetricNames(String paramString);
  
  Set<String> getAttributeNames(String paramString);
}
