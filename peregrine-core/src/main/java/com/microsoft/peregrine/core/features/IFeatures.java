package com.huawei.cloudviews.core.features;

import org.apache.spark.mllib.linalg.Vector;

public interface IFeatures {
  Vector getFeatureVector();
}
