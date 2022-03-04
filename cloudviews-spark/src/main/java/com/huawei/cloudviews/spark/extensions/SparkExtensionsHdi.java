package com.huawei.cloudviews.spark.extensions;

import com.huawei.cloudviews.core.config.PropertyConfiguration;
import com.huawei.cloudviews.core.config.legacy.Config;

public class SparkExtensionsHdi extends SparkExtensions {
  protected Config getConf() {
    return (Config)new PropertyConfiguration("/opt/cloudviews/analyze/cloudviews-spark.properties");
  }
}
