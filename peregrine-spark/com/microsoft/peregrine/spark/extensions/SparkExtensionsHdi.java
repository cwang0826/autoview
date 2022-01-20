package com.microsoft.peregrine.spark.extensions;

import com.microsoft.peregrine.core.config.PropertyConfiguration;
import com.microsoft.peregrine.core.config.legacy.Config;

public class SparkExtensionsHdi extends SparkExtensions {
  protected Config getConf() {
    return (Config)new PropertyConfiguration("/opt/peregrine/analyze/peregrine-spark.properties");
  }
}
