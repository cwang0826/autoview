package com.huawei.cloudviews.spark.extensions;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.config.legacy.PropertyConfig;
import com.huawei.cloudviews.core.connectors.spark.SparkSQL;
import com.huawei.cloudviews.core.utils.FSUtils;
import com.huawei.cloudviews.spark.extensions.rules.AbstractRule;
import com.huawei.cloudviews.spark.extensions.rules.ComputationReuse;
import com.huawei.cloudviews.spark.extensions.rules.OnlineMaterialization;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class SparkExtensions extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {
  public BoxedUnit apply(SparkSessionExtensions v1) {
    final Config conf = getConf();
    if (!ifFeedbackExists(conf))
      return null; 
    v1.injectOptimizerRule((Function1)new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
          public Rule<LogicalPlan> apply(SparkSession spark) {
            SparkSQL sparkSQL = new SparkSQL(spark);
            AbstractRule.setFeedbackType(sparkSQL, conf.get("FeedbackType"));
            AbstractRule.setFeedbackParams(sparkSQL, conf.get("FeedbackParams"));
            AbstractRule.setFeedbackLocation(sparkSQL, conf.getOrDefault("FeedbackLocation", "local"));
            OnlineMaterialization.setMaterializeFormat(sparkSQL, conf.get("MaterializeFormat"));
            FSUtils.setFS(conf.getOrDefault("MaterializeLocation", ""));
            return new OnlineMaterialization(spark);
          }
        });
    v1.injectOptimizerRule((Function1)new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
          public Rule<LogicalPlan> apply(SparkSession spark) {
            return new ComputationReuse(spark);
          }
        });
    return null;
  }
  
  private boolean ifFeedbackExists(Config conf) {
    if (conf == null || conf.isEmpty())
      return false; 
    String feedbackFile = conf.get("FeedbackParams");
    if (feedbackFile == null)
      return false; 
    if (conf.get("FeedbackLocation").equalsIgnoreCase("local"))
      return Files.exists((new File(feedbackFile)).toPath(), new java.nio.file.LinkOption[0]); 
    if (conf.get("FeedbackLocation").equalsIgnoreCase("distributed")) {
      Configuration hadoopConf = new Configuration();
      try {
        FileSystem fs = FileSystem.get(hadoopConf);
        return fs.exists(new Path(feedbackFile));
      } catch (IOException e) {
        e.printStackTrace();
      } 
    } 
    return false;
  }
  
  protected Config getConf() {
    return PropertyConfig.getInstance("cloudviews-spark.properties");
  }
}
