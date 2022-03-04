package com.huawei.cloudviews.core.planir.preprocess.enumerators;

import com.huawei.cloudviews.core.planir.parsers.entities.Application;
import com.huawei.cloudviews.core.planir.parsers.entities.Metadata;
import com.huawei.cloudviews.core.planir.parsers.entities.Metric;
import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.parsers.entities.Query;
import com.huawei.cloudviews.core.planir.parsers.entities.QueryMetadata;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ViewEnumerator implements IEnumerator<View> {
  public static boolean CARDINALITY_FEATURES = false;
  
  private static final Logger logger = LoggerFactory.getLogger(ViewEnumerator.class);
  
  protected Workload workload;
  
  protected MetricAttributeAssociation metricAttributeAssociation;
  
  public ViewEnumerator(Workload workload, MetricAttributeAssociation metricAttributeAssociation) {
    this.workload = workload;
    this.metricAttributeAssociation = metricAttributeAssociation;
  }
  
  private void getViewsRecursive(Operator operator, long queryId, Metadata metadata, List<View> views, long level) {
    if (!(operator instanceof Operator.RootOp)) {
      View v = new View();
      setOperatorInfo(v, operator, level);
      setFeatureAttributes(v, operator);
      if (metadata != null)
        setMetadataInfo(v, queryId, metadata); 
      setMetricsInfo(v, operator.getMetrics());
      views.add(v);
    } 
    long nextLevel = level + 1L;
    for (Operator child : operator.children) {
      if (child != null)
        getViewsRecursive(child, queryId, metadata, views, nextLevel); 
    } 
  }
  
  protected void setOperatorInfo(View view, Operator node, long level) {
    view.set(View.OPERATOR_NAME, node.name);
    view.set(View.OPERATOR_ID, Long.valueOf(node.nodeId));
    view.set(View.TREE_LEVEL, Long.valueOf(level));
    view.set(View.PARENT_ID, Long.valueOf((node.parent != null) ? node.parent.nodeId : 0L));
    view.set(View.CHILD_COUNT, Long.valueOf(node.getChildren().size()));
    view.set(View.PARAMETERS, node.opInfo);
    view.set(View.STRICT_SIGNATURE, node.HTS());
    view.set(View.NON_STRICT_SIGNATURE, node.HT());
    view.set(View.LOGICAL_NAME, node.getLinkedLogicalName());
  }
  
  protected void setFeatureAttributes(View view, Operator node) {
    view.set(View.INPUT_CARD, Long.valueOf(node.getInputCardinality()));
    view.set(View.AVG_ROW_LEN, Long.valueOf(node.getAvgRowLength()));
    if (CARDINALITY_FEATURES) {
      view.set(View.INPUT_DATASET, "" + node.getInputDataset().hashCode());
    } else {
      view.set(View.INPUT_DATASET, node.getInputDataset());
    } 
  }
  
  protected void setMetadataInfo(View view, long queryId, Metadata metadata) {
    view.set(View.APP_ID, metadata.AppID);
    if (CARDINALITY_FEATURES) {
      view.set(View.APP_NAME, "" + metadata.AppName.hashCode());
    } else {
      view.set(View.APP_NAME, metadata.AppName);
    } 
    view.set(View.NORM_APP_NAME, normalize(metadata.AppName));
    view.set(View.USERNAME, metadata.UserName);
    view.set(View.CLUSTER_NAME, metadata.ClusterName);
    view.set(View.SUBSCRIPTION, metadata.Subscription);
    view.set(View.APP_SUBMIT_TIME, Long.valueOf(metadata.AppSubmitTime));
    view.set(View.APP_START_TIME, Long.valueOf(metadata.AppStartTime));
    view.set(View.APP_END_TIME, Long.valueOf(metadata.AppEndTime));
    view.set(View.APP_WALL_CLOCK_TIME, Long.valueOf(metadata.AppEndTime - metadata.AppStartTime));
    QueryMetadata queryMetadata = metadata.getQueryMetadata(queryId);
    view.set(View.QUERY_ID, Long.valueOf(queryId));
    view.set(View.APP_QUERY_ID, metadata.AppID + "_" + queryId);
    view.set(View.QUERY_START_TIME, Long.valueOf(queryMetadata.QueryStartTime));
    view.set(View.QUERY_END_TIME, Long.valueOf(queryMetadata.QueryEndTime));
    view.set(View.QUERY_WALL_CLOCK_TIME, Long.valueOf(queryMetadata.QueryEndTime - queryMetadata.QueryStartTime));
  }
  
  private String normalize(String str) {
    return str;
  }
  
  protected void setMetricsInfo(View view, List<Metric> metrics) {
    for (Metric m : metrics) {
      String metricName = m.getName();
      long metricValue = m.getValue();
      Set<String> attributes = this.metricAttributeAssociation.getAttributeNames(metricName);
      if (attributes.isEmpty())
        continue; 
      for (String attribute : attributes)
        view.set(attribute, Long.valueOf(metricValue)); 
    } 
  }
  
  protected abstract void fillMissing(Operator paramOperator);
  
  private void assignMetricValues(Operator root, Map<Long, Metric> metricMap) {
    List<Operator> postOrder = root.postOrderTraversal();
    for (Operator operator : postOrder) {
      for (Metric m : operator.getMetrics()) {
        if (metricMap.containsKey(Long.valueOf(m.getId()))) {
          Metric metricWithValue = metricMap.get(Long.valueOf(m.getId()));
          long value = metricWithValue.getValue();
          m.setValue(value);
        } 
      } 
    } 
  }
  
  public Iterator<View> enumerate() {
    return enumeratePhysicalPlan();
  }
  
  public Iterator<View> enumeratePhysicalPlan() {
    List<View> views = new ArrayList<>();
    try {
      Iterator<Application> it = this.workload.getApplications();
      while (it.hasNext()) {
        Application app = it.next();
        List<Query> queries = app.getQueries();
        for (Query q : queries) {
          if (q.PhysicalPlan != null) {
            long level = 0L;
            assignMetricValues(q.PhysicalPlan.tree, app.getMetricMap());
            fillMissing(q.PhysicalPlan.tree);
            getViewsRecursive(q.PhysicalPlan.tree, q.getQueryId(), app.metadata, views, level);
          } 
        } 
      } 
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Error in creating views", e);
    } 
    return views.iterator();
  }
  
  public Iterator<View> enumerateLogicalPlan() {
    List<View> views = new ArrayList<>();
    try {
      Iterator<Application> it = this.workload.getApplications();
      while (it.hasNext()) {
        Application app = it.next();
        List<Query> queries = app.getQueries();
        for (Query q : queries) {
          if (q.OptimizedPlan != null) {
            long level = 0L;
            getViewsRecursive(q.OptimizedPlan.tree, q.getQueryId(), app.metadata, views, level);
          } 
        } 
      } 
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Error in creating views", e);
    } 
    return views.iterator();
  }
}
