package com.huawei.cloudviews.core.optimizations.multiquery.viewselection;

import com.huawei.cloudviews.core.config.legacy.Config;
import com.huawei.cloudviews.core.feedback.annotations.Annotation;
import com.huawei.cloudviews.core.feedback.annotations.MaterializeAnnotation;
import com.huawei.cloudviews.core.feedback.annotations.ReuseAnnotation;
import com.huawei.cloudviews.core.optimizations.IOptimization;
import com.huawei.cloudviews.core.planir.preprocess.data.ir.IR;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public abstract class ViewSelection<E extends IR> implements IOptimization<E, E> {
  protected E selectedViews;
  
  protected Config conf;
  
  public ViewSelection(Config conf) {
    this.conf = conf;
  }
  
  public List<Annotation> getAnnotations(String materializePath) {
    List<Annotation> annotations = new ArrayList<>();
    for (Pair<String, String> prop : getSignatureProps(this.selectedViews)) {
      String recurringHash = (String)prop.getLeft();
      String viewPath = Paths.get(materializePath, new String[] { recurringHash }).toString();
      String viewProps = StringUtils.isEmpty((CharSequence)prop.getRight()) ? viewPath : ((String)prop.getRight() + "," + viewPath);
      annotations.add(new MaterializeAnnotation(recurringHash, viewProps));
      annotations.add(new ReuseAnnotation(recurringHash, viewPath));
    } 
    return annotations;
  }
  
  public E getResult() {
    return this.selectedViews;
  }
  
  protected abstract List<Pair<String, String>> getSignatureProps(E paramE);
}
