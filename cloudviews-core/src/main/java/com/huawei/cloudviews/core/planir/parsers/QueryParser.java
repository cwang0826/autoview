package com.huawei.cloudviews.core.planir.parsers;

import com.huawei.cloudviews.core.planir.parsers.entities.Metric;
import com.huawei.cloudviews.core.planir.parsers.entities.Query;
import org.apache.log4j.Logger;

public abstract class QueryParser<Input> implements IParser<Input, Query> {
  protected static final Logger queryParserlogger = Logger.getLogger(String.valueOf(QueryParser.class));
  
  protected Metric inputQueries = new Metric(System.currentTimeMillis(), "Counter", "Input Queries", 0L);
  
  protected Metric parsedQueries = new Metric(System.currentTimeMillis(), "Counter", "Parsed Queries", 0L);
  
  protected Metric decoratedQueries = new Metric(System.currentTimeMillis(), "Counter", "Decorated Queries", 0L);
  
  public String getQueryMetrics() {
    return this.inputQueries.toString() + this.parsedQueries.toString() + this.decoratedQueries.toString();
  }
}
