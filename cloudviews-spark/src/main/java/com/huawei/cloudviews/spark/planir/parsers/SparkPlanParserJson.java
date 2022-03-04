package com.huawei.cloudviews.spark.planir.parsers;

import com.huawei.cloudviews.core.planir.parsers.PlanParser;
import com.huawei.cloudviews.core.planir.parsers.entities.Operator;
import com.huawei.cloudviews.core.planir.parsers.entities.Plan;
import com.huawei.cloudviews.spark.planir.parsers.entities.SparkOpJson;
import com.huawei.cloudviews.spark.utils.SparkTreeUtils;
import java.util.ArrayList;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SparkPlanParserJson extends PlanParser<String> {
  public Plan parse(String jsonString) {
    JSONArray jsonArray;
    try {
      jsonArray = getJsonArray(jsonString);
    } catch (RuntimeException e) {
      return (new SparkPlanParser()).parse(jsonString);
    } 
    List<SparkOpJson> preorderList = new ArrayList<>();
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = (JSONObject)jsonArray.get(i);
      SparkOpJson sparkOperator = new SparkOpJson(jsonObject);
      preorderList.add(sparkOperator);
    } 
    SparkOpJson rootOperator = SparkTreeUtils.getDagFromPreorder(preorderList);
    Plan p = new Plan();
    p.tree = rootOperator;
    return p;
  }
  
  protected Operator getOp(String jsonOpString) {
    return new SparkOpJson(jsonOpString);
  }
  
  public JSONArray getJsonArray(String jsonString) {
    JSONParser parser = new JSONParser();
    JSONArray jsonArray = null;
    try {
      jsonArray = (JSONArray)parser.parse(jsonString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    } 
    return jsonArray;
  }
}
