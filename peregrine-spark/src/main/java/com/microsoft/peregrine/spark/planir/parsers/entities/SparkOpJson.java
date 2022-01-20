package com.microsoft.peregrine.spark.planir.parsers.entities;

import com.microsoft.peregrine.core.planir.parsers.entities.Operator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SparkOpJson extends Operator {
  public JSONObject jsonObject;
  
  public int numChildren;
  
  public SparkOpJson(JSONObject jsonObject) {
    super(jsonObject.toJSONString());
  }
  
  public SparkOpJson(String jsonOpString) {
    super(jsonOpString);
  }
  
  private static Pattern NAME_PATTERN = Pattern.compile("([^.]+$)");
  
  private static Pattern HTS_PATTERN = Pattern.compile(".*HTS:(\\d+).*");
  
  private static Pattern HT_PATTERN = Pattern.compile(".*HT:(\\d+).*");
  
  public String toString() {
    return super.toString();
  }
  
  protected void parseOpInfo(String jsonOpString) {
    JSONParser parser = new JSONParser();
    JSONObject jsonObject = null;
    try {
      jsonObject = (JSONObject)parser.parse(jsonOpString);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    } 
    String canonicalName = (String)jsonObject.get("class");
    String className = extractPattern(canonicalName, NAME_PATTERN);
    this.name = className;
    this.opInfo = jsonOpString;
    this.jsonObject = jsonObject;
    this.numChildren = Math.toIntExact(((Long)jsonObject.get("num-children")).longValue());
    String signatures = (String)jsonObject.get("signatures");
    if (signatures != null) {
      String hts = extractPattern(signatures, HTS_PATTERN);
      setHTS(hts);
      String ht = extractPattern(signatures, HT_PATTERN);
      setHT(ht);
    } else {
      setHTS((String)jsonObject.get("hts"));
      setHT((String)jsonObject.get("ht"));
    } 
  }
  
  private String extractPattern(String hay, Pattern needle) {
    Matcher matcher = needle.matcher(hay);
    if (matcher.find())
      return matcher.group(1); 
    return null;
  }
}
