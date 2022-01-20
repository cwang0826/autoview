package com.microsoft.peregrine.core.planir.preprocess.data.formats;

import com.microsoft.peregrine.core.connectors.spark.SparkSQL;
import com.microsoft.peregrine.core.planir.preprocess.entities.View;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFrame implements IFormat {
  protected SparkSQL sparkSQL;
  
  protected String dfName;
  
  public DataFrame(SparkSQL sparkSQL, String dfName) {
    this.sparkSQL = sparkSQL;
    this.dfName = dfName;
  }
  
  public String getName() {
    return this.dfName;
  }
  
  public void writeAll(Object input) {
    if (input instanceof File) {
      File file = (File)input;
      this.sparkSQL.createDataFrame(this.dfName, file
          
          .getFilepath(), file
          .getDelimiter(), View.class);
    } else if (input instanceof String) {
      this.sparkSQL.writeDataFrame((String)input, this.dfName);
    } else {
      throw new RuntimeException("Unknown input object: " + input);
    } 
  }
  
  public void showAll() {
    System.out.println("Displaying " + getName());
    this.sparkSQL.executeAndShow("SELECT * FROM " + this.dfName);
  }
  
  public Dataset<Row> query(String query) {
    return this.sparkSQL.execute(query);
  }
  
  public void cleanUp() {
    this.sparkSQL.execute("DROP VIEW " + this.dfName);
  }
  
  public SparkSQL getSparkSQL() {
    return this.sparkSQL;
  }
  
  public void writeToFile(String filepath, String delimiter) {
    this.sparkSQL.writeDataFrame(this.sparkSQL.execute("SELECT * FROM " + this.dfName), filepath, delimiter);
  }
}
