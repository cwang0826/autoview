package com.microsoft.peregrine.core.planir.preprocess.data.ir;

import com.microsoft.peregrine.core.connectors.spark.SparkSQL;
import com.microsoft.peregrine.core.planir.preprocess.data.formats.DataFrame;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFrameIR implements IR {
  protected DataFrame data;
  
  private FileIR fileInput;
  
  private String queryString;
  
  public DataFrameIR(SparkSQL sparkSQL, String dfName) {
    this.data = new DataFrame(sparkSQL, dfName);
  }
  
  public DataFrameIR(SparkSQL sparkSQL, FileIR fileInput, String dfName) {
    this.fileInput = fileInput;
    this.data = new DataFrame(sparkSQL, dfName);
  }
  
  public DataFrameIR(DataFrameIR dataFrameInput, String queryString, String tableName) {
    this.queryString = queryString;
    this.data = new DataFrame(dataFrameInput.data.getSparkSQL(), tableName);
  }
  
  public Dataset<Row> query(String params) {
    return this.data.query(params);
  }
  
  public void persist() {
    if (this.fileInput != null) {
      this.data.writeAll(this.fileInput.data);
    } else if (this.queryString != null) {
      this.data.writeAll(this.queryString);
    } 
  }
  
  public String getName() {
    return this.data.getName();
  }
  
  public long getRowCount() {
    Dataset<Row> counter = this.data.query("select count(*) from " + this.data.getName());
    long numRecords = ((Row)counter.first()).getLong(0);
    return numRecords;
  }
  
  public void summarize(String queryString) {
    String s = "SUMMARY " + getName();
    System.out.println(s);
    for (int i = 0; i < s.length(); i++)
      System.out.print("-"); 
    System.out.println();
    DataFrame summaryDF = new DataFrame(this.data.getSparkSQL(), this.data.getName() + "_summary");
    summaryDF.writeAll(queryString);
    summaryDF.showAll();
  }
  
  public void show() {
    this.data.showAll();
  }
  
  public void writeToFile(String path) {
    String separator = "|";
    boolean header = false;
    try {
      FileWriter writer = new FileWriter(path);
      if (!header) {
        Dataset<Row> headerDataset = this.data.query("SHOW COLUMNS IN " + this.data.getName());
        List<Row> list = headerDataset.collectAsList();
        for (Row row : list) {
          writer.write(row.mkString());
          writer.write(separator);
        } 
        writer.write("\n");
        header = true;
      } 
      Dataset<Row> rowDataset = this.data.query("SELECT * FROM " + this.data.getName());
      List<Row> rows = rowDataset.collectAsList();
      for (Row row : rows)
        writer.write(row.mkString(separator) + "\n"); 
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
  }
  
  public void writeToFile(String filepath, String delimiter) {
    this.data.writeToFile(filepath, delimiter);
  }
}
