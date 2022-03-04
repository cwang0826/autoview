package com.huawei.cloudviews.core.planir.preprocess.data.ir;

import com.huawei.cloudviews.core.planir.preprocess.data.formats.File;
import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.IEnumerator;
import java.io.BufferedReader;
import java.io.FileReader;

public class FileIR implements IR {
  protected File data;
  
  private IEnumerator<View> enumerator;
  
  public FileIR(DataFrameIR dataFrameIR, String filepath, String delimiter) {}
  
  public FileIR(String filepath, String delimiter) {
    this(filepath, delimiter, View.defaultDateFormat);
  }
  
  public FileIR(String filepath, String delimiter, String dateformat) {
    this.data = new File(filepath, delimiter, dateformat);
  }
  
  public FileIR(IEnumerator<View> enumerator, String filepath, String delimiter) {
    this(enumerator, filepath, delimiter, View.defaultDateFormat);
  }
  
  public FileIR(IEnumerator<View> enumerator, String filepath, String delimiter, String dateformat) {
    this.enumerator = enumerator;
    this.data = new File(filepath, delimiter, dateformat);
  }
  
  public File query(String params) {
    return this.data.query(params);
  }
  
  public void persist() {
    if (this.enumerator != null)
      this.data.writeAll(this.enumerator); 
  }
  
  public String getName() {
    return this.data.getName();
  }
  
  public long getRowCount() {
    long counter = 0L;
    try {
      BufferedReader br = new BufferedReader(new FileReader(this.data.getFilepath()));
      String line = null;
      while ((line = br.readLine()) != null)
        counter++; 
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } 
    return counter;
  }
  
  public String getFilepath() {
    return this.data.getFilepath();
  }
  
  public String getDelimiter() {
    return this.data.getDelimiter();
  }
  
  public void summarize(String params) {
    throw new UnsupportedOperationException("summary not supported on files.");
  }
  
  public void show() {
    this.data.showAll();
  }
}
