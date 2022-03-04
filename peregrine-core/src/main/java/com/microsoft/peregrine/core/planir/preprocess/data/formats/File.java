package com.huawei.cloudviews.core.planir.preprocess.data.formats;

import com.huawei.cloudviews.core.planir.preprocess.entities.View;
import com.huawei.cloudviews.core.planir.preprocess.enumerators.IEnumerator;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class File implements IFormat {
  private String filepath;
  
  private String delimiter;
  
  private String dateformat;
  
  public File(String filepath, String delimiter, String dateformat) {
    this.filepath = filepath;
    this.delimiter = delimiter;
    this.dateformat = dateformat;
  }
  
  public void writeAll(Object input) {
    IEnumerator<View> enumerator = (IEnumerator<View>)input;
    View.delimiter = this.delimiter;
    FileWriter fw = null;
    try {
      fw = new FileWriter(this.filepath);
      BufferedWriter writer = new BufferedWriter(fw);
      CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.newFormat(this.delimiter.charAt(0)).withQuote('"').withRecordSeparator(System.lineSeparator()));
      boolean header = true;
      for (Iterator<View> it = enumerator.enumerate(); it.hasNext(); ) {
        View view = it.next();
        if (header) {
          csvPrinter.printRecord(view.getHeader());
          header = false;
        } 
        csvPrinter.printRecord(view.getRecord());
      } 
      csvPrinter.close();
      writer.close();
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to update file " + this.filepath + ": " + e.getMessage());
    } 
  }
  
  public void showAll() {}
  
  public File query(String params) {
    throw new UnsupportedOperationException("Querying files not supported currently.");
  }
  
  public void cleanUp() {
    try {
      Files.delete(Paths.get(this.filepath, new String[0]));
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete file " + this.filepath + ": " + e.getMessage());
    } 
  }
  
  public String getName() {
    java.io.File file = new java.io.File(this.filepath);
    return file.getName();
  }
  
  public String getFilepath() {
    return this.filepath;
  }
  
  public String getDelimiter() {
    return this.delimiter;
  }
  
  public String getDateformat() {
    return this.dateformat;
  }
  
  public boolean isFirstLineHeader() {
    return false;
  }
}
