package com.microsoft.peregrine.core.planir.preprocess.data.formats;

import com.microsoft.peregrine.core.connectors.jdbc.Database;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Table implements IFormat {
  protected Database db;
  
  protected String tableName;
  
  protected boolean append;
  
  public Table(Database db, String tableName) {
    this(db, tableName, false);
  }
  
  public Table(Database db, String tableName, boolean append) {
    this.db = db;
    this.tableName = tableName;
    this.append = append;
  }
  
  public String getName() {
    return this.tableName;
  }
  
  public void writeAll(Object input) {
    if (!this.append)
      cleanUp(); 
    try {
      if (input instanceof File) {
        File file = (File)input;
        this.db.insertAll(this.tableName, file
            
            .getFilepath(), file
            .getDelimiter(), file
            .getDateformat(), file
            .isFirstLineHeader());
      } else if (input instanceof String) {
        this.db.executeUpdate("SELECT * INTO " + this.tableName + " FROM (" + input + ")");
      } else {
        throw new RuntimeException("Unknown input object: " + input);
      } 
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to persist table " + this.tableName + ": " + e.getMessage());
    } 
  }
  
  public void showAll() {}
  
  public ResultSet query(String params) {
    try {
      return this.db.executeQuery(params);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run query: " + params + ". " + e.getMessage());
    } 
  }
  
  public void cleanUp() {
    try {
      this.db.truncateTable(this.tableName);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to truncate table " + this.tableName + ": " + e.getMessage());
    } 
  }
  
  public Database getDB() {
    return this.db;
  }
}
