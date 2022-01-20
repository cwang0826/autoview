package com.microsoft.peregrine.core.connectors.cmd;

import com.microsoft.peregrine.core.connectors.jdbc.AzureSQL;
import com.microsoft.peregrine.core.connectors.jdbc.Database;
import java.sql.SQLException;

public class Console {
  private static final String viewsMasterTable = "views_master";
  
  private enum Backend {
    AZURE_SQL;
  }
  
  protected int currentArg = 0;
  
  private Database getDB(Backend backend) {
    switch (backend) {
      case AZURE_SQL:
        return new AzureSQL();
    } 
    throw new RuntimeException("Backend not supported: " + backend);
  }
  
  protected String getNextArg(String[] args, String validArg) {
    if (this.currentArg >= args.length) {
      System.out.println("Expected: " + validArg);
      throw new RuntimeException("Too few arguments!");
    } 
    return args[this.currentArg++];
  }
  
  protected void runCommand(String[] args) {
    Database db = getDB(Backend.AZURE_SQL);
    try {
      String cmd = getNextArg(args, "uploadViews|createViewTable|dropViewTable|console");
      switch (cmd) {
        case "uploadViews":
          db.insertAll(
              getNextArg(args, "<targetTableName>"), 
              getNextArg(args, "<sourceFileName>"), 
              getNextArg(args, "<separater>"), 
              getNextArg(args, "<dateformat>"), 
              Boolean.parseBoolean(getNextArg(args, "<firstLineIsFieldNames>")));
          System.out.println("Success!");
          break;
        case "createViewTable":
          db.copyTable(getNextArg(args, "<tableName>"), "views_master");
          System.out.println("Success!");
          break;
        case "dropViewTable":
          db.dropTable(getNextArg(args, "<tableName>"));
          System.out.println("Success!");
          break;
        default:
          throw new RuntimeException("Invalid command: " + cmd);
      } 
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      db.closeConnection();
    } 
  }
  
  public static void run(String... args) {
    (new Console()).runCommand(args);
  }
  
  public static void main(String[] args) {
    (new Console()).runCommand(args);
  }
}
