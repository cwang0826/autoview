package com.microsoft.peregrine.core.connectors.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;

public abstract class Database {
  protected static final Logger dblogger = Logger.getLogger(String.valueOf(Database.class));
  
  private Connection connection;
  
  protected abstract String getConnectionString();
  
  protected abstract String getBulkLoadStatement(String paramString1, String paramString2, String paramString3);
  
  protected abstract String getCopyTableStatement(String paramString1, String paramString2);
  
  protected abstract String getInsertStatement(String paramString) throws SQLException;
  
  protected abstract String getDropTableStatament(String paramString);
  
  protected abstract String getTruncateTableStatament(String paramString);
  
  public abstract DataFrameReader decorateDataFrameReader(DataFrameReader paramDataFrameReader);
  
  Connection getConnection() throws SQLException {
    if (this.connection != null)
      return this.connection; 
    this.connection = DriverManager.getConnection(getConnectionString());
    return this.connection;
  }
  
  public String getSchema() throws SQLException {
    return getConnection().getSchema();
  }
  
  public List<Integer> getColumnTypes(String tableName) throws SQLException {
    ResultSet rsColumns = getConnection().getMetaData().getColumns(null, null, tableName, null);
    List<Integer> colTypeList = new ArrayList<>();
    while (rsColumns.next())
      colTypeList.add(Integer.valueOf(rsColumns.getInt("DATA_TYPE"))); 
    return colTypeList;
  }
  
  public List<String> getColumnNames(String tableName) throws SQLException {
    ResultSet rsColumns = getConnection().getMetaData().getColumns(null, null, tableName, null);
    List<String> colNameList = new ArrayList<>();
    while (rsColumns.next())
      colNameList.add(rsColumns.getString("COLUMN_NAME")); 
    return colNameList;
  }
  
  public ResultSet executeQuery(String query) throws SQLException {
    return getConnection().createStatement().executeQuery(query);
  }
  
  public void executeUpdate(String updateStmt) throws SQLException {
    getConnection().createStatement().executeUpdate(updateStmt);
  }
  
  public void dropTable(String tableName) throws SQLException {
    executeUpdate(getDropTableStatament(tableName));
  }
  
  public void truncateTable(String tableName) throws SQLException {
    executeUpdate(getTruncateTableStatament(tableName));
  }
  
  public void bulkLoad(String targetTableName, String sourceFilePath, String delimiter) throws SQLException {
    executeUpdate(getBulkLoadStatement(targetTableName, sourceFilePath, delimiter));
  }
  
  public void insertAll(String targetTableName, String sourceFilePath, String delimiter, String dateFormat) throws SQLException {
    insertAll(targetTableName, sourceFilePath, delimiter, dateFormat, false);
  }
  
  public void insertAll(String targetTableName, String sourceFilePath, String delimiter, String dateFormat, boolean firstLineIColumnNames) throws SQLException {
    char delimiterChar = StringEscapeUtils.unescapeJava(delimiter).charAt(0);
    dblogger.debug("Insert Command params: targetTableName=" + targetTableName + ", sourceFilePath=" + sourceFilePath + ", delimiter=" + 

        
        StringEscapeUtils.escapeJava(delimiter + "") + ", dateFormat=" + dateFormat + ", firstLineIColumnNames=" + firstLineIColumnNames);
    PreparedStatement preparedStatement = getConnection().prepareStatement(getInsertStatement(targetTableName));
    List<Integer> columnTypes = getColumnTypes(targetTableName);
    MathContext mctxt = new MathContext(20);
    BigDecimal maxBD = new BigDecimal(1.0E20D);
    BigDecimal minBD = new BigDecimal(-1.0E20D);
    Path path = Paths.get(sourceFilePath, new String[0]);
    try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {
      CSVFormat fmt = CSVFormat.newFormat(delimiterChar).withQuote('"').withRecordSeparator(System.lineSeparator());
      if (firstLineIColumnNames)
        fmt = fmt.withFirstRecordAsHeader(); 
      Iterator<CSVRecord> iterator = (new CSVParser(reader, fmt)).iterator();
      int insertCount = 0;
      while (iterator.hasNext()) {
        CSVRecord attributes = iterator.next();
        for (int i = 0; i < attributes.size(); i++) {
          Date d;
          int intVal;
          long longVal;
          float sel;
          switch (((Integer)columnTypes.get(i)).intValue()) {
            case 93:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, 93);
                break;
              } 
              d = new Date((new SimpleDateFormat(dateFormat)).parse(attributes.get(i)).getTime());
              preparedStatement.setDate(i + 1, d);
              break;
            case 2:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, 2);
                break;
              } 
              try {
                BigDecimal bd = new BigDecimal(attributes.get(i), mctxt);
                if (bd.compareTo(maxBD) > 0 || bd.compareTo(minBD) < 0) {
                  preparedStatement.setNull(i + 1, 2);
                  break;
                } 
                preparedStatement.setBigDecimal(i + 1, bd);
              } catch (NumberFormatException e) {
                System.out.println("Exception");
              } 
              break;
            case 4:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, 4);
                break;
              } 
              intVal = Integer.parseInt(attributes.get(i));
              preparedStatement.setInt(i + 1, intVal);
              break;
            case -5:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, -5);
                break;
              } 
              longVal = Long.parseLong(attributes.get(i));
              preparedStatement.setLong(i + 1, longVal);
              break;
            case 8:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, 8);
                break;
              } 
              sel = Float.parseFloat(attributes.get(i));
              if (Float.isInfinite(sel) || Float.isNaN(sel)) {
                preparedStatement.setNull(i + 1, 8);
                break;
              } 
              preparedStatement.setFloat(i + 1, sel);
              break;
            case 12:
              if (attributes.get(i).isEmpty()) {
                preparedStatement.setNull(i + 1, 12);
                break;
              } 
              preparedStatement.setString(i + 1, attributes.get(i));
              break;
            default:
              throw new UnsupportedOperationException();
          } 
        } 
        preparedStatement.executeUpdate();
        insertCount++;
      } 
      preparedStatement.close();
      dblogger.debug("Inserted: " + insertCount + " Rows");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      throw e;
    } 
  }
  
  public void copyTable(String targetTableName, String sourceTableName) throws SQLException {
    executeUpdate(getCopyTableStatement(targetTableName, sourceTableName));
  }
  
  public int executeIntAgg(String query) {
    try {
      ResultSet rs = executeQuery(query);
      rs.next();
      return rs.getInt(1);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("Could not run the int aggregate query: " + e.getMessage());
    } 
  }
  
  public int[] executeIntAggs(String query) {
    try {
      ResultSet rs = executeQuery(query);
      rs.next();
      int[] aggs = new int[rs.getMetaData().getColumnCount()];
      for (int i = 0; i < aggs.length; i++)
        aggs[i] = rs.getInt(i + 1); 
      return aggs;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("Could not run the aggregate query: " + e.getMessage());
    } 
  }
  
  public String[] executeAggs(String query) {
    try {
      ResultSet rs = executeQuery(query);
      rs.next();
      String[] aggs = new String[rs.getMetaData().getColumnCount()];
      for (int i = 0; i < aggs.length; i++)
        aggs[i] = rs.getMetaData().getColumnName(i + 1) + " = " + rs.getInt(i + 1); 
      return aggs;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("Could not run the aggregate query: " + e.getMessage());
    } 
  }
  
  public void closeConnection() {
    if (this.connection != null)
      try {
        this.connection.close();
      } catch (SQLException sQLException) {} 
  }
}
