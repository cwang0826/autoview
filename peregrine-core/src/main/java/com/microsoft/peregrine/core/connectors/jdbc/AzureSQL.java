package com.microsoft.peregrine.core.connectors.jdbc;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.config.legacy.PropertyConfig;
import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.DataFrameReader;

public class AzureSQL extends Database {
  private Config conf = PropertyConfig.getInstance("connectors.properties");
  
  private String hostName;
  
  private String dbName;
  
  private String user;
  
  private String password;
  
  public AzureSQL() {
    this.hostName = this.conf.get("azuresql_hostname");
    this.dbName = this.conf.get("azuresql_dbname");
    this.user = this.conf.get("azuresql_user");
    this.password = this.conf.get("azuresql_password");
  }
  
  protected String getConnectionString() {
    return String.format("jdbc:sqlserver://%s:1433;database=%s;user=%s;password=%s;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;", new Object[] { this.hostName, this.dbName, this.user, this.password });
  }
  
  public DataFrameReader decorateDataFrameReader(DataFrameReader dfReader) {
    return dfReader.option("url", getConnectionString());
  }
  
  protected String getBulkLoadStatement(String targetTableName, String sourceFilePath, String delimiter) {
    return String.format("BULK INSERT %s FROM '%s' WITH (FIELDTERMINATOR='%c', ROWTERMINATOR='\n')", new Object[] { targetTableName, sourceFilePath, delimiter });
  }
  
  public void bulkLoad(String targetTableName, String sourceFilePath, String delimiter) throws SQLException {
    SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(sourceFilePath, null, String.valueOf(delimiter), true);
    List<Integer> columnTypes = getColumnTypes(targetTableName);
    for (int i = 0; i < columnTypes.size(); i++) {
      if (((Integer)columnTypes.get(i)).intValue() == 2) {
        fileRecord.addColumnMetadata(i + 1, null, ((Integer)columnTypes.get(i)).intValue(), 38, 16);
      } else {
        fileRecord.addColumnMetadata(i + 1, null, ((Integer)columnTypes.get(i)).intValue(), 50, 0);
      } 
    } 
    SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
    copyOptions.setBatchSize(300000);
    copyOptions.setTableLock(true);
    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(getConnection());
    bulkCopy.setBulkCopyOptions(copyOptions);
    bulkCopy.setDestinationTableName(targetTableName);
    bulkCopy.writeToServer((ISQLServerBulkRecord)fileRecord);
  }
  
  public String getInsertStatement(String targetTableName) throws SQLException {
    List<String> columnNames = getColumnNames(targetTableName);
    List<String> placeholders = new ArrayList<>();
    for (String columnName : columnNames)
      placeholders.add("?"); 
    return "INSERT INTO " + targetTableName + " (" + String.join(",", (Iterable)columnNames) + ") values (" + String.join(",", (Iterable)placeholders) + ")";
  }
  
  protected String getCopyTableStatement(String targetTableName, String sourceTableName) {
    return String.format("SELECT * INTO %s FROM %s", new Object[] { targetTableName, sourceTableName });
  }
  
  protected String getDropTableStatament(String tableName) {
    return String.format("DROP TABLE IF EXISTS %s", new Object[] { tableName });
  }
  
  protected String getTruncateTableStatament(String tableName) {
    return String.format("IF OBJECT_ID('%s') IS NOT NULL TRUNCATE TABLE %s", new Object[] { tableName, tableName });
  }
}
