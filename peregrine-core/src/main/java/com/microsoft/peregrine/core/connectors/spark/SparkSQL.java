package com.microsoft.peregrine.core.connectors.spark;

import com.microsoft.peregrine.core.connectors.jdbc.Database;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQL {
  private SparkSession spark;
  
  private final int NUM_ROWS_SHOW_OPTION = 10;
  
  private final boolean TRUNCATE_SHOW_OPTION = false;
  
  public SparkSQL(SparkSession spark) {
    this.spark = spark;
  }
  
  public SparkSQL() {
    this("");
  }
  
  public SparkSQL(String extensionsClassName) {
    this("DefaultApp", "local", extensionsClassName);
  }
  
  public SparkSQL(String appName, String master, String extensionsClassName) {
    this.spark = getSession(appName, master, extensionsClassName);
  }
  
  public void closeSession() {
    if (this.spark != null)
      this.spark.close(); 
  }
  
  public final SparkSession getCurrentSparkSession() {
    return this.spark;
  }
  
  private SparkSession getSession(String appName, String master, String extensionsClassName) {
    if (StringUtils.isEmpty(extensionsClassName))
      return 
        SparkSession.builder()
        .appName(appName)
        .master(master)
        .getOrCreate(); 
    return 
      SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.extensions", extensionsClassName)
      .getOrCreate();
  }
  
  private StructType getSchema(Class cls) {
    StructType schema = new StructType();
    Field[] allFields = cls.getDeclaredFields();
    Field[] arrayOfField1 = allFields;
    int i = arrayOfField1.length;
    byte b = 0;
    while (true) {
      if (b < i) {
        Field field = arrayOfField1[b];
        if (!Modifier.isStatic(field.getModifiers())) {
          DataType type;
          String name = field.getName();
          Class<?> fieldCls = field.getType();
          if (fieldCls.equals(long.class)) {
            type = DataTypes.LongType;
          } else if (fieldCls.equals(int.class)) {
            type = DataTypes.IntegerType;
          } else if (fieldCls.equals(double.class)) {
            type = DataTypes.DoubleType;
          } else if (fieldCls.equals(float.class)) {
            type = DataTypes.FloatType;
          } else if (fieldCls.equals(String.class)) {
            type = DataTypes.StringType;
          } else if (fieldCls.equals(boolean.class)) {
            type = DataTypes.BooleanType;
          } else if (fieldCls.equals(Date.class)) {
            type = DataTypes.DateType;
          } else {
            System.out.println("Unknown field type: " + fieldCls.getName());
            b++;
	    continue;
          } 
          schema = schema.add(name, type, true);
        } 
      } else {
        break;
      } 
      b++;
    } 
    return schema;
  }
  
  private StructType getSchema(String schemaString) {
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    } 
    return DataTypes.createStructType(fields);
  }
  
  public void createDataFrame(String name, String filepath, String delimiter, Class schemaObject) {
    Dataset<Row> df = this.spark.read().option("delimiter", delimiter).option("inferSchema", "true").option("header", "true").csv(filepath);
    df.createOrReplaceTempView(name);
  }
  
  public void createDataFrame(String name, String filepath, String delimiter, String schemaString) {
    createDataFrame(name, filepath, delimiter, getSchema(schemaString));
  }
  
  public void createDataFrame(String name, String filepath, String delimiter, StructType schema) {
    Dataset<Row> df = createDataFrame(filepath, delimiter, schema, "csv");
    df.createOrReplaceTempView(name);
  }
  
  public Dataset<Row> createDataFrame(String filepath, String delimiter, StructType schema, String format) {
    switch (format) {
      case "csv":
        return this.spark.read().option("delimiter", delimiter).schema(schema).csv(filepath);
      case "parquet":
        return this.spark.read().schema(schema).parquet(filepath);
    } 
    throw new RuntimeException("Format not supported for materialized view: " + format);
  }
  
  public Dataset<Row> createDataFrame(String filepath, String format) {
    switch (format) {
      case "parquet":
        return this.spark.read().parquet(filepath);
    } 
    throw new RuntimeException("Format not supported for materialized view: " + format);
  }
  
  public Dataset<Row> createDataFrame(LogicalPlan plan) {
    return Dataset.ofRows(this.spark, plan);
  }
  
  public Dataset<Row> createDataFrame(List<String> objects) {
    return this.spark.createDataset(objects, Encoders.STRING()).toDF();
  }
  
  public void createDataFrame(String name, Database db, String tableName) {
    Dataset<Row> df = db.decorateDataFrameReader(this.spark.read().format("jdbc")).option("dbtable", tableName).load();
    df.createOrReplaceTempView(name);
  }
  
  public Dataset<Row> execute(String query) {
    return this.spark.sql(query);
  }
  
  public void printCatalog() {
    this.spark.catalog().listDatabases().show(10, false);
    this.spark.catalog().listTables().show(10, false);
  }
  
  public void executeAndShow(String query) {
    execute(query).show(10, false);
  }
  
  public Dataset<Row> readDataFrame(String tableName) {
    return this.spark.sql("SELECT * FROM " + tableName);
  }
  
  public void writeDataFrame(String query, String tableName) {
    this.spark.sql("CREATE TEMPORARY VIEW " + tableName + " AS " + query);
  }
  
  public void writeDataFrame(Dataset<Row> dataFrame, String tableName) {
    dataFrame.write().mode("append").saveAsTable(tableName);
  }
  
  public void writeDataFrame(Dataset<Row> df, String filepath, String delimiter) {
    writeDataFrame(df, filepath, delimiter, "csv");
  }
  
  public void writeDataFrame(Dataset<Row> df, String filepath, String delimiter, String format) {
    switch (format) {
      case "csv":
        df.write().format("csv").option("delimiter", delimiter).save(filepath);
        return;
      case "parquet":
        df.write().parquet(filepath);
        return;
    } 
    throw new RuntimeException("Format not supported: " + format);
  }
  
  public String getProperty(String key) {
    return this.spark.conf().contains(key) ? this.spark.conf().get(key) : null;
  }
  
  public void setProperty(String key, String value) {
    this.spark.conf().set(key, value);
  }
}
