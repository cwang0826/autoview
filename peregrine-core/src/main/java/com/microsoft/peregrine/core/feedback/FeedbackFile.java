package com.huawei.cloudviews.core.feedback;

import com.huawei.cloudviews.core.feedback.annotations.Annotation;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FeedbackFile extends AbstractFeedback {
  private Map<String, List<Annotation>> annotationMap;
  
  public void initialize(InputStream input) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      this.annotationMap = new HashMap<>();
      String line;
      while ((line = reader.readLine()) != null) {
        Annotation a = Annotation.getInstance(line);
        if (!this.annotationMap.containsKey(a.getRecurringSignature()))
          this.annotationMap.put(a.getRecurringSignature(), new ArrayList<>()); 
        ((List<Annotation>)this.annotationMap.get(a.getRecurringSignature())).add(a);
      } 
      reader.close();
      for (String key : this.annotationMap.keySet())
        Collections.sort(this.annotationMap
            .get(key), 
            Comparator.comparingInt(Annotation::getPriority)); 
    } catch (IOException e) {
      throw new RuntimeException("Cannot read from the annotation filepath " + e.getMessage());
    } 
  }
  
  public void update(List<Annotation> annotations, OutputStream output) {
    try {
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output));
      for (Annotation a : annotations) {
        String annStr = a.toString().replace("wasb:/", "wasb:///");
        annStr = annStr.replace("hdfs:/", "hdfs:///");
        writer.write(annStr);
        writer.newLine();
      } 
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException("Cannot write to the annotation filepath " + e.getMessage());
    } 
  }
  
  public Collection<List<Annotation>> getAllAnnotations() {
    return this.annotationMap.values();
  }
  
  public List<Annotation> get(String identifier) {
    return this.annotationMap.get(identifier);
  }
  
  public static InputStream getInputStream(String inputPath, String location) {
    InputStream input = null;
    try {
      Configuration conf;
      FileSystem fs;
      switch (location) {
        case "local":
          input = Files.newInputStream(Paths.get(inputPath, new String[0]), new java.nio.file.OpenOption[0]);
          return input;
        case "distributed":
          conf = new Configuration();
          fs = FileSystem.get(conf);
          return (InputStream)fs.open(new Path(inputPath));
      } 
      throw new RuntimeException("Invalid location for feedback file: " + location);
    } catch (IOException e) {
      throw new RuntimeException("Cannot read from annotation filepath " + e.getMessage());
    } 
  }
  
  public static OutputStream getOutputStream(String outputPath, String location) {
    try {
      OutputStream output;
      Configuration conf;
      FileSystem fs;
      switch (location) {
        case "local":
          output = Files.newOutputStream(Paths.get(outputPath, new String[0]), new java.nio.file.OpenOption[0]);
          return output;
        case "distributed":
          conf = new Configuration();
          fs = FileSystem.get(conf);
          return (OutputStream)fs.create(new Path(outputPath));
      } 
      throw new RuntimeException("Invalid location for feedback file: " + location);
    } catch (IOException e) {
      throw new RuntimeException("Cannot write to the annotation filepath " + e.getMessage());
    } 
  }
}
