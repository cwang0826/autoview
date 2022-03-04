package com.huawei.cloudviews.spark.planir.parsers;

import com.huawei.cloudviews.core.planir.parsers.ApplicationParser;
import com.huawei.cloudviews.core.planir.parsers.FileParser;
import com.huawei.cloudviews.core.planir.parsers.entities.Application;
import com.huawei.cloudviews.core.planir.parsers.entities.Workload;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ApplicationLogWorkloadParser extends FileParser<Workload> {
  public Workload parse(Path dirPath) {
    return new Workload(new ApplicationLogIterator(dirPath));
  }
  
  public Workload parseStream(InputStream fileStream) {
    throw new UnsupportedOperationException("Operations supported only on directory path.");
  }
  
  public Workload parseReader(BufferedReader reader) {
    throw new UnsupportedOperationException("Operations supported only on directory path.");
  }
  
  class ApplicationLogIterator implements Iterator<Application> {
    private Iterator<File> logFiles;
    
    public ApplicationLogIterator(Path dirPath) {
      if (!Files.exists(dirPath, new java.nio.file.LinkOption[0]))
        throw new RuntimeException("Unable to locate " + dirPath); 
      this.logFiles = getLogFiles(dirPath);
    }
    
    public Iterator<File> getLogFiles(Path dirPath) {
      File directory = dirPath.toFile();
      File[] logFileArray = directory.listFiles();
      List<File> logFileList = Arrays.asList(logFileArray);
      return logFileList.iterator();
    }
    
    public boolean hasNext() {
      return this.logFiles.hasNext();
    }
    
    public Application next() {
      if (!hasNext())
        return null; 
      File currentFile = this.logFiles.next();
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(currentFile));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      } 
      ApplicationParser parser = new ApplicationLogParser(currentFile.getName());
      Application application = (Application)parser.parseReader(reader);
      return application;
    }
  }
}
