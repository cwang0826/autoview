package com.microsoft.peregrine.core.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public abstract class FSUtils {
  private static FSUtils currentFS;
  
  public static boolean exists(String path) {
    return getFS().doExists(path);
  }
  
  public static boolean delete(String path) {
    return getFS().doDelete(path);
  }
  
  public static boolean lock(String path, long expiryMillis) {
    return getFS().doLock(path, expiryMillis);
  }
  
  public static boolean create(String path) {
    return getFS().doCreate(path);
  }
  
  public abstract boolean doExists(String paramString);
  
  public abstract boolean doDelete(String paramString);
  
  public abstract boolean doLock(String paramString, long paramLong);
  
  public abstract boolean doCreate(String paramString);
  
  public static void setFS(String fsType) {
    switch (fsType) {
      case "wasb":
        currentFS = new DefaultDistributedFSUtils();
        return;
      case "hdfs":
        currentFS = new HdfsUtils();
        return;
      case "file":
        currentFS = new LocalFSUtils();
        return;
    } 
    currentFS = new DefaultDistributedFSUtils();
  }
  
  public static FSUtils getFS() {
    return currentFS;
  }
  
  public static class HdfsUtils extends FSUtils {
    public boolean doExists(String path) {
      Configuration conf = new Configuration();
      boolean retValue = false;
      try {
        FileSystem fs = FileSystem.get(conf);
        retValue = fileExists(path);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
      return retValue;
    }
    
    private boolean fileExists(String path) {
      String command = "hadoop fs -ls " + path;
      int exitCode = 1;
      try {
        Process process = Runtime.getRuntime().exec(command);
        StringBuilder output = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null)
          output.append(line + "\n"); 
        exitCode = process.waitFor();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
      return (exitCode == 0);
    }
    
    public boolean doDelete(String path) {
      return false;
    }
    
    public boolean doLock(String path, long expiryMillis) {
      return false;
    }
    
    public boolean doCreate(String path) {
      return false;
    }
  }
  
  public static class DefaultDistributedFSUtils extends FSUtils {
    public boolean doExists(String path) {
      Configuration conf = new Configuration();
      boolean retValue = false;
      try {
        FileSystem fs = FileSystem.get(conf);
        retValue = fs.exists(new org.apache.hadoop.fs.Path(path));
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
      return retValue;
    }
    
    public boolean doDelete(String path) {
      if (!doExists(path))
        return true; 
      Configuration conf = new Configuration();
      boolean retValue = false;
      try {
        FileSystem fs = FileSystem.get(conf);
        retValue = fs.delete(new org.apache.hadoop.fs.Path(path), true);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
      return retValue;
    }
    
    public boolean doLock(String path, long expiryMillis) {
      if (doExists(path))
        return false; 
      return doCreate(path);
    }
    
    public boolean doCreate(String path) {
      Configuration conf = new Configuration();
      boolean retValue = false;
      try {
        FileSystem fs = FileSystem.get(conf);
        retValue = fs.createNewFile(new org.apache.hadoop.fs.Path(path));
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } 
      return retValue;
    }
  }
  
  public static class LocalFSUtils extends FSUtils {
    public boolean doExists(String path) {
      return Files.exists(Paths.get(path, new String[0]).toAbsolutePath(), new java.nio.file.LinkOption[0]);
    }
    
    public boolean doDelete(String path) {
      try {
        File file = new File(path);
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          file.delete();
        } 
        return true;
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      } 
    }
    
    public boolean doLock(String path, long expiryMillis) {
      try {
        if (doExists(path))
          if (Files.getLastModifiedTime(Paths.get(path, new String[0]), new java.nio.file.LinkOption[0]).compareTo(
              FileTime.fromMillis(System.currentTimeMillis() - expiryMillis)) < 0) {
            doDelete(path);
          } else {
            return false;
          }  
        Path lockPath = Paths.get(path, new String[0]);
        Files.createFile(lockPath, (FileAttribute<?>[])new FileAttribute[0]);
        return true;
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      } 
    }
    
    public boolean doCreate(String path) {
      Path output = null;
      try {
        output = Files.createDirectories(Paths.get(path, new String[0]), (FileAttribute<?>[])new FileAttribute[0]);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } 
      return (output != null);
    }
  }
}
