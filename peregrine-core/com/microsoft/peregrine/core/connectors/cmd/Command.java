package com.microsoft.peregrine.core.connectors.cmd;

import java.io.IOException;

public class Command {
  public static void run(String cmd) {
    try {
      Runtime rt = Runtime.getRuntime();
      Process pr = rt.exec(cmd);
      pr.waitFor();
    } catch (IOException e) {
      throw new RuntimeException("Failed to run command: " + cmd + System.lineSeparator() + e.getMessage());
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to run command: " + cmd + System.lineSeparator() + e.getMessage());
    } 
  }
}
