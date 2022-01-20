package com.microsoft.peregrine.core.connectors.kusto;

import com.microsoft.peregrine.core.config.legacy.Config;
import com.microsoft.peregrine.core.config.legacy.PropertyConfig;
import com.microsoft.peregrine.core.connectors.cmd.Command;
import java.nio.file.Paths;

public class KustoCli {
  private Config conf = PropertyConfig.getInstance("connectors.properties");
  
  private String cliPath;
  
  public KustoCli() {
    this.cliPath = this.conf.get("kusto_cli_path");
  }
  
  public void run(String dataSource, String database, String query, String outputPath) {
    String connStr = "https://" + dataSource + ".kusto.windows.net/;Fed=true;database=" + database;
    String cmd = Paths.get(this.cliPath, new String[0]) + " \"" + connStr + "\" -execute:\"#save " + outputPath + "\" -execute:\"" + query + "\"";
    Command.run(cmd);
  }
}
