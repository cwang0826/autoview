package com.microsoft.peregrine.core.connectors.adls;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class ADLSCommands {
  public ADLStoreClient getCLient(String accountFQDN, String clientId, String authTokenEndpoint, String clientKey) {
    ClientCredsTokenProvider clientCredsTokenProvider = new ClientCredsTokenProvider(authTokenEndpoint, clientId, clientKey);
    ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, (AccessTokenProvider)clientCredsTokenProvider);
    return client;
  }
  
  public void createDirectory(ADLStoreClient client, String path) {
    try {
      client.createDirectory(path);
    } catch (IOException e) {
      e.printStackTrace();
    } 
  }
  
  public void upload(ADLStoreClient client, String sourceFilename, String targetFilename) {
    try {
      ADLFileOutputStream aDLFileOutputStream = client.createFile(targetFilename, IfExists.OVERWRITE);
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter((OutputStream)aDLFileOutputStream));
      BufferedReader reader = new BufferedReader(new FileReader(sourceFilename));
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line);
        writer.newLine();
      } 
      reader.close();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    } 
  }
}
