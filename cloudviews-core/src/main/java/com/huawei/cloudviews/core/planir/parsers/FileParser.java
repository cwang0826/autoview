package com.huawei.cloudviews.core.planir.parsers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class FileParser<Entity> implements IParser<Path, Entity> {
  public Entity parse(Path path) {
    try {
      return parseStream(Channels.newInputStream(Files.newByteChannel(path, new java.nio.file.OpenOption[0])));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Cannot create an input stream: " + e.getMessage());
    } 
  }
  
  public Entity parseStream(InputStream fileStream) {
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    return parseReader(new BufferedReader(new InputStreamReader(fileStream, decoder)));
  }
  
  public abstract Entity parseReader(BufferedReader paramBufferedReader);
}
