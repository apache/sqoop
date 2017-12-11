package org.apache.sqoop.hive;

import java.io.IOException;

public interface HiveClient {

  void importTable() throws IOException;

  void createTable() throws IOException;
}
