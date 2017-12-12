package org.apache.sqoop.hive;

import java.io.IOException;

public class HiveImportToHiveClientAdapter implements HiveClient {

  private final HiveImport hiveImport;

  private final String inputTableName;

  private final String outputTableName;

  public HiveImportToHiveClientAdapter(HiveImport hiveImport, String inputTableName, String outputTableName) {
    this.hiveImport = hiveImport;
    this.inputTableName = inputTableName;
    this.outputTableName = outputTableName;
  }

  @Override
  public void importTable() throws IOException {
    hiveImport.importTable(inputTableName, outputTableName, false);
  }

  @Override
  public void createTable() throws IOException {
    hiveImport.importTable(inputTableName, outputTableName, true);
  }
}
