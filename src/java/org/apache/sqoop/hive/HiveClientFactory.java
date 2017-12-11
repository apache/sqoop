package org.apache.sqoop.hive;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;

public class HiveClientFactory {

  public HiveClient createHiveClient(SqoopOptions sqoopOptions, ConnManager connManager) {
    if (useHiveCli()) {
      return null;
    } else {
      return createHiveServer2Client(sqoopOptions, connManager);
    }
  }

  private HiveClient createHiveServer2Client(SqoopOptions sqoopOptions, ConnManager connManager) {
    return null;
  }

  private boolean useHiveCli() {
    return false;
  }

}
