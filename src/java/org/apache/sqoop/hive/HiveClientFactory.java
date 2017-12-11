package org.apache.sqoop.hive;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2Client;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
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
    TableDefWriter tableDefWriter = new TableDefWriter(sqoopOptions, connManager, sqoopOptions.getTableName(), sqoopOptions.getHiveTableName(), sqoopOptions.getConf(), false);
    JdbcConnectionFactory hs2JdbcConnectionFactory = new HiveServer2ConnectionFactory(null, null, null);
    return new HiveServer2Client(sqoopOptions, tableDefWriter, hs2JdbcConnectionFactory);
  }

  private boolean useHiveCli() {
    return false;
  }

}
