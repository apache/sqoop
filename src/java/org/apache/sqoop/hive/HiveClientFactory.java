package org.apache.sqoop.hive;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2Client;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import com.cloudera.sqoop.manager.ConnManager;

public class HiveClientFactory {

  public HiveClient createHiveClient(SqoopOptions sqoopOptions, ConnManager connManager) {
    if (useHiveCli()) {
      return createHiveImportToHiveClientAdapter(sqoopOptions, connManager);
    } else {
      return createHiveServer2Client(sqoopOptions, connManager);
    }
  }

  private HiveClient createHiveImportToHiveClientAdapter(SqoopOptions sqoopOptions, ConnManager connManager) {
    HiveImport hiveImport = new HiveImport(sqoopOptions, connManager, sqoopOptions.getConf(), false);
    return new HiveImportToHiveClientAdapter(hiveImport, sqoopOptions.getTableName(), sqoopOptions.getHiveTableName());
  }

  private HiveClient createHiveServer2Client(SqoopOptions sqoopOptions, ConnManager connManager) {
    TableDefWriter tableDefWriter = new TableDefWriter(sqoopOptions, connManager, sqoopOptions.getTableName(), sqoopOptions.getHiveTableName(), sqoopOptions.getConf(), false);
    JdbcConnectionFactory hs2JdbcConnectionFactory = new HiveServer2ConnectionFactory(null, null, null);
    return new HiveServer2Client(sqoopOptions, tableDefWriter, hs2JdbcConnectionFactory);
  }

  private boolean useHiveCli() {
    return true;
  }

}
