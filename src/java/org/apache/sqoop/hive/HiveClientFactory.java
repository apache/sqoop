/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2Client;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import com.cloudera.sqoop.manager.ConnManager;

public class HiveClientFactory {

  public HiveClient createHiveClient(SqoopOptions sqoopOptions, ConnManager connManager) {
    if (useHiveCli(sqoopOptions)) {
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
    JdbcConnectionFactory hs2JdbcConnectionFactory = new HiveServer2ConnectionFactory(sqoopOptions.getHs2Url(), null, null);
    return new HiveServer2Client(sqoopOptions, tableDefWriter, hs2JdbcConnectionFactory);
  }

  private boolean useHiveCli(SqoopOptions sqoopOptions) {
    return StringUtils.isEmpty(sqoopOptions.getHs2Url());
  }

}
