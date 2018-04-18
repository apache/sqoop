/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static java.util.Arrays.asList;

public class HiveServer2Client implements HiveClient {

  private static final Log LOG = LogFactory.getLog(HiveServer2Client.class.getName());

  private final SqoopOptions sqoopOptions;

  private final TableDefWriter tableDefWriter;

  private final JdbcConnectionFactory hs2ConnectionFactory;

  private final HiveClientCommon hiveClientCommon;

  public HiveServer2Client(SqoopOptions sqoopOptions, TableDefWriter tableDefWriter, JdbcConnectionFactory hs2ConnectionFactory, HiveClientCommon hiveClientCommon) {
    this.sqoopOptions = sqoopOptions;
    this.tableDefWriter = tableDefWriter;
    this.hs2ConnectionFactory = hs2ConnectionFactory;
    this.hiveClientCommon = hiveClientCommon;
  }

  public HiveServer2Client(SqoopOptions sqoopOptions, TableDefWriter tableDefWriter, JdbcConnectionFactory hs2ConnectionFactory) {
    this(sqoopOptions, tableDefWriter, hs2ConnectionFactory, new HiveClientCommon());
  }

  @Override
  public void importTable() throws IOException {
    LOG.info("Loading uploaded data into Hive.");
    String createTableStmt = tableDefWriter.getCreateTableStmt();
    String loadDataStmt = tableDefWriter.getLoadDataStmt();
    executeHiveImport(asList(createTableStmt, loadDataStmt));
    LOG.info("Hive import complete.");
  }

  @Override
  public void createTable() throws IOException {
    LOG.info("Creating Hive table: " + tableDefWriter.getOutputTableName());
    String createTableStmt = tableDefWriter.getCreateTableStmt();
    executeHiveImport(asList(createTableStmt));
    LOG.info("Hive table is successfully created.");
  }

  void executeHiveImport(List<String> commands) throws IOException {
    Path finalPath = tableDefWriter.getFinalPath();

    hiveClientCommon.removeTempLogs(sqoopOptions.getConf(), finalPath);

    hiveClientCommon.indexLzoFiles(sqoopOptions, finalPath);

    try {
      executeCommands(commands);
    } catch (SQLException e) {
      throw new RuntimeException("Error executing Hive import.", e);
    }

    hiveClientCommon.cleanUp(sqoopOptions.getConf(), finalPath);
  }

  void executeCommands(List<String> commands) throws SQLException {
    try (Connection hs2Connection = hs2ConnectionFactory.createConnection()) {
      for (String command : commands) {
        LOG.debug("Executing command: " + command);
        try (PreparedStatement statement = hs2Connection.prepareStatement(command)) {
          statement.execute();
        }
      }
    }
  }

  SqoopOptions getSqoopOptions() {
    return sqoopOptions;
  }

  TableDefWriter getTableDefWriter() {
    return tableDefWriter;
  }

  JdbcConnectionFactory getHs2ConnectionFactory() {
    return hs2ConnectionFactory;
  }
}
