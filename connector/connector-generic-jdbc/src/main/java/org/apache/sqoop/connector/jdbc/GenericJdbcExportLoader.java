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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.connector.jdbc.configuration.ConnectionConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ExportJobConfiguration;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

public class GenericJdbcExportLoader extends Loader<ConnectionConfiguration, ExportJobConfiguration> {

  public static final int DEFAULT_ROWS_PER_BATCH = 100;
  public static final int DEFAULT_BATCHES_PER_TRANSACTION = 100;
  private int rowsPerBatch = DEFAULT_ROWS_PER_BATCH;
  private int batchesPerTransaction = DEFAULT_BATCHES_PER_TRANSACTION;

  @Override
  public void load(LoaderContext context, ConnectionConfiguration connection, ExportJobConfiguration job) throws Exception{
    String driver = connection.connection.jdbcDriver;
    String url = connection.connection.connectionString;
    String username = connection.connection.username;
    String password = connection.connection.password;
    GenericJdbcExecutor executor = new GenericJdbcExecutor(driver, url, username, password);
    executor.setAutoCommit(false);

    String sql = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_DATA_SQL);
    executor.beginBatch(sql);
    try {
      int numberOfRows = 0;
      int numberOfBatches = 0;
      Object[] array;

      while ((array = context.getDataReader().readArrayRecord()) != null) {
        numberOfRows++;
        executor.addBatch(array);

        if (numberOfRows == rowsPerBatch) {
          numberOfBatches++;
          if (numberOfBatches == batchesPerTransaction) {
            executor.executeBatch(true);
            numberOfBatches = 0;
          } else {
            executor.executeBatch(false);
          }
          numberOfRows = 0;
        }
      }

      if (numberOfRows != 0) {
        // execute and commit the remaining rows
        executor.executeBatch(true);
      }

      executor.endBatch();

    } finally {
      executor.close();
    }
  }

}
