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

import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

public class GenericJdbcLoader extends Loader<LinkConfiguration, ToJobConfiguration> {

  public static final int DEFAULT_ROWS_PER_BATCH = 100;
  public static final int DEFAULT_BATCHES_PER_TRANSACTION = 100;
  private int rowsPerBatch = DEFAULT_ROWS_PER_BATCH;
  private int batchesPerTransaction = DEFAULT_BATCHES_PER_TRANSACTION;
  private long rowsWritten = 0;

  @Override
  public void load(LoaderContext context, LinkConfiguration linkConfig, ToJobConfiguration toJobConfig) throws Exception{
    GenericJdbcExecutor executor = new GenericJdbcExecutor(linkConfig.linkConfig);
    executor.setAutoCommit(false);
    String sql = context.getString(GenericJdbcConnectorConstants.CONNECTOR_JDBC_TO_DATA_SQL);
    executor.beginBatch(sql);
    try {
      int numberOfRowsPerBatch = 0;
      int numberOfBatchesPerTransaction = 0;
      Object[] array;

      while ((array = context.getDataReader().readArrayRecord()) != null) {
        numberOfRowsPerBatch++;
        executor.addBatch(array, context.getSchema());

        if (numberOfRowsPerBatch == rowsPerBatch) {
          numberOfBatchesPerTransaction++;
          if (numberOfBatchesPerTransaction == batchesPerTransaction) {
            executor.executeBatch(true);
            numberOfBatchesPerTransaction = 0;
          } else {
            executor.executeBatch(false);
          }
          numberOfRowsPerBatch = 0;
        }
        rowsWritten ++;
      }

      if (numberOfRowsPerBatch != 0 || numberOfBatchesPerTransaction != 0) {
        // execute and commit the remaining rows
        executor.executeBatch(true);
      }

      executor.endBatch();

    } finally {
      executor.close();
    }
  }

  /* (non-Javadoc)
   * @see org.apache.sqoop.job.etl.Loader#getRowsWritten()
   */
  @Override
  public long getRowsWritten() {
    return rowsWritten;
  }

}
