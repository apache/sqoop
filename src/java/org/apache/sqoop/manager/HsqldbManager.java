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

package org.apache.sqoop.manager;

import static org.apache.sqoop.manager.JdbcDrivers.HSQLDB;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.AsyncSqlOutputFormat;
import com.cloudera.sqoop.util.ExportException;
import java.io.IOException;

/**
 * Manages connections to hsqldb databases.
 * Extends generic SQL manager.
 */
public class HsqldbManager
    extends com.cloudera.sqoop.manager.GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(
      HsqldbManager.class.getName());

  // HsqlDb doesn't have a notion of multiple "databases"; the user's database
  // is always called "PUBLIC".
  private static final String HSQL_SCHEMA_NAME = "PUBLIC";

  public HsqldbManager(final SqoopOptions opts) {
    super(HSQLDB.getDriverClass(), opts);
  }

  /**
   * Return list of databases hosted by the server.
   * HSQLDB only supports a single schema named "PUBLIC".
   */
  @Override
  public String[] listDatabases() {
    String [] databases = {HSQL_SCHEMA_NAME};
    return databases;
  }

  @Override
  public String escapeTableName(String tableName) {
    return '"' + tableName + '"';
  }

  @Override
  public String escapeColName(String colName) {
    return '"' + colName + '"';
  }

  @Override
  /**
   * {@inheritDoc}
   */
  protected String getCurTimestampQuery() {
    // HSQLDB requires that you select from a table; this table is
    // guaranteed to exist.
    return "SELECT CURRENT_TIMESTAMP FROM INFORMATION_SCHEMA.SYSTEM_TABLES";
  }

  @Override
  public boolean supportsStagingForExport() {
    return true;
  }

  @Override
  /** {@inheritDoc} */
  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    // HSQLDB does not support multi-row inserts; disable that before export.
    context.getOptions().getConf().setInt(
        AsyncSqlOutputFormat.RECORDS_PER_STATEMENT_KEY, 1);
    super.exportTable(context);
  }
}
