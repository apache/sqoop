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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.mapreduce.db.Db2DataDrivenDBInputFormat;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.mapreduce.ExportBatchOutputFormat;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;
import org.apache.sqoop.util.LoggingUtils;

/**
 * Manages connections to DB2 databases. Requires the DB2 JDBC driver.
 */
public class Db2Manager
    extends com.cloudera.sqoop.manager.GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(
      Db2Manager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS =
      "com.ibm.db2.jcc.DB2Driver";

  public Db2Manager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  /**
   * Perform an import of a table from the database into HDFS.
   */
  @Override
  public void importTable(
          com.cloudera.sqoop.manager.ImportJobContext context)
      throws IOException, ImportException {
    context.setConnManager(this);
    // Specify the DB2-specific DBInputFormat for import.
    context.setInputFormat(Db2DataDrivenDBInputFormat.class);
    super.importTable(context);
  }

  /**
   * Export data stored in HDFS into a table in a database.
   */
  @Override
  public void exportTable(com.cloudera.sqoop.manager.ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    JdbcExportJob exportJob = new JdbcExportJob(context, null, null,
      ExportBatchOutputFormat.class);
    exportJob.runExport();
  }

  /**
   * DB2 does not support the CURRENT_TIMESTAMP() function. Instead
   * it uses the sysibm schema for timestamp lookup.
   */
  @Override
  public String getCurTimestampQuery() {
    return "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1 WITH UR";
  }

  @Override
  public String[] listDatabases() {
    Connection conn = null;
    ResultSet rset = null;
    List<String> databases = new ArrayList<String>();
    try {
      conn = getConnection();
      rset = conn.getMetaData().getSchemas();
      while (rset.next()) {
        // The ResultSet contains two columns - TABLE_SCHEM(1),
        // TABLE_CATALOG(2). We are only interested in TABLE_SCHEM which
        // represents schema name.
        databases.add(rset.getString(1));
      }
      conn.commit();
    } catch (SQLException sqle) {
      try {
        if (conn != null) {
          conn.rollback();
        }
      } catch (SQLException ce) {
        LoggingUtils.logAll(LOG, "Failed to rollback transaction", ce);
      }
      LoggingUtils.logAll(LOG, "Failed to list databases", sqle);
      throw new RuntimeException(sqle);
    } finally {
      if (rset != null) {
        try {
          rset.close();
        } catch (SQLException re) {
          LoggingUtils.logAll(LOG, "Failed to close resultset", re);
        }
      }
    }

    return databases.toArray(new String[databases.size()]);
  }
}
