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

package org.apache.sqoop.mapreduce;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import org.apache.sqoop.fi.ProbabilityModel;
import org.apache.sqoop.lib.SqoopRecord;
import java.util.Random;
/**
* This aspect injects faults into the SQLServerExportDBExecThread
* which handles executing the write batch to the sql server database
*/
public privileged aspect SqlServerExportAspect {

  //  Before adding to these, note that every sqlstate must have a
  // corresponding exceptionMsg and vice versa!!!!!!
  private static String[] exceptionMsg = { "Connection reset",
                                           "deadlocked on thread",
                                           "Connection initialization error",
                                           "Connection link failure"
                                         };

  private static String[] sqlStates = { "08S01", // covers SQL error states
                                                 // 40143 40197, 40501, 40613,
                                                 // 10054, 10053,64
                                        "40001", // SQL Error 1205,
                                                 // deadlock victim
                                        "01000", // SQL Error 233,
                                                 // connection init failure
                                        "08001", // 10060 connection link
                                                 // init failure
                                      };

  private static boolean allFaults = false;

  // export pointcut and advice
  pointcut ExportExecuteStatementPointcut(SQLServerExportDBExecThread thread,
    PreparedStatement stmt,
    List<SqoopRecord> records):
    execution (protected void SQLServerAsyncDBExecThread.executeStatement(
      PreparedStatement, List<SqoopRecord>))
      && target(thread) && args(stmt, records);

  void around(SQLServerExportDBExecThread thread,PreparedStatement stmt,
    List<SqoopRecord> records) throws SQLException:
      ExportExecuteStatementPointcut(thread, stmt, records) {


    Random random = new Random();

    int exceptionToThrow = 0;
    if (allFaults)
    {
      exceptionToThrow = random.nextInt(sqlStates.length);
    }
    thread.LOG.info("exception to be thrown is " + exceptionToThrow);

    // start the method like normal, execute the batch
    Connection conn = thread.getConnection();
    try {
      // throw a SQL exception before/during the execute
      if (ProbabilityModel.injectCriteria("SQLServerExportDBExecThread")) {
        thread.LOG.info("throwing " + exceptionMsg[exceptionToThrow]
          + "exception after execute and before commit");
        conn.close();
        throw new SQLException(exceptionMsg[exceptionToThrow],
          sqlStates[exceptionToThrow]);
      }
      stmt.executeBatch();
    } catch (SQLException execSqlEx) {
        thread.LOG.warn("Error executing statement: " + execSqlEx);
        //conn.rollback();
        if (thread.failedCommit &&
        thread.canIgnoreForFailedCommit(execSqlEx.getSQLState())){
        thread.LOG.info("Ignoring error after failed commit");
      } else {
        throw execSqlEx;
      }
    }

    // If the batch of records is executed successfully, then commit before
    // processing the next batch of records
    try {
      if (ProbabilityModel.injectCriteria("SQLServerExportDBExecThread")) {
        thread.LOG.info("throwing " + exceptionMsg[exceptionToThrow]
          + "exception during commit");
        conn.close();
        throw new SQLException(exceptionMsg[exceptionToThrow],
          sqlStates[exceptionToThrow]);
      }
      conn.commit();
      thread.failedCommit = false;
    } catch (SQLException commitSqlEx) {
      thread.LOG.warn("Error while committing transactions: " + commitSqlEx);
      thread.failedCommit = true;
      throw commitSqlEx;
    }
  }
}
