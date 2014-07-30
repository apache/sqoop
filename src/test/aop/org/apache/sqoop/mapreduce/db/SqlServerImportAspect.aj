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

package org.apache.sqoop.mapreduce.db;

import java.sql.SQLException;
import org.apache.sqoop.fi.ProbabilityModel;
import java.sql.ResultSet;
import java.io.IOException;
import java.sql.Connection;
import java.lang.Math;
import java.util.Random;
/**
* This aspect forces sql connection exceptions and long backoff times
* class
*/
public privileged aspect SqlServerImportAspect {

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
                                        "40001", // SQL Error 1205, deadlock victim
                                        "01000", //SQL Error 233, connection
                                                 // init failure
                                        "08001", //10060 connection link/
                                                 // init failure
                                      };

  private static final boolean allFaults = false;

  // import pointcut, throw a SQL Exception as if the connection was reset
  // during a database read
  pointcut ImportQueryPointcut():
    execution (protected ResultSet DBRecordReader.executeQuery(String))
      && target(DBRecordReader);

  after() returning throws SQLException : ImportQueryPointcut() {
      Random random = new Random();
      int exceptionToThrow = 0;
      if (allFaults)
      {
        exceptionToThrow = random.nextInt(sqlStates.length);
      }
      DBRecordReader.LOG.info("exception to be thrown is " + exceptionToThrow);
      DBRecordReader.LOG.info("Hitting import execute query pointcut,"
        + " return a SQL Exception after reading rows");
      if (ProbabilityModel.injectCriteria("DBRecordReader")) {
        DBRecordReader.LOG.info("throwing " + exceptionMsg[exceptionToThrow]
          + "exception after reading");
        throw new SQLException(exceptionMsg[exceptionToThrow],
          sqlStates[exceptionToThrow]);
      }
  }

  // connection reset pointcut.  Make the backoff wait time the maximum time
  pointcut ConnectionResetPointcut(SQLServerConnectionFailureHandler handler):
    execution (public Connection BasicRetrySQLFailureHandler.recover())
      && target(handler);

  before (SQLServerConnectionFailureHandler handler)
    throws IOException : ConnectionResetPointcut(handler) {
      handler.LOG.info("Hitting connection reset pointcut. "
        + "waiting max time of " + handler.DEFAULT_RETRY_WAIT_MAX
        + " and interval default is " + handler.DEFAULT_RETRY_WAIT_INTERVAL);

      // calculate the max number of retries by solving for numRetries
      // in the retry logic
      // where timeToWait = retryNum^2 * DEFAULT_RETRY_WAIT_INTERVAL
      // so therefore since we want to know the number of retries it
      // takes to get the DEFAULT_RETRY_WAIT_MAX we solve for retryNum
      long maxNumRetries = (long)Math.ceil(Math.sqrt
        ((double)handler.DEFAULT_RETRY_WAIT_MAX
          /handler.DEFAULT_RETRY_WAIT_INTERVAL));

      long maxTimeWait = 0;
      for (double i = 0; i <= maxNumRetries; i++)
      {
        maxTimeWait += (long)(Math.pow(i, 2)
          * (double)handler.DEFAULT_RETRY_WAIT_INTERVAL);
      }
      handler.LOG.info("Maximum retries possible is " + maxNumRetries
        + " and maximum time to wait is " + maxTimeWait);
      if (ProbabilityModel.injectCriteria("SQLServerConnectionFailureHandler")) {
        try {
          handler.LOG.info("sleeping waiting for a connection for max time");
          Thread.sleep(maxTimeWait);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }
  }
}