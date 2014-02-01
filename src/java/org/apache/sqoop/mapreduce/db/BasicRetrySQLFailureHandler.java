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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A failure handler which uses basic retry mechanism for handling
 * SQL failures. Retry settings are embedded in job configuration
 */
public class BasicRetrySQLFailureHandler
    extends SQLFailureHandler {

  private static final Log LOG =
      LogFactory.getLog(BasicRetrySQLFailureHandler.class);

  // Configuration name for retry attempts
  public static final String CONNECTION_RETRY_WAIT_MAX =
      "connection.recover.wait.max";

  // Configuration name for retry interval
  public static final String CONNECTION_RETRY_WAIT_INTERVAL =
      "connection.recover.wait.interval";

  // Default values for retry settings
  public static final int DEFAULT_RETRY_WAIT_MAX = 2 * 60 * 1000;
  public static final int DEFAULT_RETRY_WAIT_INTERVAL = 500;

  protected int retryWaitMax = 0;
  protected int retryWaitInterval = 0;

  public BasicRetrySQLFailureHandler() {
  }

  /**
   * Initialize the the handler with job configuration.
   */
  public void initialize(Configuration conf) throws IOException {
    super.initialize(conf);

    // Retrieve retry settings from job-configuration
    retryWaitMax = conf.getInt(CONNECTION_RETRY_WAIT_MAX,
        DEFAULT_RETRY_WAIT_MAX);
    retryWaitInterval = conf.getInt(CONNECTION_RETRY_WAIT_INTERVAL,
        DEFAULT_RETRY_WAIT_INTERVAL);

    if (retryWaitMax <= retryWaitInterval || retryWaitInterval <= 0) {
      LOG.error("Failed to initialize handler");
      throw new IOException("Invalid retry paramers. Wait Max:  "
        + retryWaitMax + ". wait interval: " + retryWaitInterval);
    }
    LOG.trace("Retry Handler initialized successfully");
  }

  /**
   * Check whether the given failure is supported by this failure handler
   *
   * This is a generic handler for all SQLException failures. Subclasses
   * should override this method for specific error handling
   */
  public boolean canHandleFailure(Throwable failureCause) {
    return failureCause != null
      && SQLException.class.isAssignableFrom(failureCause.getClass());
  }

  /**
   * Provide specific handling for the failure and return a new valid
   * connection.
   */
  public Connection recover() throws IOException {
    long nextRetryWait = 0;
    int retryAttempts = 0;
    boolean doRetry = true;
    boolean validConnection = false;
    Connection conn = null;

    do {
      validConnection = false;

      // Use increasing wait interval
      nextRetryWait = (long) Math.pow(retryAttempts, 2) * retryWaitInterval;

      // Increase the number of retry attempts
      ++retryAttempts;

      // If we exceeded max retry attempts, try one last time with max value
      if (nextRetryWait > retryWaitMax) {
        nextRetryWait = retryWaitMax;
        doRetry = false;
      }

      try {
        // Wait before trying to recover the connection
        Thread.sleep(nextRetryWait);

        // Discard the connection
        discardConnection(conn);

        // Try to get a new connection
        conn = super.getConnection();
        if (!validateConnection(conn)) {
          // Log failure and continue
          LOG.warn("Connection not valid");
        } else {
          LOG.info("A new connection has been established");

          // Connection has been recovered so stop recovery retries
          doRetry = false;
          validConnection = true;
        }
      } catch (SQLException sqlEx) {
        LOG.warn("Connection recovery attempt [" + retryAttempts + "] failed."
            + "Exception details: " + sqlEx.toString());
      } catch (Exception ex) {
        // Handle unexpected exceptions
        LOG.error("Failed while recovering the connection. Exception details:"
            + ex.toString());
        throw new IOException(ex);
      }
    } while (doRetry);

    if (!validConnection) {
      throw new IOException("Failed to recover connection after " +
          retryAttempts + " retries. Giving up");
    }
    return conn;
  }

  /**
   * Verify the provided connection is valid.
   */
  protected boolean validateConnection(Connection connection)
      throws SQLException {
    return connection != null && !connection.isClosed()
      && connection.isValid(DEFAULT_RETRY_WAIT_INTERVAL);
  }

  /**
   * Close the given connection.
   */
  protected void discardConnection(Connection connection) throws IOException {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch(SQLException sqlEx) {
      LOG.warn("Could not close connection. Exception details: " + sqlEx);
    }
  }
}
