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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Connection failure handler for SQL Server which extends basic retry
 * mechanism for handling connection failures. Retry settings are embedded
 * in job configuration
 */
public class SQLServerConnectionFailureHandler
    extends BasicRetrySQLFailureHandler {

  private static final Log LOG =
      LogFactory.getLog(SQLServerConnectionFailureHandler.class);

  protected static final String CONNECTION_RESET_ERR_REGEX =
      "(^Connection reset)(.*?)";
  protected static final String SQLSTATE_CODE_CONNECTION_RESET = "08S01";

  protected static final String VALIDATION_QUERY =
      "SELECT CONVERT(NVARCHAR, CONTEXT_INFO()) AS contextInfo";

  public SQLServerConnectionFailureHandler() {
  }

  /**
   * Handle only connection reset or TCP exceptions.
   */
  public boolean canHandleFailure(Throwable failureCause) {
    if (!super.canHandleFailure(failureCause)) {
      return false;
    }
    SQLException sqlEx = (SQLException) failureCause;
    String errStateCode = sqlEx.getSQLState();
    boolean canHandle = false;
    // By default check SQLState code if available
    if (errStateCode != null) {
      canHandle = errStateCode == SQLSTATE_CODE_CONNECTION_RESET;
    } else {
      errStateCode = "NULL";
      // In case SQLState code is not available, check the exception message
      String errMsg = sqlEx.getMessage();
      canHandle = errMsg.matches(CONNECTION_RESET_ERR_REGEX);
    }

    if (!canHandle) {
      LOG.warn("Cannot handle error with SQL State: " + errStateCode);
    }
    return canHandle;
  }

  /**
   * Verify the provided connection is valid. For SQL Server we test the
   * connection by querying the session context.
   */
  protected boolean validateConnection(Connection connection)
      throws SQLException {
    boolean isValid = false;
    String contextInfo = null;
    if (super.validateConnection(connection)) {
      // Execute the validation query
      PreparedStatement stmt = connection.prepareStatement(VALIDATION_QUERY);
      ResultSet results = stmt.executeQuery();

      // Read the context from the result set
      if (results.next()) {
        contextInfo = results.getString("contextInfo");
        LOG.info("Session context is: "
          + ((contextInfo == null) ? "NULL" : contextInfo));
        isValid = true;
      }
    }
    return isValid;
  }
}
