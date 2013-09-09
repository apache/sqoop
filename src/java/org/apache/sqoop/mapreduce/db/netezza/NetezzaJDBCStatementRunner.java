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

package org.apache.sqoop.mapreduce.db.netezza;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple class for JDBC External table statement execution for Netezza. Even
 * though the statements are execute only (no support for bind variables or
 * resultsets, we use a two step process so that sql statement errors are caught
 * during construction itself.
 */
public class NetezzaJDBCStatementRunner extends Thread {
  public static final Log LOG = LogFactory
      .getLog(NetezzaJDBCStatementRunner.class.getName());

  private Connection con;
  private Exception exception;
  private PreparedStatement ps;
  private Thread parent;

  public boolean hasExceptions() {
    return exception != null;
  }

  public void printException() {
    if (exception != null) {
      LOG.error("Errors encountered during external table JDBC processing");
      LOG.error("Exception " + exception.getMessage(), exception);
    }
  }

  public Throwable getException() {
    if (!hasExceptions()) {
      return null;
    }
    return exception;
  }

  public NetezzaJDBCStatementRunner(Thread parent, Connection con,
      String sqlStatement) throws SQLException {
    this.parent = parent;
    this.con = con;
    this.ps = con.prepareStatement(sqlStatement);
    this.exception = null;
  }

  public void run() {
    boolean interruptParent = false;
    try {

      // Excecute the statement - this will make data to flow in the
      // named pipes
      ps.execute();

    } catch (SQLException sqle) {
      interruptParent = true;
      LOG.error("Unable to execute external table export", sqle);
      this.exception = sqle;
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (Exception e) {
          LOG.debug("Exception closing connection " + e.getMessage());
        }
      }
      con = null;
    }
    if (interruptParent) {
      this.parent.interrupt();
    }
  }
}
