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

import org.apache.hadoop.conf.Configuration;

/**
 * Base class for handling SQL failures to while executing operations on the
 * target database.
 * Subclasses can provide different handling for connection errors like
 * recovering from connection resets, or handling server throttling
 */
public abstract class SQLFailureHandler {

  // Job configuration for the currently running job
  protected Configuration conf;

  /**
   * Initialize the the handler with job configuration.
   */
  public void initialize(Configuration c) throws IOException {
    this.conf = c;
  }

  /**
   * Check whether the given failure is supported by the connection failure
   * handler.
   */
  public abstract boolean canHandleFailure(Throwable failureCause);

  /**
   * Provide specific handling for the connection failure and return a new
   * valid connection.
   */
  public abstract Connection recover() throws IOException;

  /**
   * Establish a connection to the target database.
   */
  protected Connection getConnection()
    throws ClassNotFoundException, SQLException {
    // Get a DBConfiguration object from the current job configuration
    DBConfiguration dbConf = new DBConfiguration(conf);
    Connection conn = null;
    conn = dbConf.getConnection();
    return conn;
  }
}
