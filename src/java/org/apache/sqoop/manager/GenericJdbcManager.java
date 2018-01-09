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

import java.sql.Connection;
import java.sql.SQLException;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.cli.RelatedOptions;

/**
 * Database manager that is connects to a generic JDBC-compliant
 * database; its constructor is parameterized on the JDBC Driver
 * class to load.
 */
public class GenericJdbcManager
    extends SqlManager {

  public static final Log LOG = LogFactory.getLog(
      GenericJdbcManager.class.getName());

  private String jdbcDriverClass;
  private Connection connection;
  private static final String SCHEMA = "schema";

  public GenericJdbcManager(final String driverClass, final SqoopOptions opts) {
    super(opts);

    this.jdbcDriverClass = driverClass;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (null == this.connection) {
      this.connection = makeConnection();
    }

    return this.connection;
  }

  protected boolean hasOpenConnection() {
    return this.connection != null;
  }

  /**
   * Any reference to the connection managed by this manager is nulled.
   * If doClose is true, then this method will attempt to close the
   * connection first.
   * @param doClose if true, try to close the connection before forgetting it.
   */
  public void discardConnection(boolean doClose) {
    if (doClose && hasOpenConnection()) {
      try {
        this.connection.close();
      } catch(SQLException sqe) {
      }
    }

    this.connection = null;
  }

  public void close() throws SQLException {
    super.close();
    discardConnection(true);
  }

  public String getDriverClass() {
    return jdbcDriverClass;
  }

  public String parseExtraScheArgs(String[] args,RelatedOptions opts) throws ParseException {
    // No-op when no extra arguments are present
    if (args == null || args.length == 0) {
      return null;
    }

    // We do not need extended abilities of SqoopParser, so we're using
    // Gnu parser instead.
    CommandLineParser parser = new GnuParser();
    CommandLine cmdLine = parser.parse(opts, args, true);

    //Apply parsed arguments
    return applyExtraScheArguments(cmdLine);
  }

  public String applyExtraScheArguments(CommandLine cmdLine) {
    if (cmdLine.hasOption(SCHEMA)) {
      String schemaName = cmdLine.getOptionValue(SCHEMA);
      LOG.info("We will use schema " + schemaName);

      return schemaName;
    }

    return null;
  }
}

