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
package org.apache.sqoop.metastore.hsqldb;

import java.io.File;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.StringUtils;

import org.hsqldb.Server;
import org.hsqldb.ServerConstants;

import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.manager.HsqldbManager;

/**
 * Container for an HSQLDB-backed metastore.
 */
public class HsqldbMetaStore {

  public static final Log LOG = LogFactory.getLog(
      HsqldbMetaStore.class.getName());

  /** Where on the local fs does the metastore put files? */
  public static final String META_STORAGE_LOCATION_KEY =
      "sqoop.metastore.server.location";

  /**
   * What port does the metastore listen on?
   */
  public static final String META_SERVER_PORT_KEY =
      "sqoop.metastore.server.port";

  /** Default to this port if unset. */
  public static final int DEFAULT_PORT = 16000;

  private int port;
  private String fileLocation;
  private Server server;
  private Configuration conf;

  public HsqldbMetaStore(Configuration config) {
    this.conf = config;
    init();
  }

  /**
   * Determine the user's home directory and return a file path
   * under this root where the shared metastore can be placed.
   */
  private String getHomeDirFilePath() {
    String homeDir = System.getProperty("user.home");

    File homeDirObj = new File(homeDir);
    File sqoopDataDirObj = new File(homeDirObj, ".sqoop");
    File databaseFileObj = new File(sqoopDataDirObj, "shared-metastore.db");

    return databaseFileObj.toString();
  }

  private void init() {
    if (null != server) {
      LOG.debug("init(): server already exists.");
      return;
    }

    fileLocation = conf.get(META_STORAGE_LOCATION_KEY, null);
    if (null == fileLocation) {
      fileLocation = getHomeDirFilePath();
      LOG.warn("The location for metastore data has not been explicitly set. "
          + "Placing shared metastore files in " + fileLocation);
    }

    this.port = conf.getInt(META_SERVER_PORT_KEY, DEFAULT_PORT);
  }


  public void start() {
    try {
      if (server != null) {
        server.checkRunning(false);
      }
    } catch (RuntimeException re) {
      LOG.info("Server is already started.");
      return;
    }

    server = new Server();
    server.setDatabasePath(0, "file:" + fileLocation);
    server.setDatabaseName(0, "sqoop");
    server.putPropertiesFromString("hsqldb.write_delay=false");
    server.setPort(port);
    server.setSilent(true);
    server.setNoSystemExit(true);

    server.start();
    LOG.info("Server started on port " + port + " with protocol "
        + server.getProtocol());
  }

  /**
   * Blocks the current thread until the server is shut down.
   */
  public void waitForServer() {
    while (true) {
      int curState = server.getState();
      if (curState == ServerConstants.SERVER_STATE_SHUTDOWN) {
        LOG.info("Got shutdown notification");
        break;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        LOG.info("Interrupted while blocking for server:"
            + StringUtils.stringifyException(ie));
      }
    }
  }

  /**
   * Connects to the server and instructs it to shutdown.
   */
  public void shutdown() {
    // Send the SHUTDOWN command to the server via SQL.
    SqoopOptions options = new SqoopOptions(conf);
    options.setConnectString("jdbc:hsqldb:hsql://localhost:"
        + port + "/sqoop");
    options.setUsername("SA");
    options.setPassword("");
    HsqldbManager manager = new HsqldbManager(options);
    Statement s = null;
    try {
      Connection c = manager.getConnection();
      s = c.createStatement();
      s.execute("SHUTDOWN");
    } catch (SQLException sqlE) {
      LOG.warn("Exception shutting down database: "
          + StringUtils.stringifyException(sqlE));
    } finally {
      if (null != s) {
        try {
          s.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing statement: " + sqlE);
        }
      }

      try {
        manager.close();
      } catch (SQLException sqlE) {
        LOG.warn("Error closing manager: " + sqlE);
      }
    }
  }
}

