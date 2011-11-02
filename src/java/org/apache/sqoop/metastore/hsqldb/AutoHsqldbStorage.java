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
import java.io.IOException;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

/**
 * JobStorage implementation that auto-configures an HSQLDB
 * local-file-based instance to hold jobs.
 */
public class AutoHsqldbStorage
    extends com.cloudera.sqoop.metastore.hsqldb.HsqldbJobStorage {

  public static final Log LOG = LogFactory.getLog(
      AutoHsqldbStorage.class.getName());

  /**
   * Configuration key specifying whether this storage agent is active.
   * Defaults to "on" to allow zero-conf local users.
   */
  public static final String AUTO_STORAGE_IS_ACTIVE_KEY =
      "sqoop.metastore.client.enable.autoconnect";

  /**
   * Configuration key specifying the connect string used by this
   * storage agent.
   */
  public static final String AUTO_STORAGE_CONNECT_STRING_KEY =
      "sqoop.metastore.client.autoconnect.url";

  /**
   * Configuration key specifying the username to bind with.
   */
  public static final String AUTO_STORAGE_USER_KEY =
      "sqoop.metastore.client.autoconnect.username";


  /** HSQLDB default user is named 'SA'. */
  private static final String DEFAULT_AUTO_USER = "SA";

  /**
   * Configuration key specifying the password to bind with.
   */
  public static final String AUTO_STORAGE_PASS_KEY =
      "sqoop.metastore.client.autoconnect.password";

  /** HSQLDB default user has an empty password. */
  public static final String DEFAULT_AUTO_PASSWORD = "";

  @Override
  /** {@inheritDoc} */
  public boolean canAccept(Map<String, String> descriptor) {
    Configuration conf = this.getConf();
    return conf.getBoolean(AUTO_STORAGE_IS_ACTIVE_KEY, true);
  }

  /**
   * Determine the user's home directory and return a connect
   * string to HSQLDB that uses ~/.sqoop/ as the storage location
   * for the metastore database.
   */
  private String getHomeDirFileConnectStr() {
    String homeDir = System.getProperty("user.home");

    File homeDirObj = new File(homeDir);
    File sqoopDataDirObj = new File(homeDirObj, ".sqoop");
    File databaseFileObj = new File(sqoopDataDirObj, "metastore.db");

    String dbFileStr = databaseFileObj.toString();
    return "jdbc:hsqldb:file:" + dbFileStr
        + ";hsqldb.write_delay=false;shutdown=true";
  }

  @Override
  /**
   * Set the connection information to use the auto-inferred connection
   * string.
   */
  public void open(Map<String, String> descriptor) throws IOException {
    Configuration conf = getConf();
    setMetastoreConnectStr(conf.get(AUTO_STORAGE_CONNECT_STRING_KEY,
        getHomeDirFileConnectStr()));
    setMetastoreUser(conf.get(AUTO_STORAGE_USER_KEY, DEFAULT_AUTO_USER));
    setMetastorePassword(conf.get(AUTO_STORAGE_PASS_KEY,
        DEFAULT_AUTO_PASSWORD));
    setConnectedDescriptor(descriptor);

    init();
  }
}

