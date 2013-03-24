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
package org.apache.sqoop.test.db;

import org.apache.log4j.Logger;
import org.apache.derby.drda.NetworkServerControl;

import java.net.InetAddress;

/**
 * Implementation of database provider that is based on embedded derby server.
 *
 * This provider will work out of the box without any extra configuration.
 */
public class DerbyProvider extends DatabaseProvider {

  private static final Logger LOG = Logger.getLogger(DerbyProvider.class);

  public static final String DRIVER = "org.apache.derby.jdbc.ClientDriver";

  NetworkServerControl server = null;

  @Override
  public void start() {
    // Start embedded server
    try {
      server = new NetworkServerControl(InetAddress.getByName("localhost"), 1527);
      server.start(null);
    } catch (Exception e) {
      LOG.error("Can't start Derby network server", e);
      throw new RuntimeException("Can't derby server", e);
    }

    super.start();
  }

  @Override
  public void stop() {
    super.stop();

    // Shutdown embedded server
    try {
      server.shutdown();
    } catch (Exception e) {
      LOG.info("Can't shut down embedded server", e);
    }
  }

  @Override
  public String escapeColumnName(String columnName) {
    return escape(columnName);
  }

  @Override
  public String escapeTableName(String tableName) {
    return escape(tableName);
  }

  @Override
  public String escapeValueString(String value) {
    return "'" + value + "'";
  }

  @Override
  public boolean isSupportingScheme() {
    return true;
  }

  public String escape(String entity) {
    return "\"" + entity + "\"";
  }

  @Override
  public String getJdbcDriver() {
    return DRIVER;
  }

  @Override
  public String getConnectionUrl() {
    return "jdbc:derby://localhost:1527/memory:sqoop;create=true";
  }

  @Override
  public String getConnectionUsername() {
    return null;
  }

  @Override
  public String getConnectionPassword() {
    return null;
  }
}
