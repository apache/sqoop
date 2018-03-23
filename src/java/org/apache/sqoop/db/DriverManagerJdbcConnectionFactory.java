/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerJdbcConnectionFactory implements JdbcConnectionFactory {

  private final String driverClass;
  private final String connectionString;
  private final String username;
  private final String password;
  private final Properties additionalProps;

  public DriverManagerJdbcConnectionFactory(String driverClass, String connectionString, String username,
                                            String password, Properties additionalProps) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.additionalProps = additionalProps;
  }

  public DriverManagerJdbcConnectionFactory(String driverClass, String connectionString, String username, String password) {
    this(driverClass, connectionString, username, password, new Properties());
  }

  @Override
  public Connection createConnection() {
    loadDriverClass();

    Properties connectionProperties = new Properties();
    if (username != null) {
      connectionProperties.put("user", username);
    }

    if (password != null) {
      connectionProperties.put("password", password);
    }

    connectionProperties.putAll(additionalProps);
    try {
      return DriverManager.getConnection(connectionString, connectionProperties);
    } catch (SQLException e) {
      throw new RuntimeException("Establishing connection failed!", e);
    }
  }

  private void loadDriverClass() {
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load db driver class: " + driverClass);
    }
  }

  public String getDriverClass() {
    return driverClass;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Properties getAdditionalProps() {
    return new Properties(additionalProps);
  }
}
