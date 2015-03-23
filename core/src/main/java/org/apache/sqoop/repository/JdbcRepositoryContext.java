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
package org.apache.sqoop.repository;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.MapContext;


public final class JdbcRepositoryContext {

  private static final Logger LOG =
      Logger.getLogger(JdbcRepositoryContext.class);

  private final MapContext context;
  private final String handlerClassName;
  private final String connectionUrl;
  private final String driverClassName;
  private final Properties connectionProperties;
  private final JdbcTransactionIsolation transactionIsolation;
  private final int maxConnections;

  private DataSource dataSource;
  private JdbcRepositoryTransactionFactory txFactory;

  public JdbcRepositoryContext(MapContext context) {
    this.context = context;

    handlerClassName = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_HANDLER);

    if (handlerClassName == null || handlerClassName.trim().length() == 0) {
      throw new SqoopException(RepositoryError.JDBCREPO_0001,
          RepoConfigurationConstants.SYSCFG_REPO_JDBC_HANDLER);
    }

    connectionUrl = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_URL);

    driverClassName = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_DRIVER);

    String jdbcUserName = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_USER);

    String jdbcPassword = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_PASSWORD);

    connectionProperties = new Properties();

    Map<String, String> params = context.getNestedProperties(
        RepoConfigurationConstants.PREFIX_SYSCFG_REPO_JDBC_PROPERTIES);
    for (Map.Entry<String, String> entry : params.entrySet()) {
      connectionProperties.setProperty(entry.getKey(), entry.getValue());
    }

    if (jdbcUserName != null) {
      Object oldUser = connectionProperties.put("user", jdbcUserName);
      if (oldUser != null) {
        LOG.warn("Overriding user (" + oldUser
            + ") with explicitly specified user (" + jdbcUserName + ")");
      }
    }

    if (jdbcPassword != null) {
      Object oldPassword = connectionProperties.put("password", jdbcPassword);
      if (oldPassword != null) {
        LOG.warn("Overriding password from jdbc connection properties with "
            + "explicitly specified password.");
      }
    }

    String txIsolation = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_TX_ISOLATION);

    if (txIsolation == null || txIsolation.trim().length() == 0) {
      throw new SqoopException(RepositoryError.JDBCREPO_0004);
    }

    try {
      transactionIsolation = JdbcTransactionIsolation.getByName(txIsolation);
    } catch (IllegalArgumentException ex) {
      throw new SqoopException(RepositoryError.JDBCREPO_0004,
          txIsolation, ex);
    }

    String maxConnStr = context.getString(
        RepoConfigurationConstants.SYSCFG_REPO_JDBC_MAX_CONN);

    if (maxConnStr == null || maxConnStr.trim().length() == 0) {
      throw new SqoopException(RepositoryError.JDBCREPO_0005,
          RepoConfigurationConstants.SYSCFG_REPO_JDBC_MAX_CONN);
    }

    int maxConnInt = 0;
    try {
      maxConnInt = Integer.parseInt(maxConnStr);
    } catch (NumberFormatException ex) {
      throw new SqoopException(RepositoryError.JDBCREPO_0005, maxConnStr, ex);
    }

    if (maxConnInt <= 0) {
      throw new SqoopException(RepositoryError.JDBCREPO_0005, maxConnStr);
    }

    maxConnections = maxConnInt;

    if (LOG.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder("[repo-ctx] ");
      sb.append("handler=").append(handlerClassName).append(", ");
      sb.append("conn-url=").append(connectionUrl).append(", ");
      sb.append("driver=").append(driverClassName).append(", ");
      sb.append("user=").append(jdbcUserName).append(", ");
      sb.append("password=").append("*****").append(", ");
      sb.append("jdbc-props={");
      boolean first = true;
      for (Map.Entry<String, String> entry: params.entrySet()) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(entry.getKey()).append("=");
        if (entry.getKey().equalsIgnoreCase("password")) {
          sb.append("*****");
        } else {
          sb.append(entry.getValue());
        }
      }
      sb.append("}").append(", ");
      sb.append("tx-isolation=").append(transactionIsolation).append(", ");
      sb.append("max-conn=").append(maxConnections);

      LOG.info(sb.toString());
    }
  }

  void initialize(DataSource source, JdbcRepositoryTransactionFactory factory) {
    if (dataSource != null || txFactory != null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0011);
    }

    dataSource = source;
    txFactory = factory;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public JdbcRepositoryTransactionFactory getTransactionFactory() {
    return txFactory;
  }

  public String getHandlerClassName() {
    return handlerClassName;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public String getDriverClass() {
    return driverClassName;
  }

  public JdbcTransactionIsolation getTransactionIsolation() {
    return transactionIsolation;
  }

  public int getMaximumConnections() {
    return maxConnections;
  }

  public Properties getConnectionProperties() {
    Properties props = new Properties();
    props.putAll(connectionProperties);

    return props;
  }

  public MapContext getContext() {
    return context;
  }
}
