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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.KeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.utils.ClassUtils;


public class JdbcRepositoryProvider implements RepositoryProvider {

  private static final Logger LOG =
      Logger.getLogger(JdbcRepositoryProvider.class);

  private JdbcRepositoryContext repoContext;

  private Driver driver;
  private GenericObjectPool connectionPool;
  private KeyedObjectPoolFactory statementPool;
  private DataSource dataSource;

  private JdbcRepositoryHandler handler;
  private JdbcRepositoryTransactionFactory txFactory;
  private JdbcRepository repository;


  public JdbcRepositoryProvider() {
    // Default constructor
  }

  @Override
  public synchronized void initialize(MapContext context) {
    repoContext = new JdbcRepositoryContext(SqoopConfiguration.getInstance().getContext());

    initializeRepositoryHandler();

    LOG.info("JdbcRepository initialized.");
  }

  @Override
  public synchronized void destroy() {
    try {
      connectionPool.close();
    } catch (Exception ex) {
      LOG.error("Failed to shutdown connection pool", ex);
    }

    handler.shutdown();

    if (driver != null) {
      try {
        LOG.info("Deregistering JDBC driver");
        DriverManager.deregisterDriver(driver);
      } catch (SQLException ex) {
        LOG.error("Failed to deregister driver", ex);
      }
    }
    handler = null;
    driver = null;
    dataSource = null;
  }

  private void initializeRepositoryHandler() {
    String jdbcHandlerClassName = repoContext.getHandlerClassName();

    Class<?> handlerClass = ClassUtils.loadClass(jdbcHandlerClassName);

    if (handlerClass == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0001,
          jdbcHandlerClassName);
    }

    try {
      handler = (JdbcRepositoryHandler) handlerClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(RepositoryError.JDBCREPO_0001,
          jdbcHandlerClassName, ex);
    }

    String connectUrl = repoContext.getConnectionUrl();
    if (connectUrl == null || connectUrl.trim().length() == 0) {
      throw new SqoopException(RepositoryError.JDBCREPO_0002);
    }

    String jdbcDriverClassName = repoContext.getDriverClass();
    if (jdbcDriverClassName == null || jdbcDriverClassName.trim().length() == 0)
    {
      throw new SqoopException(RepositoryError.JDBCREPO_0003);
    }

    // Initialize a datasource
    Class<?> driverClass = ClassUtils.loadClass(jdbcDriverClassName);

    if (driverClass == null) {
      throw new SqoopException(RepositoryError.JDBCREPO_0003,
          jdbcDriverClassName);
    }

    try {
      driver = (Driver) driverClass.newInstance();
    } catch (Exception ex) {
      throw new SqoopException(RepositoryError.JDBCREPO_0003,
          jdbcDriverClassName, ex);
    }

    Properties jdbcProps = repoContext.getConnectionProperties();

    ConnectionFactory connFactory =
        new DriverManagerConnectionFactory(connectUrl, jdbcProps);

    connectionPool = new GenericObjectPool();
    connectionPool.setMaxActive(repoContext.getMaximumConnections());

    statementPool = new GenericKeyedObjectPoolFactory(null);

    // creating the factor automatically wires the connection pool
    new PoolableConnectionFactory(connFactory, connectionPool, statementPool,
        handler.validationQuery(), false, false,
        repoContext.getTransactionIsolation().getCode());

    dataSource = new PoolingDataSource(connectionPool);
    txFactory = new JdbcRepositoryTransactionFactory(dataSource);

    repoContext.initialize(dataSource, txFactory);

    handler.initialize(repoContext);

    if (repoContext.shouldCreateSchema()) {
      if (!handler.schemaExists()) {
        LOG.info("Creating repository schema objects");
        handler.createSchema();
      }
    }

    repository = new JdbcRepository(handler, repoContext);

    LOG.info("JdbcRepositoryProvider initialized");
  }

  @Override
  public synchronized Repository getRepository() {
    return repository;
  }
}
