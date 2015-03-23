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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

import javax.sql.DataSource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;

public class JdbcRepositoryTransaction implements RepositoryTransaction {

  private static final Logger LOG =
      LogManager.getLogger(JdbcRepositoryTransaction.class);

  private final DataSource dataSource;
  private Connection connection;
  private JdbcRepositoryTransactionFactory txFactory;
  private boolean active = true;
  private int count = 0;

  private boolean rollback = false;

  protected JdbcRepositoryTransaction(DataSource dataSource,
      JdbcRepositoryTransactionFactory factory) {
    this.dataSource = dataSource;
    txFactory = factory;
  }

  public Connection getConnection() {
    return connection;
  }

  @Override
  public void begin() {
    if (!active) {
      throw new SqoopException(RepositoryError.JDBCREPO_0006);
    }
    if (count == 0) {
      // Lease a connection now
      try {
        connection = dataSource.getConnection();
      } catch (SQLException ex) {
        throw new SqoopException(RepositoryError.JDBCREPO_0007, ex);
      }
      // Clear any prior warnings on the connection
      try {
        connection.clearWarnings();
      } catch (SQLException ex) {
        LOG.error("Error while clearing warnings: " + ex.getErrorCode(), ex);
      }
    }
    count++;
    LOG.debug("Tx count-begin: " + count + ", rollback: " + rollback);
  }

  @Override
  public void commit() {
    if (!active) {
      throw new SqoopException(RepositoryError.JDBCREPO_0006);
    }
    if (rollback) {
      throw new SqoopException(RepositoryError.JDBCREPO_0008);
    }
    LOG.debug("Tx count-commit: " + count + ", rollback: " + rollback);
  }

  @Override
  public void rollback() {
    if (!active) {
      throw new SqoopException(RepositoryError.JDBCREPO_0006);
    }
    LOG.warn("Marking transaction for rollback");
    rollback = true;
    LOG.debug("Tx count-rollback: " + count + ", rollback: " + rollback);
  }

  @Override
  public void close() {
    if (!active) {
      throw new SqoopException(RepositoryError.JDBCREPO_0006);
    }
    count--;
    LOG.debug("Tx count-close: " + count + ", rollback: " + rollback);
    if (count == 0) {
      active = false;
      try {
        if (rollback) {
          LOG.info("Attempting transaction roll-back");
          connection.rollback();
        } else {
          LOG.info("Attempting transaction commit");
          connection.commit();
        }
      } catch (SQLException ex) {
        throw new SqoopException(RepositoryError.JDBCREPO_0009, ex);
      } finally {
        if (connection != null) {
          // Log Warnings
          try {
            SQLWarning warning = connection.getWarnings();
            if (warning != null) {
              StringBuilder sb = new StringBuilder("Connection warnigns: ");
              boolean first = true;
              while (warning != null) {
                if (first) {
                  first = false;
                } else {
                  sb.append("; ");
                }
                sb.append("[").append(warning.getErrorCode()).append("] ");
                sb.append(warning.getMessage());
                warning = warning.getNextWarning();
              }
              LOG.warn(sb.toString());
            }
          } catch (SQLException ex) {
            LOG.error("Error while retrieving warnigns: "
                                + ex.getErrorCode(), ex);
          }

          // Close Connection
          try {
            connection.close();
          } catch (SQLException ex) {
            LOG.error(
                "Unable to close connection: " + ex.getErrorCode(), ex);
          }
        }

        // Clean up thread local
        txFactory.remove();

        // Destroy local state
        connection = null;
        txFactory = null;
      }
    }
  }
}
