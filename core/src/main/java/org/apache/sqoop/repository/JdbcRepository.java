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
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;

public class JdbcRepository implements Repository {

  private static final Logger LOG =
      Logger.getLogger(JdbcRepository.class);

  private final JdbcRepositoryHandler handler;
  private final JdbcRepositoryContext repoContext;

  protected JdbcRepository(JdbcRepositoryHandler handler,
      JdbcRepositoryContext repoContext) {
    this.handler = handler;
    this.repoContext = repoContext;
  }

  /**
   * Private interface to wrap specific code that requires fresh connection to
   * repository with general code that will get the connection and handle
   * exceptions.
   */
  private interface DoWithConnection {
    /**
     * Do what is needed to be done with given connection object.
     *
     * @param conn Connection to metadata repository.
     * @return Arbitrary value
     */
    public Object doIt(Connection conn) throws Exception;
  }

  /**
   * Handle transaction and connection functionality and delegate action to
   * given delegator.
   *
   * @param delegator Code for specific action
   * @return Arbitrary value
   */
  private Object doWithConnection(DoWithConnection delegator) {
    JdbcRepositoryTransaction tx = null;

    try {
      // Get transaction and connection
      tx = getTransaction();
      tx.begin();
      Connection conn = tx.getConnection();

      // Delegate the functionality to our delegator
      Object returnValue = delegator.doIt(conn);

      // Commit transaction
      tx.commit();

      // Return value that the underlying code needs to return
      return returnValue;

    } catch (Exception ex) {
      if (tx != null) {
        tx.rollback();
      }
      if (ex instanceof SqoopException) {
        throw (SqoopException) ex;
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0012, ex);
    } finally {
      if (tx != null) {
        tx.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public JdbcRepositoryTransaction getTransaction() {
    return repoContext.getTransactionFactory().get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector registerConnector(final MConnector mConnector) {

    return (MConnector) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        String connectorUniqueName = mConnector.getUniqueName();

        MConnector result = handler.findConnector(connectorUniqueName, conn);
        if (result == null) {
          handler.registerConnector(mConnector, conn);
        } else {
          if (!result.equals(mConnector)) {
            throw new SqoopException(RepositoryError.JDBCREPO_0013,
                "given[" + mConnector + "] found[" + result + "]");
          }
          mConnector.setPersistenceId(result.getPersistenceId());
        }

        return result;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerFramework(final MFramework mFramework) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        MFramework result = handler.findFramework(conn);
        if (result == null) {
          handler.registerFramework(mFramework, conn);
        } else {
          if (!result.equals(mFramework)) {
            throw new SqoopException(RepositoryError.JDBCREPO_0014,
                "given[" + mFramework + "] found[" + result + "]");
          }
          mFramework.setPersistenceId(result.getPersistenceId());
        }

        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createConnection(final MConnection connection) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(connection.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0015);
        }

        handler.createConnection(connection, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateConnection(final MConnection connection) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
       if(!connection.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0016);
        }
        if(!handler.existsConnection(connection.getPersistenceId(), conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017,
            "Invalid id: " + connection.getPersistenceId());
        }

        handler.updateConnection(connection, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteConnection(final long connectionId) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsConnection(connectionId, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017,
            "Invalid id: " + connectionId);
        }

        handler.deleteConnection(connectionId, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnection findConnection(final long connectionId) {
    return (MConnection) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findConnection(connectionId, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MConnection> findConnections() {
    return (List<MConnection>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findConnections(conn);
      }
    });
  }
}
