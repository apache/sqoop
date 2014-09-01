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
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;

public class JdbcRepository extends Repository {

  private static final Logger LOG = Logger.getLogger(JdbcRepository.class);

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
    Object doIt(Connection conn) throws Exception;
  }

  private Object doWithConnection(DoWithConnection delegator) {
    return doWithConnection(delegator, null);
  }

  /**
   * Handle transaction and connection functionality and delegate action to
   * given delegator.
   *
   * @param delegator Code for specific action
   * @param tx The transaction to use for the operation. If a transaction is
   *           specified, this method will not commit, rollback or close it.
   *           If null, a new transaction will be created - which will be
   *           committed/closed/rolled back.
   * @return Arbitrary value
   */
  private Object doWithConnection(DoWithConnection delegator,
    JdbcRepositoryTransaction tx) {
    boolean shouldCloseTxn = false;

    try {
      // Get transaction and connection
      Connection conn;
      if (tx == null) {
        tx = getTransaction();
        shouldCloseTxn = true;
        tx.begin();
      }
      conn = tx.getConnection();

      // Delegate the functionality to our delegator
      Object returnValue = delegator.doIt(conn);

      if (shouldCloseTxn) {
        // Commit transaction
        tx.commit();
      }

      // Return value that the underlying code needs to return
      return returnValue;

    } catch (SqoopException ex) {
      throw  ex;
    } catch (Exception ex) {
      if (tx != null && shouldCloseTxn) {
        tx.rollback();
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0012, ex);
    } finally {
      if (tx != null && shouldCloseTxn) {
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
  public void createOrUpdateInternals() {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        LOG.info("Creating repository schema objects");
        handler.createOrUpdateInternals(conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean haveSuitableInternals() {
    return (Boolean) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.haveSuitableInternals(conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector registerConnector(final MConnector mConnector, final boolean autoUpgrade) {

    return (MConnector) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        String connectorUniqueName = mConnector.getUniqueName();

        MConnector result = handler.findConnector(connectorUniqueName, conn);
        if (result == null) {
          handler.registerConnector(mConnector, conn);
          return mConnector;
        } else {
          // Same connector, check if the version is the same.
          // For now, use the "string" versions itself - later we should
          // probably include a build number or something that is
          // monotonically increasing.
          if (result.getUniqueName().equals(mConnector.getUniqueName()) &&
            mConnector.getVersion().compareTo(result.getVersion()) > 0) {
            if (autoUpgrade) {
              upgradeConnector(result, mConnector);
              return mConnector;
            } else {
              throw new SqoopException(RepositoryError.JDBCREPO_0026,
                "Connector: " + mConnector.getUniqueName());
            }
          }
          if (!result.equals(mConnector)) {
            throw new SqoopException(RepositoryError.JDBCREPO_0013,
              "Connector: " + mConnector.getUniqueName()
                + " given: " + mConnector
                + " found: " + result);
          }
          return result;
        }
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector findConnector(final String shortName) {
    return (MConnector) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findConnector(shortName, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
    @Override
    public List<MConnector> findConnectors() {
      return (List<MConnector>) doWithConnection(new DoWithConnection() {
          @Override
          public Object doIt(Connection conn) {
              return handler.findConnectors(conn);
          }
      });
    }

  /**
   * {@inheritDoc}
   */
  @Override
  public MFramework registerFramework(final MFramework mFramework, final boolean autoUpgrade) {
    return (MFramework) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        MFramework result = handler.findFramework(conn);
        if (result == null) {
          handler.registerFramework(mFramework, conn);
          return mFramework;
        } else {
          // We're currently not serializing framework version into repository
          // so let's just compare the structure to see if we need upgrade.
          if(!mFramework.equals(result)) {
            if (autoUpgrade) {
              upgradeFramework(mFramework);
              return mFramework;
            } else {
              throw new SqoopException(RepositoryError.JDBCREPO_0026,
                "Framework: " + mFramework.getPersistenceId());
            }
          }
          return result;
        }
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
    updateConnection(connection, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateConnection(final MConnection connection,
    RepositoryTransaction tx) {
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
    }, (JdbcRepositoryTransaction) tx);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableConnection(final long connectionId, final boolean enabled) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsConnection(connectionId, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017,
            "Invalid id: " + connectionId);
        }

        handler.enableConnection(connectionId, enabled, conn);
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
        if(handler.inUseConnection(connectionId, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0021,
            "Id in use: " + connectionId);
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void createJob(final MJob job) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(job.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0018);
        }

        handler.createJob(job, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateJob(final MJob job) {
    updateJob(job, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateJob(final MJob job, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
       if(!job.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0019);
        }
        if(!handler.existsJob(job.getPersistenceId(), conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid id: " + job.getPersistenceId());
        }

        handler.updateJob(job, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableJob(final long id, final boolean enabled) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsJob(id, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid id: " + id);
        }

        handler.enableJob(id, enabled, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(final long id) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsJob(id, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid id: " + id);
        }
        if(handler.inUseJob(id, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0022,
            "Id in use: " + id);
        }

        handler.deleteJob(id, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MJob findJob(final long id) {
    return (MJob) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findJob(id, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MJob> findJobs() {
   return (List<MJob>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findJobs(conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createSubmission(final MSubmission submission) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(submission.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0023);
        }

        handler.createSubmission(submission, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateSubmission(final MSubmission submission) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
       if(!submission.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0024);
        }
        if(!handler.existsSubmission(submission.getPersistenceId(), conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0025,
            "Invalid id: " + submission.getPersistenceId());
        }

        handler.updateSubmission(submission, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeSubmissions(final Date threshold) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        handler.purgeSubmissions(threshold, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MSubmission> findSubmissionsUnfinished() {
    return (List<MSubmission>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findSubmissionsUnfinished(conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MSubmission> findSubmissions() {
    return (List<MSubmission>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findSubmissions(conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MSubmission> findSubmissionsForJob(final long jobId) {
    return (List<MSubmission>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        if(!handler.existsJob(jobId, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid id: " + jobId);
        }
        return handler.findSubmissionsForJob(jobId, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MSubmission findSubmissionLastForJob(final long jobId) {
    return (MSubmission) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsJob(jobId, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid id: " + jobId);
        }
        return handler.findSubmissionLastForJob(jobId, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MConnection> findConnectionsForConnector(final long
    connectorID) {
    return (List<MConnection>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findConnectionsForConnector(connectorID, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<MJob> findJobsForConnector(final long connectorID) {
    return (List<MJob>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findJobsForConnector(connectorID, conn);
      }
    });
  }

  @Override
  protected void deleteJobInputs(final long jobID, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.deleteJobInputs(jobID, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);

  }

  @Override
  protected void deleteConnectionInputs(final long connectionID,
    RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.deleteConnectionInputs(connectionID, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateConnector(final MConnector newConnector,
    RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.updateConnector(newConnector, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }


  protected void updateFramework(final MFramework mFramework,
    RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.updateFramework(mFramework, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }
}
