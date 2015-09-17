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
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
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
   * Private interface to wrap specific code that requires fresh link to
   * repository with general code that will get the link and handle
   * exceptions.
   */
  private interface DoWithConnection {
    /**
     * Do what is needed to be done with given link object.
     *
     * @param conn Connection to the repository.
     * @return Arbitrary value
     */
    Object doIt(Connection conn) throws Exception;
  }

  private Object doWithConnection(DoWithConnection delegator) {
    return doWithConnection(delegator, null);
  }

  /**
   * Handle transaction and link functionality and delegate action to
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
      // Get transaction and link
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
  public void createOrUpgradeRepository() {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        LOG.info("Creating repository schema objects");
        handler.createOrUpgradeRepository(conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isRepositorySuitableForUse() {
    return (Boolean) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.isRepositorySuitableForUse(conn);
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

        MConnector connectorResult = handler.findConnector(connectorUniqueName, conn);
        if (connectorResult == null) {
          handler.registerConnector(mConnector, conn);
          return mConnector;
        } else {
          if (connectorResult.getUniqueName().equals(mConnector.getUniqueName()) &&
            mConnector.getVersion().compareTo(connectorResult.getVersion()) > 0) {
            if (autoUpgrade) {
              upgradeConnector(connectorResult, mConnector);
              return mConnector;
            } else {
              throw new SqoopException(RepositoryError.JDBCREPO_0026,
                "Connector: " + mConnector.getUniqueName());
            }
          }
          if (!connectorResult.equals(mConnector)) {
            throw new SqoopException(RepositoryError.JDBCREPO_0013,
              "Connector: " + mConnector.getUniqueName()
                + " given: " + mConnector
                + " found: " + connectorResult);
          }
          return connectorResult;
        }
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MConnector findConnector(final long id) {
    return (MConnector) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findConnector(id, conn);
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
    @SuppressWarnings("unchecked")
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
  public MDriver registerDriver(final MDriver mDriver, final boolean autoUpgrade) {
    return (MDriver) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        MDriver existingDriver = handler.findDriver(mDriver.getUniqueName(), conn);
        if (existingDriver == null) {
          handler.registerDriver(mDriver, conn);
          return mDriver;
        } else {
          // We're currently not serializing version into repository
          // so let's just compare the structure to see if we need upgrade.
          if(!mDriver.equals(existingDriver)) {
            if (autoUpgrade) {
              upgradeDriver(mDriver);
              return mDriver;
            } else {
              throw new SqoopException(RepositoryError.JDBCREPO_0026,
                "Driver: " + mDriver.getPersistenceId());
            }
          }
          return existingDriver;
        }
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MDriver findDriver(final String shortName) {
    return (MDriver) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findDriver(shortName, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createLink(final MLink link) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(link.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0015);
        }

        handler.createLink(link, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLink(final MLink link) {
    updateLink(link, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLink(final MLink link, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if (!link.hasPersistenceId()) {
          throw new SqoopException(RepositoryError.JDBCREPO_0016);
        }
        if (!handler.existsLink(link.getName(), conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017, "Invalid name: "
              + link.getName());
        }

        handler.updateLink(link, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableLink(final String linkName, final boolean enabled) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsLink(linkName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017,
            "Invalid name: " + linkName);
        }

        handler.enableLink(linkName, enabled, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteLink(final String linkName) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsLink(linkName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0017,
            "Invalid name: " + linkName);
        }
        if(handler.inUseLink(linkName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0021,
            "Name in use: " + linkName);
        }

        handler.deleteLink(linkName, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MLink findLink(final long id) {
    return (MLink) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findLink(id, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MLink findLink(final String name) {
    return (MLink) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findLink(name, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MLink> findLinks() {
    return (List<MLink>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findLinks(conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<MLink> findLinksForConnector(final String connectorName) {
    return (List<MLink>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findLinksForConnector(connectorName, conn);
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
        if(!handler.existsJob(job.getName(), conn)) {
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
  public void enableJob(final String jobName, final boolean enabled) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if(!handler.existsJob(jobName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid name: " + jobName);
        }

        handler.enableJob(jobName, enabled, conn);
        return null;
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteJob(final String jobName) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if (!handler.existsJob(jobName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020, "Invalid id: " + jobName);
        }
        if (handler.inUseJob(jobName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0022, "Name in use: " + jobName);
        }

        handler.deleteJob(jobName, conn);
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
  @Override
  public MJob findJob(final String name) {
    return (MJob) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findJob(name, conn);
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
  @SuppressWarnings("unchecked")
  @Override
  public List<MJob> findJobsForConnector(final long connectorId) {
    return (List<MJob>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        return handler.findJobsForConnector(connectorId, conn);
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
  public List<MSubmission> findUnfinishedSubmissions() {
    return (List<MSubmission>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        return handler.findUnfinishedSubmissions(conn);
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
  public List<MSubmission> findSubmissionsForJob(final String jobName) {
    return (List<MSubmission>) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        if(!handler.existsJob(jobName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020,
            "Invalid name: " + jobName);
        }
        return handler.findSubmissionsForJob(jobName, conn);
      }
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MSubmission findLastSubmissionForJob(final String jobName) {
    return (MSubmission) doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) {
        if (!handler.existsJob(jobName, conn)) {
          throw new SqoopException(RepositoryError.JDBCREPO_0020, "Invalid name: " + jobName);
        }
        return handler.findLastSubmissionForJob(jobName, conn);
      }
    });
  }

  @Override
  protected void deleteJobInputs(final String jobName, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.deleteJobInputs(jobName, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }

  @Override
  protected void deleteLinkInputs(final String linkName, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.deleteLinkInputs(linkName, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void upgradeConnectorAndConfigs(final MConnector newConnector,
    RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.upgradeConnectorAndConfigs(newConnector, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }


  protected void upgradeDriverAndConfigs(final MDriver mDriver, RepositoryTransaction tx) {
    doWithConnection(new DoWithConnection() {
      @Override
      public Object doIt(Connection conn) throws Exception {
        handler.upgradeDriverAndConfigs(mDriver, conn);
        return null;
      }
    }, (JdbcRepositoryTransaction) tx);
  }
}
