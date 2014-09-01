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

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * Defines the contract of a Repository used by Sqoop. A Repository allows
 * Sqoop to store metadata, statistics and other state relevant to Sqoop
 * Jobs in the system.
 */
public abstract class Repository {

  private static final Logger LOG = Logger.getLogger(Repository.class);

  public abstract RepositoryTransaction getTransaction();

  /**
   * Create or update disk data structures.
   *
   * This method will be called only if Sqoop server is enabled with changing
   * repository on disk structures. Repository should not change its disk structures
   * outside of this method. This method must be no-op in case that the structures
   * do not need any maintenance.
   */
  public abstract void createOrUpdateInternals();

  /**
   * Return true if internal repository structures exists and are suitable for use.
   *
   * This method should return false in case that the structures do exists, but
   * are not suitable for use or if they requires upgrade.
   *
   * @return Boolean values if internal structures are suitable for use
   */
  public abstract boolean haveSuitableInternals();

  /**
   * Registers given connector in the repository and return registered
   * variant. This method might return an exception in case that metadata for
   * given connector are already registered with different structure.
   *
   * @param mConnector the connector metadata to be registered
   * autoupgrade whether to upgrade framework automatically
   * @return Registered connector structure
   */
  public abstract MConnector registerConnector(MConnector mConnector, boolean autoUpgrade);

  /**
   * Search for connector with given name in repository.
   *
   * And return corresponding metadata structure.
   *
   * @param shortName Connector unique name
   * @return null if connector is not yet registered in repository or
   *   loaded representation.
   */
  public abstract MConnector findConnector(String shortName);

  /**
   * Get all connectors in repository
   *
   * @return List will all connectors in repository
   */
  public abstract List<MConnector> findConnectors();


  /**
   * Registers given framework in the repository and return registered
   * variant. This method might return an exception in case that metadata for
   * given framework are already registered with different structure.
   *
   * @param mFramework framework metadata to be registered
   * autoupgrade whether to upgrade framework automatically
   * @return Registered connector structure
   */
  public abstract MFramework registerFramework(MFramework mFramework, boolean autoUpgrade);

  /**
   * Save given connection to repository. This connection must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param connection Connection object to serialize into repository.
   */
  public abstract void createConnection(MConnection connection);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   */
  public abstract void updateConnection(MConnection connection);

  /**
   * Update given connection representation in repository. This connection
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param connection Connection object that should be updated in repository.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  public abstract void updateConnection(final MConnection connection,
    RepositoryTransaction tx);

  /**
   * Enable or disable connection with given id from metadata repository
   *
   * @param id Connection object that is going to be enabled or disabled
   * @param enabled enable or disable
   */
  public abstract void enableConnection(long id, boolean enabled);

  /**
   * Delete connection with given id from metadata repository.
   *
   * @param id Connection object that should be removed from repository
   */
  public abstract void deleteConnection(long id);

  /**
   * Find connection with given id in repository.
   *
   * @param id Connection id
   * @return Deserialized form of the connection that is saved in repository
   */
  public abstract MConnection findConnection(long id);

  /**
   * Get all connection objects.
   *
   * @return List will all saved connection objects
   */
  public abstract List<MConnection> findConnections();

  /**
   * Save given job to repository. This job object must not be already present
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be saved to repository
   */
  public abstract void createJob(MJob job);

  /**
   * Update given job metadata in repository. This object must already be saved
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be updated in the repository
   */
  public abstract void updateJob(MJob job);

  /**
   * Update given job metadata in repository. This object must already be saved
   * in repository otherwise exception will be thrown.
   *
   * @param job Job object that should be updated in the repository
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  public abstract void updateJob(MJob job, RepositoryTransaction tx);

  /**
   * Enable or disable job with given id from metadata repository
   *
   * @param id Job object that is going to be enabled or disabled
   * @param enabled Enable or disable
   */
  public abstract void enableJob(long id, boolean enabled);

  /**
   * Delete job with given id from metadata repository.
   *
   * @param id Job id that should be removed
   */
  public abstract void deleteJob(long id);

  /**
   * Find job object with given id.
   *
   * @param id Job id
   * @return Deserialized form of job loaded from repository
   */
  public abstract MJob findJob(long id);

  /**
   * Get all job objects.
   *
   * @return List of all jobs in the repository
   */
  public abstract List<MJob> findJobs();

  /**
   * Create new submission record in repository.
   *
   * @param submission Submission object that should be serialized to repository
   */
  public abstract void createSubmission(MSubmission submission);

  /**
   * Update already existing submission record in repository.
   *
   * @param submission Submission object that should be updated
   */
  public abstract void updateSubmission(MSubmission submission);

  /**
   * Remove submissions older then given date from repository.
   *
   * @param threshold Threshold date
   */
  public abstract void purgeSubmissions(Date threshold);

  /**
   * Return all unfinished submissions as far as repository is concerned.
   *
   * @return List of unfinished submissions
   */
  public abstract List<MSubmission> findSubmissionsUnfinished();

  /**
   * Return all submissions from repository
   *
   * @return List of all submissions
   */
  public abstract List<MSubmission> findSubmissions();

  /**
   * Return all submissions for given jobId.
   *
   * @return List of of submissions
   */
  public abstract List<MSubmission> findSubmissionsForJob(long jobId);

  /**
   * Find last submission for given jobId.
   *
   * @param jobId Job id
   * @return Most recent submission
   */
  public abstract MSubmission findSubmissionLastForJob(long jobId);

  /**
   * Retrieve connections which use the given connector.
   * @param connectorID Connector ID whose connections should be fetched
   * @return List of MConnections that use <code>connectorID</code>.
   */
  public abstract List<MConnection> findConnectionsForConnector(long
    connectorID);

  /**
   * Retrieve jobs which use the given connection.
   *
   * @param connectorID Connector ID whose jobs should be fetched
   * @return List of MJobs that use <code>connectionID</code>.
   */
  public abstract List<MJob> findJobsForConnector(long
    connectorID);

  /**
   * Update the connector with the new data supplied in the
   * <tt>newConnector</tt>. Also Update all forms associated with this
   * connector in the repository with the forms specified in
   * <tt>mConnector</tt>. <tt>mConnector </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param newConnector The new data to be inserted into the repository for
   *                     this connector.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void updateConnector(MConnector newConnector,
    RepositoryTransaction tx);


  /**
   * Update the framework with the new data supplied in the
   * <tt>mFramework</tt>. Also Update all forms associated with the framework
   * in the repository with the forms specified in
   * <tt>mFramework</tt>. <tt>mFramework </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param mFramework The new data to be inserted into the repository for
   *                     the framework.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void updateFramework(MFramework mFramework,
    RepositoryTransaction tx);


  /**
   * Delete all inputs for a job
   * @param jobId The id of the job whose inputs are to be deleted.
   * @param tx A transaction on the repository. This
   *           method will not call <code>begin, commit,
   *           rollback or close on this transaction.</code>
   */
  protected abstract void deleteJobInputs(long jobId, RepositoryTransaction tx);

  /**
   * Delete all inputs for a connection
   * @param connectionID The id of the connection whose inputs are to be
   *                     deleted.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void deleteConnectionInputs(long connectionID,
    RepositoryTransaction tx);

  private void deleteConnectionsAndJobs(List<MConnection> connections,
    List<MJob> jobs, RepositoryTransaction tx) {
    for (MJob job : jobs) {
      deleteJobInputs(job.getPersistenceId(), tx);
    }
    for (MConnection connection : connections) {
      deleteConnectionInputs(connection.getPersistenceId(), tx);
    }
  }

  /**
   * Upgrade the connector with the same {@linkplain MConnector#uniqueName}
   * in the repository with values from <code>newConnector</code>.
   * <p/>
   * All connections and jobs associated with this connector will be upgraded
   * automatically.
   *
   * @param oldConnector The old connector that should be upgraded.
   * @param newConnector New properties for the Connector that should be
   *                     upgraded.
   */
  public final void upgradeConnector(MConnector oldConnector, MConnector newConnector) {
    LOG.info("Upgrading metadata for connector: " + oldConnector.getUniqueName());
    long connectorID = oldConnector.getPersistenceId();
    newConnector.setPersistenceId(connectorID);
    /* Algorithms:
     * 1. Get an upgrader for the connector.
     * 2. Get all connections associated with the connector.
     * 3. Get all jobs associated with the connector.
     * 4. Delete the inputs for all of the jobs and connections (in that order)
     * 5. Remove all inputs and forms associated with the connector, and
     *    register the new forms and inputs.
     * 6. Create new connections and jobs with connector part being the ones
     *    returned by the upgrader.
     * 7. Validate new connections and jobs with connector's validator
     * 8. If any invalid connections or jobs detected, throw an exception
     *    and stop the bootup of Sqoop server
     * 9. Otherwise, Insert the connection inputs followed by job inputs (using
     *    updateJob and updateConnection)
     */
    RepositoryTransaction tx = null;
    try {
      SqoopConnector connector =
        ConnectorManager.getInstance().getConnector(newConnector
          .getUniqueName());

      Validator validator = connector.getValidator();

      boolean upgradeSuccessful = true;

      MetadataUpgrader upgrader = connector.getMetadataUpgrader();
      List<MConnection> connections = findConnectionsForConnector(
        connectorID);
      List<MJob> jobs = findJobsForConnector(connectorID);
      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      deleteConnectionsAndJobs(connections, jobs, tx);
      updateConnector(newConnector, tx);
      for (MConnection connection : connections) {
        // Make a new copy of the forms from the connector,
        // else the values will get set in the forms in the connector for
        // each connection.
        List<MForm> forms = newConnector.getConnectionForms().clone(false).getForms();
        MConnectionForms newConnectionForms = new MConnectionForms(forms);
        upgrader.upgrade(connection.getConnectorPart(), newConnectionForms);
        MConnection newConnection = new MConnection(connection, newConnectionForms, connection.getFrameworkPart());

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(connector.getConnectionConfigurationClass());
        FormUtils.fromForms(newConnection.getConnectorPart().getForms(), newConfigurationObject);

        Validation validation = validator.validateConnection(newConfigurationObject);
        if (validation.getStatus().canProceed()) {
          updateConnection(newConnection, tx);
        } else {
          logInvalidModelObject("connection", newConnection, validation);
          upgradeSuccessful = false;
        }
      }
      for (MJob job : jobs) {
        // Make a new copy of the forms from the connector,
        // else the values will get set in the forms in the connector for
        // each job.
        List<MForm> fromForms = newConnector.getJobForms(Direction.FROM).clone(false).getForms();
        List<MForm> toForms = newConnector.getJobForms(Direction.TO).clone(false).getForms();

        // New FROM direction forms, old TO direction forms.
        if (job.getConnectorId(Direction.FROM) == newConnector.getPersistenceId()) {
          MJobForms newFromJobForms = new MJobForms(fromForms);
          MJobForms oldToJobForms = job.getConnectorPart(Direction.TO);
          upgrader.upgrade(job.getConnectorPart(Direction.FROM), newFromJobForms);
          MJob newJob = new MJob(job, newFromJobForms, oldToJobForms, job.getFrameworkPart());
          updateJob(newJob, tx);

          // Transform form structures to objects for validations
//          Object newFromConfigurationObject = ClassUtils.instantiate(connector.getJobConfigurationClass(Direction.FROM));
//          FormUtils.fromForms(newJob.getConnectorPart(Direction.FROM).getForms(), newFromConfigurationObject);
//          Validation fromValidation = validator.validateJob(newFromConfigurationObject);
//          if (fromValidation.getStatus().canProceed()) {
//            updateJob(newJob, tx);
//          } else {
//            logInvalidModelObject("job", newJob, fromValidation);
//            upgradeSuccessful = false;
//          }
        }

        // Old FROM direction forms, new TO direction forms.
        if (job.getConnectorId(Direction.TO) == newConnector.getPersistenceId()) {
          MJobForms oldFromJobForms = job.getConnectorPart(Direction.FROM);
          MJobForms newToJobForms = new MJobForms(toForms);
          upgrader.upgrade(job.getConnectorPart(Direction.TO), newToJobForms);
          MJob newJob = new MJob(job, oldFromJobForms, newToJobForms, job.getFrameworkPart());
          updateJob(newJob, tx);

          // Transform form structures to objects for validations
//          Object newToConfigurationObject = ClassUtils.instantiate(connector.getJobConfigurationClass(Direction.TO));
//          FormUtils.fromForms(newJob.getConnectorPart(Direction.TO).getForms(), newToConfigurationObject);
//          Validation toValidation = validator.validateJob(newToConfigurationObject);
//          if (toValidation.getStatus().canProceed()) {
//            updateJob(newJob, tx);
//          } else {
//            logInvalidModelObject("job", newJob, toValidation);
//            upgradeSuccessful = false;
//          }
        }
      }

      if (upgradeSuccessful) {
        tx.commit();
      } else {
        throw new SqoopException(RepositoryError.JDBCREPO_0027);
      }
    } catch (SqoopException ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw ex;
    } catch (Exception ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0000, ex);
    } finally {
      if(tx != null) {
        tx.close();
      }
      LOG.info("Metadata upgrade finished for connector: " + oldConnector.getUniqueName());
    }
  }

  public final void upgradeFramework(MFramework framework) {
    LOG.info("Upgrading framework metadata");
    RepositoryTransaction tx = null;
    try {
      MetadataUpgrader upgrader = FrameworkManager.getInstance()
        .getMetadataUpgrader();
      List<MConnection> connections = findConnections();
      List<MJob> jobs = findJobs();

      Validator validator = FrameworkManager.getInstance().getValidator();

      boolean upgradeSuccessful = true;

      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      deleteConnectionsAndJobs(connections, jobs, tx);
      updateFramework(framework, tx);
      for (MConnection connection : connections) {
        // Make a new copy of the forms from the connector,
        // else the values will get set in the forms in the connector for
        // each connection.
        // @TODO(Abe): From/To connection forms.
        List<MForm> forms = framework.getConnectionForms().clone(false).getForms();
        MConnectionForms newConnectionForms = new MConnectionForms(forms);
        upgrader.upgrade(connection.getFrameworkPart(), newConnectionForms);
        MConnection newConnection = new MConnection(connection, connection.getConnectorPart(), newConnectionForms);

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(FrameworkManager.getInstance().getConnectionConfigurationClass());
        FormUtils.fromForms(newConnection.getFrameworkPart().getForms(), newConfigurationObject);

        Validation validation = validator.validateConnection(newConfigurationObject);
        if (validation.getStatus().canProceed()) {
          updateConnection(newConnection, tx);
        } else {
          logInvalidModelObject("connection", newConnection, validation);
          upgradeSuccessful = false;
        }
      }
      for (MJob job : jobs) {
        // Make a new copy of the forms from the framework,
        // else the values will get set in the forms in the connector for
        // each connection.
        List<MForm> forms = framework.getJobForms().clone(false).getForms();
        MJobForms newJobForms = new MJobForms(forms);
        upgrader.upgrade(job.getFrameworkPart(), newJobForms);
        MJob newJob = new MJob(job, job.getConnectorPart(Direction.FROM), job.getConnectorPart(Direction.TO), newJobForms);

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(FrameworkManager.getInstance().getJobConfigurationClass());
        FormUtils.fromForms(newJob.getFrameworkPart().getForms(), newConfigurationObject);

        Validation validation = validator.validateJob(newConfigurationObject);
        if (validation.getStatus().canProceed()) {
          updateJob(newJob, tx);
        } else {
          logInvalidModelObject("job", newJob, validation);
          upgradeSuccessful = false;
        }
      }

      if (upgradeSuccessful) {
        tx.commit();
      } else {
        throw new SqoopException(RepositoryError.JDBCREPO_0027);
      }
    } catch (SqoopException ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw ex;
    } catch (Exception ex) {
      if(tx != null) {
        tx.rollback();
      }
      throw new SqoopException(RepositoryError.JDBCREPO_0000, ex);
    } finally {
      if(tx != null) {
        tx.close();
      }
      LOG.info("Framework metadata upgrade finished");
    }
  }

  private void logInvalidModelObject(String objectType, MPersistableEntity entity, Validation validation) {
    LOG.error("Upgrader created invalid " + objectType + " with id" + entity.getPersistenceId());

    for(Map.Entry<Validation.FormInput, Validation.Message> entry : validation.getMessages().entrySet()) {
      LOG.error("\t" + entry.getKey() + ": " + entry.getValue());
    }
  }
}
