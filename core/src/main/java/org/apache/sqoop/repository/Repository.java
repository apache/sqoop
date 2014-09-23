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

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.RepositoryUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;


/**
 * Defines the contract for repository used by Sqoop. A Repository allows
 * Sqoop to store entities such as connectors, links, jobs, submissions and its related configs,
 * statistics and other state relevant the entities in the store
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
   * variant. This method might return an exception in case that 
   * given connector are already registered with different structure.
   *
   * @param mConnector the connector to be registered
   * autoupgrade whether to upgrade driver config automatically
   * @return Registered connector structure
   */
  public abstract MConnector registerConnector(MConnector mConnector, boolean autoUpgrade);

  /**
   * Search for connector with given name in repository.
   *
   * And return corresponding entity structure.
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
   * Registers given driverConfig in the repository and return registered
   * variant. This method might return an exception in case that the
   * given driverConfig are already registered with different structure.
   *
   * @param mDriverConfig driverConfig to be registered
   * autoupgrade whether to upgrade driverConfig automatically
   * @return Registered connector structure
   */
  public abstract MDriverConfig registerDriverConfig(MDriverConfig mDriverConfig, boolean autoUpgrade);

  /**
   * Save given link to repository. This link must not be already
   * present in the repository otherwise exception will be thrown.
   *
   * @param link link object to serialize into repository.
   */
  public abstract void createLink(MLink link);

  /**
   * Update given link representation in repository. This link
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param link link object that should be updated in repository.
   */
  public abstract void updateLink(MLink link);

  /**
   * Update given link representation in repository. This link
   * object must already exists in the repository otherwise exception will be
   * thrown.
   *
   * @param link Link object that should be updated in repository.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  public abstract void updateLink(final MLink link, RepositoryTransaction tx);

  /**
   * Enable or disable Link with given id from the repository
   *
   * @param id Link object that is going to be enabled or disabled
   * @param enabled enable or disable
   */
  public abstract void enableLink(long id, boolean enabled);

  /**
   * Delete Link with given id from the repository.
   *
   * @param id Link object that should be removed from repository
   */
  public abstract void deleteLink(long id);

  /**
   * Find link with given id in repository.
   *
   * @param id Link id
   * @return Deserialized form of the link that is saved in repository
   */
  public abstract MLink findLink(long id);

  /**
   * Get all Link objects.
   *
   * @return List will all saved link objects
   */
  public abstract List<MLink> findLinks();

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
   * Retrieve links which use the given connector.
   * @param connectorID Connector ID whose links should be fetched
   * @return List of MLink that use <code>connectorID</code>.
   */
  public abstract List<MLink> findLinksForConnector(long
    connectorID);

  /**
   * Retrieve jobs which use the given link.
   *
   * @param connectorID Connector ID whose jobs should be fetched
   * @return List of MJobs that use <code>linkID</code>.
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
  protected abstract void updateConnector(MConnector newConnector, RepositoryTransaction tx);

  /**
   * Update the driverConfig with the new data supplied in the
   * <tt>mDriverConfig</tt>. Also Update all forms associated with the driverConfig
   * in the repository with the forms specified in
   * <tt>mDriverConfig</tt>. <tt>mDriverConfig </tt> must
   * minimally have the connectorID and all required forms (including ones
   * which may not have changed). After this operation the repository is
   * guaranteed to only have the new forms specified in this object.
   *
   * @param mDriverConfig The new data to be inserted into the repository for
   *                     the driverConfig.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void updateDriverConfig(MDriverConfig mDriverConfig, RepositoryTransaction tx);

  /**
   * Delete all inputs for a job
   * @param jobId The id of the job whose inputs are to be deleted.
   * @param tx A transaction on the repository. This
   *           method will not call <code>begin, commit,
   *           rollback or close on this transaction.</code>
   */
  protected abstract void deleteJobInputs(long jobId, RepositoryTransaction tx);

  /**
   * Delete all inputs for a link
   * @param linkId The id of the link whose inputs are to be
   *                     deleted.
   * @param tx The repository transaction to use to push the data to the
   *           repository. If this is null, a new transaction will be created.
   *           method will not call begin, commit,
   *           rollback or close on this transaction.
   */
  protected abstract void deleteLinkInputs(long linkId, RepositoryTransaction tx);

  private void deletelinksAndJobs(List<MLink> links, List<MJob> jobs, RepositoryTransaction tx) {
    for (MJob job : jobs) {
      deleteJobInputs(job.getPersistenceId(), tx);
    }
    for (MLink link : links) {
      deleteLinkInputs(link.getPersistenceId(), tx);
    }
  }

  /**
   * Upgrade the connector with the same {@linkplain MConnector#uniqueName}
   * in the repository with values from <code>newConnector</code>.
   * <p/>
   * All links and jobs associated with this connector will be upgraded
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
     * 2. Get all links associated with the connector.
     * 3. Get all jobs associated with the connector.
     * 4. Delete the inputs for all of the jobs and links (in that order)
     * 5. Remove all inputs and forms associated with the connector, and
     *    register the new forms and inputs.
     * 6. Create new links and jobs with connector part being the ones
     *    returned by the upgrader.
     * 7. Validate new links and jobs with connector's validator
     * 8. If any invalid links or jobs detected, throw an exception
     *    and stop the bootup of Sqoop server
     * 9. Otherwise, Insert the link inputs followed by job inputs (using
     *    updateJob and updatelink)
     */
    RepositoryTransaction tx = null;
    try {
      SqoopConnector connector =
        ConnectorManager.getInstance().getConnector(newConnector
          .getUniqueName());

      Validator validator = connector.getValidator();

      boolean upgradeSuccessful = true;

      RepositoryUpgrader upgrader = connector.getRepositoryUpgrader();
      List<MLink> links = findLinksForConnector(
        connectorID);
      List<MJob> jobs = findJobsForConnector(connectorID);
      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      deletelinksAndJobs(links, jobs, tx);
      updateConnector(newConnector, tx);
      for (MLink link : links) {
        // Make a new copy of the forms from the connector,
        // else the values will get set in the forms in the connector for
        // each link.
        List<MForm> forms = newConnector.getConnectionForms().clone(false).getForms();
        MConnectionForms newlinkForms = new MConnectionForms(forms);
        upgrader.upgrade(link.getConnectorPart(), newlinkForms);
        MLink newlink = new MLink(link, newlinkForms, link.getFrameworkPart());

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(connector.getLinkConfigurationClass());
        FormUtils.fromForms(newlink.getConnectorPart().getForms(), newConfigurationObject);

        Validation validation = validator.validateLink(newConfigurationObject);
        if (validation.getStatus().canProceed()) {
          updateLink(newlink, tx);
        } else {
          logInvalidModelObject("link", newlink, validation);
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

  public final void upgradeDriverConfig(MDriverConfig driverConfig) {
    LOG.info("Upgrading driver config");
    RepositoryTransaction tx = null;
    try {
      RepositoryUpgrader upgrader = Driver.getInstance()
        .getDriverConfigRepositoryUpgrader();
      List<MLink> links = findLinks();
      List<MJob> jobs = findJobs();

      Validator validator = Driver.getInstance().getValidator();

      boolean upgradeSuccessful = true;

      // -- BEGIN TXN --
      tx = getTransaction();
      tx.begin();
      deletelinksAndJobs(links, jobs, tx);
      updateDriverConfig(driverConfig, tx);
      for (MLink link : links) {
        // Make a new copy of the forms from the connector,
        // else the values will get set in the forms in the connector for
        // each link.
        // @TODO(Abe): From/To link forms.
        List<MForm> forms = driverConfig.getConnectionForms().clone(false).getForms();
        MConnectionForms newlinkForms = new MConnectionForms(forms);
        upgrader.upgrade(link.getFrameworkPart(), newlinkForms);
        MLink newlink = new MLink(link, link.getConnectorPart(), newlinkForms);

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(Driver.getInstance().getLinkConfigurationClass());
        FormUtils.fromForms(newlink.getFrameworkPart().getForms(), newConfigurationObject);

        Validation validation = validator.validateLink(newConfigurationObject);
        if (validation.getStatus().canProceed()) {
          updateLink(newlink, tx);
        } else {
          logInvalidModelObject("link", newlink, validation);
          upgradeSuccessful = false;
        }
      }
      for (MJob job : jobs) {
        // Make a new copy of the forms from the framework,
        // else the values will get set in the forms in the connector for
        // each link.
        List<MForm> forms = driverConfig.getJobForms().clone(false).getForms();
        MJobForms newJobForms = new MJobForms(forms);
        upgrader.upgrade(job.getFrameworkPart(), newJobForms);
        MJob newJob = new MJob(job, job.getConnectorPart(Direction.FROM), job.getConnectorPart(Direction.TO), newJobForms);

        // Transform form structures to objects for validations
        Object newConfigurationObject = ClassUtils.instantiate(Driver.getInstance().getJobConfigurationClass());
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
      LOG.info("Driver config upgrade finished");
    }
  }

  private void logInvalidModelObject(String objectType, MPersistableEntity entity, Validation validation) {
    LOG.error("Upgrader created invalid " + objectType + " with id" + entity.getPersistenceId());

    for(Map.Entry<Validation.FormInput, Validation.Message> entry : validation.getMessages().entrySet()) {
      LOG.error("\t" + entry.getKey() + ": " + entry.getValue());
    }
  }
}
