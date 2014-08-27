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
package org.apache.sqoop.client;

import org.apache.sqoop.client.request.SqoopRequests;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ValidationResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Sqoop client API.
 *
 * High level Sqoop client API to communicate with Sqoop server. Current
 * implementation is not thread safe.
 *
 * SqoopClient is keeping cache of objects that are unlikely to be changed
 * (Resources, Connector structures). Volatile structures (Connections, Jobs)
 * are not cached.
 */
public class SqoopClient {

  /**
   * Underlying request object to fetch data from Sqoop server.
   */
  private SqoopRequests requests;

  /**
   * True if user retrieved all connectors at once.
   */
  private boolean allConnectors;

  /**
   * All cached bundles for all connectors.
   */
  private Map<Long, ResourceBundle> bundles;

  /**
   * Cached framework bundle.
   */
  private ResourceBundle frameworkBundle;

  /**
   * All cached connectors.
   */
  private Map<Long, MConnector> connectors;

  /**
   * Cached framework.
   */
  private MFramework framework;

  /**
   * Status flags used when updating the submission callback status
   */
  private enum SubmissionStatus {
    SUBMITTED,
    UPDATED,
    FINISHED
  }

  public SqoopClient(String serverUrl) {
    requests = new SqoopRequests();
    setServerUrl(serverUrl);
  }

  /**
   * Set new server URL.
   *
   * Setting new URL will also clear all caches used by the client.
   *
   * @param serverUrl Server URL
   */
  public void setServerUrl(String serverUrl) {
    requests.setServerUrl(serverUrl);
    clearCache();
  }

  /**
   * Set arbitrary request object.
   *
   * @param requests SqoopRequests object
   */
  public void setSqoopRequests(SqoopRequests requests) {
    this.requests = requests;
    clearCache();
  }

  /**
   * Clear internal cache.
   */
  public void clearCache() {
    bundles = new HashMap<Long, ResourceBundle>();
    frameworkBundle = null;
    connectors = new HashMap<Long, MConnector>();
    framework = null;
    allConnectors = false;
  }

  /**
   * Get connector with given id.
   *
   * @param cid Connector id.
   * @return
   */
  public MConnector getConnector(long cid) {
    if(connectors.containsKey(cid)) {
      return connectors.get(cid).clone(false);
    }

    retrieveConnector(cid);
    return connectors.get(cid).clone(false);
  }

  /**
   * Return connector with given name.
   *
   * @param connectorName Connector name
   * @return Connector model or NULL if the connector do not exists.
   */
  public MConnector getConnector(String connectorName) {
    // Firstly try if we have this connector already in cache
    MConnector connector = getConnectorFromCache(connectorName);
    if(connector != null) return connector;

    // If the connector wasn't in cache and we have all connectors,
    // it simply do not exists.
    if(allConnectors) return null;

    // Retrieve all connectors from server
    getConnectors();
    return getConnectorFromCache(connectorName);
  }

  /**
   * Iterate over cached connectors and return connector of given name.
   * This method will not contact server in case that the connector is
   * not found in the cache.
   *
   * @param connectorName Connector name
   * @return
   */
  private MConnector getConnectorFromCache(String connectorName) {
    for(MConnector connector : connectors.values()) {
      if(connector.getUniqueName().equals(connectorName)) {
        return connector;
      }
    }

    return null;
  }

  /**
   * Retrieve connector structure from server and cache it.
   *
   * @param cid Connector id
   */
  private void retrieveConnector(long cid) {
    ConnectorBean request = requests.readConnector(cid);
    connectors.put(cid, request.getConnectors().get(0));
    bundles.put(cid, request.getResourceBundles().get(cid));
  }

  /**
   * Get list of all connectors.
   *
   * @return
   */
  public Collection<MConnector> getConnectors() {
    if(allConnectors) {
      return connectors.values();
    }

    ConnectorBean bean = requests.readConnector(null);
    allConnectors = true;
    for(MConnector connector : bean.getConnectors()) {
      connectors.put(connector.getPersistenceId(), connector);
    }
    bundles = bean.getResourceBundles();

    return connectors.values();
  }

  /**
   * Get resource bundle for given connector.
   *
   * @param cid Connector id.
   * @return
   */
  public ResourceBundle getResourceBundle(long cid) {
    if(bundles.containsKey(cid)) {
      return bundles.get(cid);
    }

    retrieveConnector(cid);
    return bundles.get(cid);
  }

  /**
   * Return framework metadata.
   *
   * @return
   */
  public MFramework getFramework() {
    if(framework != null) {
      return framework.clone(false);
    }

    retrieveFramework();
    return framework.clone(false);

  }

  /**
   * Retrieve framework structure and cache it.
   */
  private void retrieveFramework() {
    FrameworkBean request =  requests.readFramework();
    framework = request.getFramework();
    frameworkBundle = request.getResourceBundle();
  }

  /**
   * Return framework bundle.
   *
   * @return
   */
  public ResourceBundle getFrameworkResourceBundle() {
    if(frameworkBundle != null) {
      return frameworkBundle;
    }

    retrieveFramework();
    return frameworkBundle;
  }

  /**
   * Create new connection object for given connector.
   *
   * @param cid Connector id
   * @return
   */
  public MConnection newConnection(long cid) {
    return new MConnection(
      cid,
      getConnector(cid).getConnectionForms(),
      getFramework().getConnectionForms()
    );
  }

  /**
   * Create new connection object for given connector.
   *
   * @param connectorName Connector name
   * @return
   */
  public MConnection newConnection(String connectorName) {
    MConnector connector = getConnector(connectorName);
    if(connector == null) {
      throw new SqoopException(ClientError.CLIENT_0003, connectorName);
    }

    return newConnection(connector.getPersistenceId());
  }

  /**
   * Retrieve connection with given id.
   *
   * @param xid Connnection id
   * @return
   */
  public MConnection getConnection(long xid) {
    return requests.readConnection(xid).getConnections().get(0);
  }

  /**
   * Retrieve list of all connections.
   *
   * @return
   */
  public List<MConnection> getConnections() {
    return requests.readConnection(null).getConnections();
  }

  /**
   * Create the connection on server.
   *
   * @param connection Connection that should be created
   * @return
   */
  public Status createConnection(MConnection connection) {
    return applyValidations(requests.createConnection(connection), connection);
  }

  /**
   * Update connection on the server.
   *
   * @param connection Connection that should be updated
   * @return
   */
  public Status updateConnection(MConnection connection) {
    return applyValidations(requests.updateConnection(connection), connection);
  }

  /**
   * Enable/disable connection with given id
   *
   * @param xid Connection id
   * @param enabled Enable or disable
   */
  public void enableConnection(long xid, boolean enabled) {
    requests.enableConnection(xid, enabled);
  }

  /**
   * Delete connection with given id.
   *
   * @param xid Connection id
   */
  public void deleteConnection(long xid) {
    requests.deleteConnection(xid);
  }

  /**
   * Create new job the for given connections.
   *
   * @param fromXid From Connection id
   * @param toXid To Connection id
   * @return
   */
  public MJob newJob(long fromXid, long toXid) {
    MConnection fromConnection = getConnection(fromXid);
    MConnection toConnection = getConnection(toXid);

    return new MJob(
      fromConnection.getConnectorId(),
      toConnection.getConnectorId(),
      fromConnection.getPersistenceId(),
      toConnection.getPersistenceId(),
      getConnector(fromConnection.getConnectorId()).getJobForms(Direction.FROM),
      getConnector(toConnection.getConnectorId()).getJobForms(Direction.TO),
      getFramework().getJobForms()
    );
  }

  /**
   * Retrieve job for given id.
   *
   * @param jid Job id
   * @return
   */
  public MJob getJob(long jid) {
    return requests.readJob(jid).getJobs().get(0);
  }

  /**
   * Retrieve list of all jobs.
   *
   * @return
   */
  public List<MJob> getJobs() {
    return requests.readJob(null).getJobs();
  }

  /**
   * Create job on server.
   *
   * @param job Job that should be created
   * @return
   */
  public Status createJob(MJob job) {
    return applyValidations(requests.createJob(job), job);
  }

  /**
   * Update job on server.
   * @param job Job that should be updated
   * @return
   */
  public Status updateJob(MJob job) {
    return applyValidations(requests.updateJob(job), job);
  }

  /**
   * Enable/disable job with given id
   *
   * @param jid Job that is going to be enabled/disabled
   * @param enabled Enable or disable
   */
  public void enableJob(long jid, boolean enabled) {
    requests.enableJob(jid, enabled);
  }

  /**
   * Delete job with given id.
   *
   * @param jid Job id
   */
  public void deleteJob(long jid) {
    requests.deleteJob(jid);
  }

  /**
   * Start job with given id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission startSubmission(long jid) {
    return requests.createSubmission(jid).getSubmissions().get(0);
  }

  /**
   * Method used for synchronous job submission.
   * Pass null to callback parameter if submission status is not required and after completion
   * job execution returns MSubmission which contains final status of submission.
   * @param jid - Job ID
   * @param callback - User may set null if submission status is not required, else callback methods invoked
   * @param pollTime - Server poll time
   * @return MSubmission - Final status of job submission
   * @throws InterruptedException
   */
  public MSubmission startSubmission(long jid, SubmissionCallback callback, long pollTime) throws InterruptedException {
    if(pollTime <= 0) {
      throw new SqoopException(ClientError.CLIENT_0002);
    }
    boolean first = true;
    MSubmission submission = requests.createSubmission(jid).getSubmissions().get(0);
    while(submission.getStatus().isRunning()) {
      if(first) {
        submissionCallback(callback, submission, SubmissionStatus.SUBMITTED);
        first = false;
      } else {
        submissionCallback(callback, submission, SubmissionStatus.UPDATED);
      }
      Thread.sleep(pollTime);
      submission = getSubmissionStatus(jid);
    }
    submissionCallback(callback, submission, SubmissionStatus.FINISHED);
    return submission;
  }

  /**
   * Invokes the callback's methods with MSubmission object
   * based on SubmissionStatus. If callback is null, no operation performed.
   * @param callback
   * @param submission
   * @param status
   */
  private void submissionCallback(SubmissionCallback callback,
      MSubmission submission, SubmissionStatus status) {
    if(callback == null) {
      return;
    }
    switch (status) {
    case SUBMITTED:
      callback.submitted(submission);
      break;
    case UPDATED:
      callback.updated(submission);
      break;
    case FINISHED:
      callback.finished(submission);
    }
  }

  /**
   * Stop job with given id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission stopSubmission(long jid) {
    return requests.deleteSubmission(jid).getSubmissions().get(0);
  }

  /**
   * Get status for given job id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission getSubmissionStatus(long jid) {
    return requests.readSubmission(jid).getSubmissions().get(0);
  }

  /**
   * Retrieve list of all submissions.
   *
   * @return
   */
  public List<MSubmission> getSubmissions() {
    return requests.readHistory(null).getSubmissions();
  }

  /**
   * Retrieve list of submissions for given jobId.
   *
   * @param jid Job id
   * @return
   */
  public List<MSubmission> getSubmissionsForJob(long jid) {
    return requests.readHistory(jid).getSubmissions();
  }

  private Status applyValidations(ValidationResultBean bean, MConnection connection) {
    ValidationResult connector = bean.getValidationResults()[0];
    ValidationResult framework = bean.getValidationResults()[1];

    // Apply validation results
    FormUtils.applyValidation(connection.getConnectorPart().getForms(), connector);
    FormUtils.applyValidation(connection.getFrameworkPart().getForms(), framework);

    Long id = bean.getId();
    if(id != null) {
      connection.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  private Status applyValidations(ValidationResultBean bean, MJob job) {
    ValidationResult fromConnector = bean.getValidationResults()[0];
    ValidationResult toConnector = bean.getValidationResults()[1];
    ValidationResult framework = bean.getValidationResults()[2];

    // Apply validation results
    // @TODO(Abe): From/To validation.
    FormUtils.applyValidation(
        job.getConnectorPart(Direction.FROM).getForms(),
        fromConnector);
    FormUtils.applyValidation(job.getFrameworkPart().getForms(), framework);
    FormUtils.applyValidation(
        job.getConnectorPart(Direction.TO).getForms(),
        toConnector);

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(fromConnector.getStatus(), framework.getStatus(), toConnector.getStatus());
  }
}
