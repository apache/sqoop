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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.client.request.SqoopResourceRequests;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.*;
import org.apache.sqoop.model.*;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.Status;

/**
 * Sqoop client API.
 *
 * High level Sqoop client API to communicate with Sqoop server. Current
 * implementation is not thread safe.
 *
 * SqoopClient is keeping cache of objects that are unlikely to be changed
 * (Resources, Connector structures). Volatile structures (Links, Jobs)
 * are not cached.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SqoopClient {

  /**
   * Underlying request object to fetch data from Sqoop server.
   */
  private SqoopResourceRequests resourceRequests;

  /**
   * True if user retrieved all connectors at once.
   */
  private boolean isAllConnectors;
  /**
   * All cached connectors.
   */
  private Map<Long, MConnector> connectors;
  /**
   * All cached config params for every registered connector in the sqoop system.
   */
  private Map<Long, ResourceBundle> connectorConfigBundles;

  /**
   * Cached driver.
   */
  private MDriver mDriver;
  /**
   * Cached driverConfig bundle.
   */
  private ResourceBundle driverConfigBundle;

  /**
   * Status flags used when updating the submission callback status
   */
  //TODO(https://issues.apache.org/jira/browse/SQOOP-1652): Why do wee need a duplicate status enum in client when shell is using the server status?
  // NOTE: the getStatus method is on the job resource and this needs to be revisited
  private enum SubmissionStatus {
    SUBMITTED,
    UPDATED,
    FINISHED
  }

  public SqoopClient(String serverUrl) {
    resourceRequests = new SqoopResourceRequests();
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
    resourceRequests.setServerUrl(serverUrl);
    clearCache();
  }

  /**
   * Set arbitrary request object.
   *
   * @param requests SqoopRequests object
   */
  public void setSqoopRequests(SqoopResourceRequests requests) {
    this.resourceRequests = requests;
    clearCache();
  }

  /**
   * Clear internal cache.
   */
  public void clearCache() {
    connectorConfigBundles = new HashMap<Long, ResourceBundle>();
    driverConfigBundle = null;
    connectors = new HashMap<Long, MConnector>();
    mDriver = null;
    isAllConnectors = false;
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
    if(isAllConnectors) return null;

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
    ConnectorBean request = resourceRequests.readConnector(cid);
    connectors.put(cid, request.getConnectors().get(0));
    connectorConfigBundles.put(cid, request.getResourceBundles().get(cid));
  }

  /**
   * Get list of all connectors.
   *
   * @return
   */
  public Collection<MConnector> getConnectors() {
    if(isAllConnectors) {
      return connectors.values();
    }

    ConnectorBean bean = resourceRequests.readConnector(null);
    isAllConnectors = true;
    for(MConnector connector : bean.getConnectors()) {
      connectors.put(connector.getPersistenceId(), connector);
    }
    connectorConfigBundles = bean.getResourceBundles();

    return connectors.values();
  }

  /**
   * Get resource bundle for given connector.
   *
   * @param connectorId Connector id.
   * @return
   */
  public ResourceBundle getConnectorConfigBundle(long connectorId) {
    if(connectorConfigBundles.containsKey(connectorId)) {
      return connectorConfigBundles.get(connectorId);
    }
    retrieveConnector(connectorId);
    return connectorConfigBundles.get(connectorId);
  }

  /**
   * Return driver config.
   *
   * @return
   */
  public MDriverConfig getDriverConfig() {
    if (mDriver != null) {
      return mDriver.clone(false).getDriverConfig();
    }
    retrieveAndCacheDriver();
    return mDriver.clone(false).getDriverConfig();
  }
  
  /**
   * Return driver.
   *
   * @return
   */
  public MDriver getDriver() {
    if (mDriver != null) {
      return mDriver.clone(false);
    }
    retrieveAndCacheDriver();
    return mDriver.clone(false);
 
  }

  /**
   * Retrieve driverConfig and cache it.
   */
  private void retrieveAndCacheDriver() {
    DriverBean driverBean =  resourceRequests.readDriver();
    mDriver = driverBean.getDriver();
    driverConfigBundle = driverBean.getDriverConfigResourceBundle();
  }

  /**
   * Return driverConfig bundle.
   *xx
   * @return
   */
  public ResourceBundle getDriverConfigBundle() {
    if(driverConfigBundle != null) {
      return driverConfigBundle;
    }
    retrieveAndCacheDriver();
    return driverConfigBundle;
  }

  /**
   * Create new link object for given connector id
   *
   * @param connectorId Connector id
   * @return
   */
  public MLink createLink(long connectorId) {
    return new MLink(connectorId, getConnector(connectorId).getLinkConfig());
  }

  /**
   * Create new link object for given connector name
   *
   * @param connectorName Connector name
   * @return
   */
  public MLink createLink(String connectorName) {
    MConnector connector = getConnector(connectorName);
    if (connector == null) {
      throw new SqoopException(ClientError.CLIENT_0003, connectorName);
    }
    return createLink(connector.getPersistenceId());
  }

  /**
   * Retrieve link for given id.
   *
   * @param linkId Link id
   * @return
   */
  public MLink getLink(long linkId) {
    return resourceRequests.readLink(linkId).getLinks().get(0);
  }

  /**
   * Retrieve list of all links.
   *
   * @return
   */
  public List<MLink> getLinks() {
    return resourceRequests.readLink(null).getLinks();
  }

  /**
   * Create the link and save to the repository
   *
   * @param link link that should be created
   * @return
   */
  public Status saveLink(MLink link) {
    return applyLinkValidations(resourceRequests.saveLink(link), link);
  }

  /**
   * Update link on the server.
   *
   * @param link link that should be updated
   * @return
   */
  public Status updateLink(MLink link) {
    return applyLinkValidations(resourceRequests.updateLink(link), link);
  }

  /**
   * Enable/disable link with given id
   *
   * @param linkId link id
   * @param enabled Enable or disable
   */
  public void enableLink(long linkId, boolean enabled) {
    resourceRequests.enableLink(linkId, enabled);
  }

  /**
   * Delete link with given id.
   *
   * @param linkId link id
   */
  public void deleteLink(long linkId) {
    resourceRequests.deleteLink(linkId);
  }

  /**
   * Create new job the for given links.
   *
   * @param fromLinkId From link id
   * @param toLinkId To link id
   * @return
   */
  public MJob createJob(long fromLinkId, long toLinkId) {
    MLink fromLink = getLink(fromLinkId);
    MLink toLink = getLink(toLinkId);

    return new MJob(
      fromLink.getConnectorId(),
      toLink.getConnectorId(),
      fromLink.getPersistenceId(),
      toLink.getPersistenceId(),
      getConnector(fromLink.getConnectorId()).getFromConfig(),
      getConnector(toLink.getConnectorId()).getToConfig(),
      getDriverConfig()
    );
  }

  /**
   * Retrieve job for given id.
   *
   * @param jobId Job id
   * @return
   */
  public MJob getJob(long jobId) {
    return resourceRequests.readJob(jobId).getJobs().get(0);
  }

  /**
   * Retrieve list of all jobs.
   *
   * @return
   */
  public List<MJob> getJobs() {
    return resourceRequests.readJob(null).getJobs();
  }

  /**
   * Retrieve list of all jobs by connector
   *
   * @return
   */
  public List<MJob> getJobsByConnector(long cId) {
    return resourceRequests.readJobsByConnector(cId).getJobs();
  }

  /**
   * Create job on server and save to the repository
   *
   * @param job Job that should be created
   * @return
   */
  public Status saveJob(MJob job) {
    return applyJobValidations(resourceRequests.saveJob(job), job);
  }

  /**
   * Update job on server.
   * @param job Job that should be updated
   * @return
   */
  public Status updateJob(MJob job) {
    return applyJobValidations(resourceRequests.updateJob(job), job);
  }

  /**
   * Enable/disable job with given id
   *
   * @param jid Job that is going to be enabled/disabled
   * @param enabled Enable or disable
   */
  public void enableJob(long jid, boolean enabled) {
    resourceRequests.enableJob(jid, enabled);
  }

  /**
   * Delete job with given id.
   *
   * @param jobId Job id
   */
  public void deleteJob(long jobId) {
    resourceRequests.deleteJob(jobId);
  }

  /**
   * Start job with given id.
   *
   * @param jobId Job id
   * @return
   */
  public MSubmission startJob(long jobId) {
    return resourceRequests.startJob(jobId).getSubmissions().get(0);
  }

  /**
   * Method used for synchronous job submission.
   * Pass null to callback parameter if submission status is not required and after completion
   * job execution returns MSubmission which contains final status of submission.
   * @param jobId - Job ID
   * @param callback - User may set null if submission status is not required, else callback methods invoked
   * @param pollTime - Server poll time
   * @return MSubmission - Final status of job submission
   * @throws InterruptedException
   */
  public MSubmission startJob(long jobId, SubmissionCallback callback, long pollTime)
      throws InterruptedException {
    if(pollTime <= 0) {
      throw new SqoopException(ClientError.CLIENT_0002);
    }
    //TODO(https://issues.apache.org/jira/browse/SQOOP-1652): address the submit/start/first terminology difference
    // What does first even mean in s distributed client/server model?
    boolean first = true;
    MSubmission submission = resourceRequests.startJob(jobId).getSubmissions().get(0);
    // what happens when the server fails, do we just say finished?
    while(submission.getStatus().isRunning()) {
      if(first) {
        invokeSubmissionCallback(callback, submission, SubmissionStatus.SUBMITTED);
        first = false;
      } else {
        invokeSubmissionCallback(callback, submission, SubmissionStatus.UPDATED);
      }
      Thread.sleep(pollTime);
      submission = getJobStatus(jobId);
    }
    invokeSubmissionCallback(callback, submission, SubmissionStatus.FINISHED);
    return submission;
  }

  /**
   * Invokes the callback's methods with MSubmission object
   * based on SubmissionStatus. If callback is null, no operation performed.
   * @param callback
   * @param submission
   * @param status
   */
  private void invokeSubmissionCallback(SubmissionCallback callback, MSubmission submission,
      SubmissionStatus status) {
    if (callback == null) {
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
    default:
      break;
    }
  }

  /**
   * stop job with given id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission stopJob(long jid) {
    return resourceRequests.stopJob(jid).getSubmissions().get(0);
  }

  /**
   * Get status for given job id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission getJobStatus(long jid) {
    return resourceRequests.getJobStatus(jid).getSubmissions().get(0);
  }

  /**
   * Retrieve list of all submissions.
   *
   * @return
   */
  public List<MSubmission> getSubmissions() {
    return resourceRequests.readSubmission(null).getSubmissions();
  }

  /**
   * Retrieve list of submissions for given jobId.
   *
   * @param jobId Job id
   * @return
   */
  public List<MSubmission> getSubmissionsForJob(long jobId) {
    return resourceRequests.readSubmission(jobId).getSubmissions();
  }

  /**
   * Retrieve list of all roles.
   *
   * @return
   */
  public List<MRole> getRoles() {
    return resourceRequests.readRoles().getRoles();
  }

  /**
   * Create a new role.
   *
   * @param role MRole
   * @return
   */
  public void createRole(MRole role) {
    resourceRequests.createRole(role);
  }

  /**
   * Drop a role.
   *
   * @param role MRole
   * @return
   */
  public void dropRole(MRole role) {
    resourceRequests.dropRole(role);
  }

  /**
   * Grant roles on principals.
   *
   * @param roles      MRole List
   * @param principals MPrincipal List
   * @return
   */
  public void grantRole(List<MRole> roles, List<MPrincipal> principals) {
    resourceRequests.grantRole(roles, principals);
  }

  /**
   * Revoke roles on principals.
   *
   * @param roles      MRole List
   * @param principals MPrincipal List
   * @return
   */
  public void revokeRole(List<MRole> roles, List<MPrincipal> principals) {
    resourceRequests.revokeRole(roles, principals);
  }

  /**
   * Get roles by principal.
   *
   * @param principal MPrincipal
   * @return
   */
  public List<MRole> getRolesByPrincipal(MPrincipal principal) {
    return resourceRequests.readRolesByPrincipal(principal).getRoles();
  }

  /**
   * Get principals by role.
   *
   * @param role MRole
   * @return
   */
  public List<MPrincipal> getPrincipalsByRole(MRole role) {
    return resourceRequests.readPrincipalsByRole(role).getPrincipals();
  }

  /**
   * Grant privileges on principals.
   *
   * @param principals MPrincipal List
   * @param privileges MPrivilege List
   * @return
   */
  public void grantPrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    resourceRequests.grantPrivilege(principals, privileges);
  }

  /**
   * Revoke privileges on principals.
   *
   * @param principals MPrincipal List
   * @param privileges MPrivilege List
   * @return
   */
  public void revokePrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    resourceRequests.revokePrivilege(principals, privileges);
  }

  /**
   * Get privileges by principal.
   *
   * @param principal MPrincipal
   * @param resource MResource
   * @return
   */
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal, MResource resource) {
    return resourceRequests.readPrivilegesByPrincipal(principal, resource).getPrivileges();
  }

  /**
   * Add delegation token into credentials of Hadoop security.
   *
   * @param renewer renewer string
   * @param credentials credentials of Hadoop security, which will be added delegation token
   * @return
   */
  public Token<?>[] addDelegationTokens(String renewer,
                                        Credentials credentials) throws IOException {
    return resourceRequests.addDelegationTokens(renewer, credentials);
  }

  private Status applyLinkValidations(ValidationResultBean bean, MLink link) {
    ConfigValidationResult linkConfig = bean.getValidationResults()[0];
    // Apply validation results
    ConfigUtils.applyValidation(link.getConnectorLinkConfig().getConfigs(), linkConfig);
    Long id = bean.getId();
    if (id != null) {
      link.setPersistenceId(id);
    }
    return Status.getWorstStatus(linkConfig.getStatus());
  }


  private Status applyJobValidations(ValidationResultBean bean, MJob job) {
    ConfigValidationResult fromConfig = bean.getValidationResults()[0];
    ConfigValidationResult toConfig = bean.getValidationResults()[1];
    ConfigValidationResult driver = bean.getValidationResults()[2];

    ConfigUtils.applyValidation(
        job.getFromJobConfig().getConfigs(),
        fromConfig);
    ConfigUtils.applyValidation(
        job.getToJobConfig().getConfigs(),
        toConfig);
    ConfigUtils.applyValidation(
      job.getDriverConfig().getConfigs(),
      driver
    );

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(fromConfig.getStatus(), toConfig.getStatus(), driver.getStatus());
  }
}
