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
package org.apache.sqoop.client.request;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.Credentials;
import org.apache.sqoop.json.*;
import org.apache.sqoop.model.*;

import java.io.IOException;
import java.util.List;

/**
 * Unified class for all request objects.
 */
public class SqoopResourceRequests {

  private String serverUrl;

  private DriverResourceRequest driverRequest;
  private ConnectorResourceRequest connectorRequest;
  private LinkResourceRequest linkRequest;
  private JobResourceRequest jobRequest;
  private SubmissionResourceRequest submissionRequest;
  private AuthorizationResourceRequest authorizationRequest;
  private VersionResourceRequest versionRequest;
  private DelegationTokenAuthenticatedURL.Token authToken;

  public SqoopResourceRequests() {
    authToken = new DelegationTokenAuthenticatedURL.Token();
  }

  public void setServerUrl(String serverUrl) {
    this.serverUrl = serverUrl;
  }

  public String getServerUrl() {
    return serverUrl;
  }

  public DriverResourceRequest getDriverResourceRequest() {
    if (driverRequest == null) {
      driverRequest = new DriverResourceRequest(authToken);
    }

    return driverRequest;
  }

  public ConnectorResourceRequest getConnectorResourceRequest() {
    if (connectorRequest == null) {
      connectorRequest = new ConnectorResourceRequest(authToken);
    }

    return connectorRequest;
  }

  public LinkResourceRequest getLinkResourceRequest() {
    if (linkRequest == null) {
      linkRequest = new LinkResourceRequest(authToken);
    }

    return linkRequest;
  }

  public JobResourceRequest getJobResourceRequest() {
    if (jobRequest == null) {
      jobRequest = new JobResourceRequest(authToken);
    }

    return jobRequest;
  }

  public SubmissionResourceRequest getSubmissionResourceRequest() {
    if (submissionRequest == null) {
      submissionRequest = new SubmissionResourceRequest(authToken);
    }

    return submissionRequest;
  }

  public AuthorizationResourceRequest getAuthorizationRequest() {
    if (authorizationRequest == null) {
      authorizationRequest = new AuthorizationResourceRequest(authToken);
    }

    return authorizationRequest;
  }

  public VersionResourceRequest getVersionRequest() {
    if (versionRequest == null) {
      versionRequest = new VersionResourceRequest(authToken);
    }

    return versionRequest;
  }

  public DriverBean readDriver() {
    return getDriverResourceRequest().read(serverUrl);
  }

  public ConnectorBean readConnector(String connectorName) {
    return getConnectorResourceRequest().read(serverUrl, connectorName);
  }

  public ValidationResultBean saveLink(MLink link) {
    return getLinkResourceRequest().create(serverUrl, link);
  }

  public LinkBean readLink(String linkArg) {
    return getLinkResourceRequest().read(serverUrl, linkArg);
  }

  public ValidationResultBean updateLink(MLink link, String oldLinkName) {
    return getLinkResourceRequest().update(serverUrl, link, oldLinkName);
  }

  public void enableLink(String lArg, Boolean enabled) {
    getLinkResourceRequest().enable(serverUrl, lArg, enabled);
  }

  public void deleteLink(String lArg) {
    getLinkResourceRequest().delete(serverUrl, lArg);
  }

  public ValidationResultBean saveJob(MJob job) {
    return getJobResourceRequest().create(serverUrl, job);
  }

  public JobBean readJob(String jobArg) {
    return getJobResourceRequest().read(serverUrl, jobArg);
  }

  public JobBean readJobsByConnector(String cArg) {
    return getJobResourceRequest().readByConnector(serverUrl, cArg);
  }

  public ValidationResultBean updateJob(MJob job, String oldJobName) {
    return getJobResourceRequest().update(serverUrl, job, oldJobName);
  }

  public void enableJob(String jArg, Boolean enabled) {
    getJobResourceRequest().enable(serverUrl, jArg, enabled);
  }

  public void deleteJob(String jArg) {
    getJobResourceRequest().delete(serverUrl, jArg);
  }

  public SubmissionBean getJobStatus(String jArg) {
    return getJobResourceRequest().status(serverUrl, jArg);
  }

  public SubmissionBean startJob(String jArg) {
    return getJobResourceRequest().start(serverUrl, jArg);
  }

  public SubmissionBean stopJob(String jArg) {
    return getJobResourceRequest().stop(serverUrl, jArg);
  }

  public SubmissionsBean readSubmission(String jArg) {
    return getSubmissionResourceRequest().read(serverUrl, jArg);
  }

  public RoleBean readRoles() {
    return getAuthorizationRequest().readRoles(serverUrl);
  }

  public void createRole(MRole role) {
    getAuthorizationRequest().createRole(serverUrl, role);
  }

  public void dropRole(MRole role) {
    getAuthorizationRequest().dropRole(serverUrl, role);
  }

  public void grantRole(List<MRole> roles, List<MPrincipal> principals) {
    getAuthorizationRequest().grantRevokeRole(serverUrl, roles, principals, true);
  }

  public void revokeRole(List<MRole> roles, List<MPrincipal> principals) {
    getAuthorizationRequest().grantRevokeRole(serverUrl, roles, principals, false);
  }

  public RoleBean readRolesByPrincipal(MPrincipal principal) {
    return getAuthorizationRequest().readRolesByPrincipal(serverUrl, principal);
  }

  public PrincipalBean readPrincipalsByRole(MRole role) {
    return getAuthorizationRequest().readPrincipalsByRole(serverUrl, role);
  }

  public void grantPrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    getAuthorizationRequest().grantRevokePrivilege(serverUrl, principals, privileges, true);
  }

  public PrivilegesBean readPrivilegesByPrincipal(MPrincipal principal, MResource resource) {
    return getAuthorizationRequest().readPrivilegesByPrincipal(serverUrl, principal, resource);
  }

  public void revokePrivilege(List<MPrincipal> principals, List<MPrivilege> privileges) {
    getAuthorizationRequest().grantRevokePrivilege(serverUrl, principals, privileges, false);
  }

  public VersionBean readVersion() {
    return getVersionRequest().read(serverUrl);
  }

  public Token<?>[] addDelegationTokens(String renewer,
                                        Credentials credentials) throws IOException {
    return getDriverResourceRequest().addDelegationTokens(serverUrl + DriverResourceRequest.RESOURCE, renewer, credentials);
  }
}
