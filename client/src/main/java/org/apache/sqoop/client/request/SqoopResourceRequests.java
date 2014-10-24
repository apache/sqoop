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

import org.apache.sqoop.json.LinkBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MJob;

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

  public void setServerUrl(String serverUrl) {
    this.serverUrl = serverUrl;
  }

  public DriverResourceRequest getDriverResourceRequest() {
    if (driverRequest == null) {
      driverRequest = new DriverResourceRequest();
    }

    return driverRequest;
  }

  public ConnectorResourceRequest getConnectorResourceRequest() {
    if (connectorRequest == null) {
      connectorRequest = new ConnectorResourceRequest();
    }

    return connectorRequest;
  }

  public LinkResourceRequest getLinkResourceRequest() {
    if (linkRequest == null) {
      linkRequest = new LinkResourceRequest();
    }

    return linkRequest;
  }

  public JobResourceRequest getJobResourceRequest() {
    if (jobRequest == null) {
      jobRequest = new JobResourceRequest();
    }

    return jobRequest;
  }

  public SubmissionResourceRequest getSubmissionResourceRequest() {
    if (submissionRequest == null) {
      submissionRequest = new SubmissionResourceRequest();
    }

    return submissionRequest;
  }

  public DriverBean readDriver() {
    return getDriverResourceRequest().read(serverUrl);
  }

  public ConnectorBean readConnector(Long cid) {
    return getConnectorResourceRequest().read(serverUrl, cid);
  }

  public ValidationResultBean saveLink(MLink link) {
    return getLinkResourceRequest().create(serverUrl, link);
  }

  public LinkBean readLink(Long linkId) {
    return getLinkResourceRequest().read(serverUrl, linkId);
  }

  public ValidationResultBean updateLink(MLink link) {
    return getLinkResourceRequest().update(serverUrl, link);
  }

  public void enableLink(Long lid, Boolean enabled) {
    getLinkResourceRequest().enable(serverUrl, lid, enabled);
  }

  public void deleteLink(Long lid) {
    getLinkResourceRequest().delete(serverUrl, lid);
  }

  public ValidationResultBean saveJob(MJob job) {
    return getJobResourceRequest().create(serverUrl, job);
  }

  public JobBean readJob(Long jobId) {
    return getJobResourceRequest().read(serverUrl, jobId);
  }

  public ValidationResultBean updateJob(MJob job) {
    return getJobResourceRequest().update(serverUrl, job);
  }

  public void enableJob(Long jid, Boolean enabled) {
    getJobResourceRequest().enable(serverUrl, jid, enabled);
  }

  public void deleteJob(Long jid) {
    getJobResourceRequest().delete(serverUrl, jid);
  }

  public SubmissionBean readHistory(Long jid) {
    return getSubmissionResourceRequest().readHistory(serverUrl, jid);
  }

  public SubmissionBean readSubmission(Long jid) {
    return getSubmissionResourceRequest().read(serverUrl, jid);
  }

  public SubmissionBean createSubmission(Long jid) {
    return getSubmissionResourceRequest().create(serverUrl, jid);
  }

  public SubmissionBean deleteSubmission(Long jid) {
    return getSubmissionResourceRequest().delete(serverUrl, jid);
  }
}
