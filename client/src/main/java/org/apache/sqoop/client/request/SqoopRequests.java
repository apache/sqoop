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

import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MJob;

/**
 * Unified class for all request objects.
 */
public class SqoopRequests {

  private String serverUrl;

  private FrameworkRequest frameworkRequest;
  private ConnectorRequest connectorRequest;
  private ConnectionRequest connectionRequest;
  private JobRequest jobRequest;
  private SubmissionRequest submissionRequest;

  public void setServerUrl(String serverUrl) {
    this.serverUrl = serverUrl;
  }

  public FrameworkRequest getFrameworkRequest() {
    if (frameworkRequest == null) {
      frameworkRequest = new FrameworkRequest();
    }

    return frameworkRequest;
  }

  public ConnectorRequest getConnectorRequest() {
    if (connectorRequest == null) {
      connectorRequest = new ConnectorRequest();
    }

    return connectorRequest;
  }

  public ConnectionRequest getConnectionRequest() {
    if (connectionRequest == null) {
      connectionRequest = new ConnectionRequest();
    }

    return connectionRequest;
  }

  public JobRequest getJobRequest() {
    if (jobRequest == null) {
      jobRequest = new JobRequest();
    }

    return jobRequest;
  }

  public SubmissionRequest getSubmissionRequest() {
    if (submissionRequest == null) {
      submissionRequest = new SubmissionRequest();
    }

    return submissionRequest;
  }

  public FrameworkBean readFramework() {
    return getFrameworkRequest().read(serverUrl);
  }

  public ConnectorBean readConnector(Long cid) {
    return getConnectorRequest().read(serverUrl, cid);
  }

  public ValidationResultBean createConnection(MConnection connection) {
    return getConnectionRequest().create(serverUrl, connection);
  }

  public ConnectionBean readConnection(Long connectionId) {
    return getConnectionRequest().read(serverUrl, connectionId);
  }

  public ValidationResultBean updateConnection(MConnection connection) {
    return getConnectionRequest().update(serverUrl, connection);
  }

  public void enableConnection(Long xid, Boolean enabled) {
    getConnectionRequest().enable(serverUrl, xid, enabled);
  }

  public void deleteConnection(Long xid) {
    getConnectionRequest().delete(serverUrl, xid);
  }

  public ValidationResultBean createJob(MJob job) {
    return getJobRequest().create(serverUrl, job);
  }

  public JobBean readJob(Long jobId) {
    return getJobRequest().read(serverUrl, jobId);
  }

  public ValidationResultBean updateJob(MJob job) {
    return getJobRequest().update(serverUrl, job);
  }

  public void enableJob(Long jid, Boolean enabled) {
    getJobRequest().enable(serverUrl, jid, enabled);
  }

  public void deleteJob(Long jid) {
    getJobRequest().delete(serverUrl, jid);
  }

  public SubmissionBean readHistory(Long jid) {
    return getSubmissionRequest().readHistory(serverUrl, jid);
  }

  public SubmissionBean readSubmission(Long jid) {
    return getSubmissionRequest().read(serverUrl, jid);
  }

  public SubmissionBean createSubmission(Long jid) {
    return getSubmissionRequest().create(serverUrl, jid);
  }

  public SubmissionBean deleteSubmission(Long jid) {
    return getSubmissionRequest().delete(serverUrl, jid);
  }
}
