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
package org.apache.sqoop.client.core;

import org.apache.sqoop.client.request.ConnectionRequest;
import org.apache.sqoop.client.request.ConnectorRequest;
import org.apache.sqoop.client.request.FrameworkRequest;
import org.apache.sqoop.client.request.JobRequest;
import org.apache.sqoop.client.request.SubmissionRequest;
import org.apache.sqoop.json.ConnectionBean;
import org.apache.sqoop.json.ConnectorBean;
import org.apache.sqoop.json.FrameworkBean;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.json.ValidationBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;

/**
 *  Client wise caching of various request objects
 */
public final class RequestCache {

  private static FrameworkRequest frameworkRequest;
  private static ConnectorRequest connectorRequest;
  private static ConnectionRequest connectionRequest;
  private static JobRequest jobRequest;
  private static SubmissionRequest submissionRequest;

  public static FrameworkRequest getFrameworkRequest() {
    if (frameworkRequest == null) {
      frameworkRequest = new FrameworkRequest();
    }

    return frameworkRequest;
  }

  public static ConnectorRequest getConnectorRequest() {
    if (connectorRequest == null) {
      connectorRequest = new ConnectorRequest();
    }

    return connectorRequest;
  }

  public static ConnectionRequest getConnectionRequest() {
    if (connectionRequest == null) {
      connectionRequest = new ConnectionRequest();
    }

    return connectionRequest;
  }

  public static JobRequest getJobRequest() {
    if (jobRequest == null) {
      jobRequest = new JobRequest();
    }

    return jobRequest;
  }

  public static SubmissionRequest getSubmissionRequest() {
    if (submissionRequest == null) {
      submissionRequest = new SubmissionRequest();
    }

    return submissionRequest;
  }

  public static FrameworkBean readFramework() {
    return getFrameworkRequest().read(Environment.getServerUrl());
  }

  public static ConnectorBean readConnector(String cid) {
    return getConnectorRequest().read(Environment.getServerUrl(), cid);
  }

  public static ValidationBean createConnection(MConnection connection) {
    return getConnectionRequest()
      .create(Environment.getServerUrl(), connection);
  }

  public static Status createConnectionApplyValidations(MConnection connection) {
    ValidationBean bean = createConnection(connection);

    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(connection.getConnectorPart().getForms(),
      connector);
    FormUtils.applyValidation(connection.getFrameworkPart().getForms(),
      connector);

    Long id = bean.getId();
    if(id != null) {
      connection.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  public static ConnectionBean readConnection(String connectionId) {
    return getConnectionRequest()
      .read(Environment.getServerUrl(), connectionId);
  }

  public static ValidationBean updateConnection(MConnection connection) {
    return getConnectionRequest()
      .update(Environment.getServerUrl(), connection);
  }

  public static Status updateConnectionApplyValidations(MConnection connection){
    ValidationBean bean = updateConnection(connection);

    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(connection.getConnectorPart().getForms(),
      connector);
    FormUtils.applyValidation(connection.getFrameworkPart().getForms(),
      connector);

    Long id = bean.getId();
    if(id != null) {
      connection.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  public static void deleteConnection(String xid) {
    getConnectionRequest().delete(Environment.getServerUrl(), xid);
  }

  public static ValidationBean createJob(MJob job) {
    return getJobRequest().create(Environment.getServerUrl(), job);
  }

  public static Status createJobApplyValidations(MJob job) {
    ValidationBean bean = createJob(job);

    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(job.getConnectorPart().getForms(),
      connector);
    FormUtils.applyValidation(job.getFrameworkPart().getForms(),
      connector);

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  public static JobBean readJob(String jobId) {
    return getJobRequest().read(Environment.getServerUrl(), jobId);
  }

  public static ValidationBean updateJob(MJob job) {
    return getJobRequest().update(Environment.getServerUrl(), job);
  }

  public static Status updateJobApplyValidations(MJob job) {
    ValidationBean bean = updateJob(job);

    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(job.getConnectorPart().getForms(),
      connector);
    FormUtils.applyValidation(job.getFrameworkPart().getForms(),
      connector);

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  public static void deleteJob(String jid) {
    getJobRequest().delete(Environment.getServerUrl(), jid);
  }

  public static MSubmission readSubmission(String jid) {
    return getSubmissionRequest()
      .read(Environment.getServerUrl(), jid)
      .getSubmission();
  }

  public static MSubmission createSubmission(String jid) {
    return getSubmissionRequest()
      .create(Environment.getServerUrl(), jid)
      .getSubmission();
  }

  public static MSubmission deleteSubmission(String jid) {
    return getSubmissionRequest()
      .delete(Environment.getServerUrl(), jid)
      .getSubmission();
  }

  private RequestCache() {
    // Instantiation is prohibited
  }
}
