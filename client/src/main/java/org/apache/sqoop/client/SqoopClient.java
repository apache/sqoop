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
import org.apache.sqoop.json.ValidationBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;

import java.util.List;
import java.util.ResourceBundle;

/**
 * Sqoop client API.
 *
 * High level Sqoop client API to communicate with Sqoop server.
 */
public class SqoopClient {

  private SqoopRequests requests;

  public SqoopClient(String serverUrl) {
    requests = new SqoopRequests();
    requests.setServerUrl(serverUrl);
  }

  /**
   * Set new server URL.
   *
   * @param serverUrl Server URL
   */
  public void setServerUrl(String serverUrl) {
    requests.setServerUrl(serverUrl);
  }

  /**
   * Get connector with given id.
   *
   * @param cid Connector id.
   * @return
   */
  public MConnector getConnector(long cid) {
    return requests.readConnector(cid).getConnectors().get(0);
  }

  /**
   * Get list of all connectors.
   *
   * @return
   */
  public List<MConnector> getConnectors() {
    return requests.readConnector(null).getConnectors();
  }

  /**
   * Get resouce bundle for given connector.
   *
   * @param cid Connector id.
   * @return
   */
  public ResourceBundle getResourceBundle(long cid) {
    return requests.readConnector(cid).getResourceBundles().get(cid);
  }

  /**
   * Return framework metadata.
   *
   * @return
   */
  public MFramework getFramework() {
    return requests.readFramework().getFramework();
  }

  /**
   * Return framework bundle.
   *
   * @return
   */
  public ResourceBundle getFrameworkResourceBundle() {
    return requests.readFramework().getResourceBundle();
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
   * Delete connection with given id.
   *
   * @param xid Connection id
   */
  public void deleteConnection(long xid) {
    requests.deleteConnection(xid);
  }

  /**
   * Create new job of given type and for given connection.
   *
   * @param xid Connection id
   * @param type Job type
   * @return
   */
  public MJob newJob(long xid, MJob.Type type) {
    MConnection connection = getConnection(xid);

    return new MJob(
      connection.getConnectorId(),
      connection.getPersistenceId(),
      type,
      getConnector(connection.getConnectorId()).getJobForms(type),
      getFramework().getJobForms(type)
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
    return requests.createSubmission(jid).getSubmission();
  }

  /**
   * Stop job with given id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission stopSubmission(long jid) {
    return requests.deleteSubmission(jid).getSubmission();
  }

  /**
   * Get status for given job id.
   *
   * @param jid Job id
   * @return
   */
  public MSubmission getSubmissionStatus(long jid) {
    return requests.readSubmission(jid).getSubmission();
  }

  private Status applyValidations(ValidationBean bean, MConnection connection) {
    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(connection.getConnectorPart().getForms(), connector);
    FormUtils.applyValidation(connection.getFrameworkPart().getForms(), framework);

    Long id = bean.getId();
    if(id != null) {
      connection.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }

  private Status applyValidations(ValidationBean bean, MJob job) {
    Validation connector = bean.getConnectorValidation();
    Validation framework = bean.getFrameworkValidation();

    FormUtils.applyValidation(job.getConnectorPart().getForms(), connector);
    FormUtils.applyValidation(job.getFrameworkPart().getForms(), framework);

    Long id = bean.getId();
    if(id != null) {
      job.setPersistenceId(id);
    }

    return Status.getWorstStatus(connector.getStatus(), framework.getStatus());
  }
}
