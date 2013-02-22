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
package org.apache.sqoop.handler;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.ValidationBean;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.utils.ClassUtils;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Job request handler is supporting following resources:
 *
 * GET /v1/job/:jid
 * Return details about one particular job with id :jid or about all of
 * them if :jid equals to "all".
 *
 * POST /v1/job
 * Create new job
 *
 * PUT /v1/job/:jid
 * Update job with id :jid.
 *
 * DELETE /v1/job/:jid
 * Remove job with id :jid
 *
 * Planned resources:
 *
 * GET /v1/job
 * Get brief list of all jobs present in the system. This resource is not yet
 * implemented.
 */
public class JobRequestHandler implements RequestHandler {

  private static final Logger LOG =
      Logger.getLogger(ConnectorRequestHandler.class);

  public JobRequestHandler() {
    LOG.info("JobRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    switch (ctx.getMethod()) {
      case GET:
        return getJobs(ctx);
      case POST:
        return createUpdateJob(ctx, false);
      case PUT:
        return createUpdateJob(ctx, true);
      case DELETE:
        return deleteJob(ctx);
    }

    return null;
  }

  /**
   * Delete job from metadata repository.
   *
   * @param ctx Context object
   * @return Empty bean
   */
  private JsonBean deleteJob(RequestContext ctx) {
    String sxid = ctx.getLastURLElement();
    long jid = Long.valueOf(sxid);

    Repository repository = RepositoryManager.getInstance().getRepository();
    repository.deleteJob(jid);

    return JsonBean.EMPTY_BEAN;
  }

  /**
   * Update or create job metadata in repository.
   *
   * @param ctx Context object
   * @return Validation bean object
   */
  private JsonBean createUpdateJob(RequestContext ctx, boolean update) {
//    Check that given ID equals with sent ID, otherwise report an error UPDATE
//    String sxid = ctx.getLastURLElement();
//    long xid = Long.valueOf(sxid);

    JobBean bean = new JobBean();

    try {
      JSONObject json =
        (JSONObject) JSONValue.parse(ctx.getRequest().getReader());
      bean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Can't read request content", e);
    }

    // Get job object
    List<MJob> jobs = bean.getJobs();

    if(jobs.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Expected one job metadata but got " + jobs.size());
    }

    // Job object
    MJob job = jobs.get(0);

    // Verify that user is not trying to spoof us
    MJobForms connectorForms
      = ConnectorManager.getInstance().getConnectorMetadata(job.getConnectorId())
      .getJobForms(job.getType());
    MJobForms frameworkForms = FrameworkManager.getInstance().getFramework()
      .getJobForms(job.getType());

    if(!connectorForms.equals(job.getConnectorPart())
      || !frameworkForms.equals(job.getFrameworkPart())) {
      throw new SqoopException(ServerError.SERVER_0003,
        "Detected incorrect form structure");
    }

    // Responsible connector for this session
    SqoopConnector connector =
      ConnectorManager.getInstance().getConnector(job.getConnectorId());

    // Get validator objects
    Validator connectorValidator = connector.getValidator();
    Validator frameworkValidator = FrameworkManager.getInstance().getValidator();

    // We need translate forms to configuration objects
    Object connectorConfig = ClassUtils.instantiate(
      connector.getJobConfigurationClass(job.getType()));
    Object frameworkConfig = ClassUtils.instantiate(
      FrameworkManager.getInstance().getJobConfigurationClass(job.getType()));

    FormUtils.fromForms(job.getConnectorPart().getForms(), connectorConfig);
    FormUtils.fromForms(job.getFrameworkPart().getForms(), frameworkConfig);

    // Validate both parts
    Validation connectorValidation =
      connectorValidator.validateJob(job.getType(), connectorConfig);
    Validation frameworkValidation =
      frameworkValidator.validateJob(job.getType(), frameworkConfig);

    Status finalStatus = Status.getWorstStatus(connectorValidation.getStatus(),
      frameworkValidation.getStatus());

    // Return back validations in all cases
    ValidationBean outputBean =
      new ValidationBean(connectorValidation, frameworkValidation);

    // If we're good enough let's perform the action
    if(finalStatus.canProceed()) {
      if(update) {
        RepositoryManager.getInstance().getRepository().updateJob(job);
      } else {
        RepositoryManager.getInstance().getRepository().createJob(job);
        outputBean.setId(job.getPersistenceId());
      }

    }

    return outputBean;
  }

  private JsonBean getJobs(RequestContext ctx) {
    String sjid = ctx.getLastURLElement();
    JobBean bean;

    Locale locale = ctx.getAcceptLanguageHeader();
    Repository repository = RepositoryManager.getInstance().getRepository();

    if (sjid.equals("all")) {

      List<MJob> jobs = repository.findJobs();
      bean = new JobBean(jobs);

      // Add associated resources into the bean
      for( MJob job : jobs) {
        long connectorId = job.getConnectorId();
        if(!bean.hasConnectorBundle(connectorId)) {
          bean.addConnectorBundle(connectorId,
            ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
        }
      }
    } else {
      long jid = Long.valueOf(sjid);

      MJob job = repository.findJob(jid);
      long connectorId = job.getConnectorId();

      bean = new JobBean(job);

      bean.addConnectorBundle(connectorId,
        ConnectorManager.getInstance().getResourceBundle(connectorId, locale));
    }

    // Sent framework resource bundle in all cases
    bean.setFrameworkBundle(FrameworkManager.getInstance().getBundle(locale));

    return bean;
  }
}
