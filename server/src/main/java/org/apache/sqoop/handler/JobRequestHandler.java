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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.apache.sqoop.audit.AuditLoggerManager;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.JobManager;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.json.JobBean;
import org.apache.sqoop.json.JobsBean;
import org.apache.sqoop.json.JsonBean;
import org.apache.sqoop.json.SubmissionBean;
import org.apache.sqoop.json.ValidationResultBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.request.HttpEventContext;
import org.apache.sqoop.security.authorization.AuthorizationEngine;
import org.apache.sqoop.security.AuthorizationManager;
import org.apache.sqoop.server.RequestContext;
import org.apache.sqoop.server.RequestHandler;
import org.apache.sqoop.server.common.ServerError;
import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.Status;
import org.json.simple.JSONObject;

public class JobRequestHandler implements RequestHandler {

  /** enum for representing the actions supported on the job resource*/
  enum JobAction {
    ENABLE("enable"),
    DISABLE("disable"),
    START("start"),
    STOP("stop"),
    ;
    JobAction(String name) {
      this.name = name;
    }

    String name;

    public static JobAction fromString(String name) {
      if (name != null) {
        for (JobAction action : JobAction.values()) {
          if (name.equalsIgnoreCase(action.name)) {
            return action;
          }
        }
      }
      return null;
    }
  }

  private static final Logger LOG = Logger.getLogger(JobRequestHandler.class);

  static final String JOBS_PATH = "jobs";
  static final String JOB_PATH = "job";
  static final String STATUS = "status";

  public JobRequestHandler() {
    LOG.info("JobRequestHandler initialized");
  }

  @Override
  public JsonBean handleEvent(RequestContext ctx) {
    LOG.info("Got job request");
    switch (ctx.getMethod()) {
    case GET:
      if (STATUS.equals(ctx.getLastURLElement())) {
        return getJobStatus(ctx);
      }
      return getJobs(ctx);
    case POST:
      return createUpdateJob(ctx, true);
    case PUT:
      JobAction action = JobAction.fromString(ctx.getLastURLElement());
      if (action != null) {
        switch (action) {
          case ENABLE:
            return enableJob(ctx, true);
          case DISABLE:
            return enableJob(ctx, false);
          case START:
            return startJob(ctx);
          case STOP:
            return stopJob(ctx);
        }
      }
      return createUpdateJob(ctx, false);
    case DELETE:
      return deleteJob(ctx);
    }

    return null;
  }

  /**
   * Delete job from repository.
   *
   * @param ctx
   *          Context object
   * @return Empty bean
   */
  private JsonBean deleteJob(RequestContext ctx) {

    Repository repository = RepositoryManager.getInstance().getRepository();

    String jobIdentifier = ctx.getLastURLElement();
    long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);

    // Authorization check
    AuthorizationEngine.deleteJob(String.valueOf(jobId));

    AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
        ctx.getRequest().getRemoteAddr(), "delete", "job", jobIdentifier);
    repository.deleteJob(jobId);
    MResource resource = new MResource(String.valueOf(jobId), MResource.TYPE.JOB);
    AuthorizationManager.getAuthorizationHandler().removeResource(resource);
    return JsonBean.EMPTY_BEAN;
  }

  /**
   * Update or create job in repository.
   *
   * @param ctx
   *          Context object
   * @return Validation bean object
   */
  private JsonBean createUpdateJob(RequestContext ctx, boolean create) {

    Repository repository = RepositoryManager.getInstance().getRepository();

    JobBean bean = new JobBean();

    try {
      JSONObject json = JSONUtils.parse(ctx.getRequest().getReader());
      bean.restore(json);
    } catch (IOException e) {
      throw new SqoopException(ServerError.SERVER_0003, "Can't read request content", e);
    }

    String username = ctx.getUserName();

    // Get job object
    List<MJob> jobs = bean.getJobs();

    if (jobs.size() != 1) {
      throw new SqoopException(ServerError.SERVER_0003, "Expected one job but got " + jobs.size());
    }

    // Job object
    MJob postedJob = jobs.get(0);

    // Authorization check
    if (create) {
      AuthorizationEngine.createJob(String.valueOf(postedJob.getFromLinkId()),
              String.valueOf(postedJob.getToLinkId()));
    } else {
      AuthorizationEngine.updateJob(String.valueOf(postedJob.getFromLinkId()),
              String.valueOf(postedJob.getToLinkId()),
              String.valueOf(postedJob.getPersistenceId()));
    }

    // Verify that user is not trying to spoof us
    MFromConfig fromConfig = ConnectorManager.getInstance()
        .getConnectorConfigurable(postedJob.getFromConnectorId()).getFromConfig();
    MToConfig toConfig = ConnectorManager.getInstance()
        .getConnectorConfigurable(postedJob.getToConnectorId()).getToConfig();
    MDriverConfig driverConfig = Driver.getInstance().getDriver().getDriverConfig();

    if (!fromConfig.equals(postedJob.getFromJobConfig())
        || !driverConfig.equals(postedJob.getDriverConfig())
        || !toConfig.equals(postedJob.getToJobConfig())) {
      throw new SqoopException(ServerError.SERVER_0003, "Detected incorrect config structure");
    }

    // if update get the job id from the request URI
    if (!create) {
      String jobIdentifier = ctx.getLastURLElement();
      // support jobName or jobId for the api
      long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);
      if (postedJob.getPersistenceId() == MPersistableEntity.PERSISTANCE_ID_DEFAULT) {
        MJob existingJob = repository.findJob(jobId);
        postedJob.setPersistenceId(existingJob.getPersistenceId());
      }
    }

    // Corresponding connectors for this
    SqoopConnector fromConnector = ConnectorManager.getInstance().getSqoopConnector(
        postedJob.getFromConnectorId());
    SqoopConnector toConnector = ConnectorManager.getInstance().getSqoopConnector(
        postedJob.getToConnectorId());

    if (!fromConnector.getSupportedDirections().contains(Direction.FROM)) {
      throw new SqoopException(ServerError.SERVER_0004, "Connector "
          + fromConnector.getClass().getCanonicalName() + " does not support FROM direction.");
    }

    if (!toConnector.getSupportedDirections().contains(Direction.TO)) {
      throw new SqoopException(ServerError.SERVER_0004, "Connector "
          + toConnector.getClass().getCanonicalName() + " does not support TO direction.");
    }

    // Validate user supplied data
    ConfigValidationResult fromConfigValidator = ConfigUtils.validateConfigs(
        postedJob.getFromJobConfig().getConfigs(),
        fromConnector.getJobConfigurationClass(Direction.FROM));
    ConfigValidationResult toConfigValidator = ConfigUtils.validateConfigs(
        postedJob.getToJobConfig().getConfigs(),
        toConnector.getJobConfigurationClass(Direction.TO));
    ConfigValidationResult driverConfigValidator = ConfigUtils.validateConfigs(postedJob
        .getDriverConfig().getConfigs(), Driver.getInstance().getDriverJobConfigurationClass());
    Status finalStatus = Status.getWorstStatus(fromConfigValidator.getStatus(),
        toConfigValidator.getStatus(), driverConfigValidator.getStatus());
    // Return back validations in all cases
    ValidationResultBean validationResultBean = new ValidationResultBean(fromConfigValidator, toConfigValidator, driverConfigValidator);

    // If we're good enough let's perform the action
    if (finalStatus.canProceed()) {
      if (create) {
        AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "create", "job",
            String.valueOf(postedJob.getPersistenceId()));

        postedJob.setCreationUser(username);
        postedJob.setLastUpdateUser(username);
        repository.createJob(postedJob);
        validationResultBean.setId(postedJob.getPersistenceId());
      } else {
        AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
            ctx.getRequest().getRemoteAddr(), "update", "job",
            String.valueOf(postedJob.getPersistenceId()));

        postedJob.setLastUpdateUser(username);
        repository.updateJob(postedJob);
      }
    }
    return validationResultBean;
  }

  private JsonBean getJobs(RequestContext ctx) {
    String connectorIdentifier = ctx.getLastURLElement();
    JobBean jobBean;
    Locale locale = ctx.getAcceptLanguageHeader();
    Repository repository = RepositoryManager.getInstance().getRepository();
    // jobs by connector
    if (ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM) != null) {
      connectorIdentifier = ctx.getParameterValue(CONNECTOR_NAME_QUERY_PARAM);
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "jobsByConnector", connectorIdentifier);
      long connectorId = HandlerUtils.getConnectorIdFromIdentifier(connectorIdentifier);
      List<MJob> jobList = repository.findJobsForConnector(connectorId);

      // Authorization check
      jobList = AuthorizationEngine.filterResource(MResource.TYPE.JOB, jobList);

      jobBean = createJobsBean(jobList, locale);
    } else
    // all jobs in the system
    if (ctx.getPath().contains(JOBS_PATH)
        || (ctx.getPath().contains(JOB_PATH) && connectorIdentifier.equals("all"))) {
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "jobs", "all");
      List<MJob> jobList = repository.findJobs();

      // Authorization check
      jobList = AuthorizationEngine.filterResource(MResource.TYPE.JOB, jobList);

      jobBean = createJobsBean(jobList, locale);
    }
    // job by Id
    else {
      AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
          ctx.getRequest().getRemoteAddr(), "get", "job", connectorIdentifier);

      long jobId = HandlerUtils.getJobIdFromIdentifier(connectorIdentifier, repository);
      MJob job = repository.findJob(jobId);

      // Authorization check
      AuthorizationEngine.readJob(String.valueOf(job.getPersistenceId()));

      jobBean = createJobBean(Arrays.asList(job), locale);
    }
    return jobBean;
  }

  private JobBean createJobBean(List<MJob> jobs, Locale locale) {
    JobBean jobBean = new JobBean(jobs);
    addJob(jobs, locale, jobBean);
    return jobBean;
  }

  private JobsBean createJobsBean(List<MJob> jobs, Locale locale) {
    JobsBean jobsBean = new JobsBean(jobs);
    addJob(jobs, locale, jobsBean);
    return jobsBean;
  }

  private void addJob(List<MJob> jobs, Locale locale, JobBean bean) {
    // Add associated resources into the bean
    for (MJob job : jobs) {
      long fromConnectorId = job.getFromConnectorId();
      long toConnectorId = job.getToConnectorId();
      // replace it only if it does not already exist
      if (!bean.hasConnectorConfigBundle(fromConnectorId)) {
        bean.addConnectorConfigBundle(fromConnectorId, ConnectorManager.getInstance()
            .getResourceBundle(fromConnectorId, locale));
      }
      if (!bean.hasConnectorConfigBundle(toConnectorId)) {
        bean.addConnectorConfigBundle(toConnectorId, ConnectorManager.getInstance()
            .getResourceBundle(toConnectorId, locale));
      }
    }
  }

  private JsonBean enableJob(RequestContext ctx, boolean enabled) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String[] elements = ctx.getUrlElements();
    String jobIdentifier = elements[elements.length - 2];
    long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);

    // Authorization check
    AuthorizationEngine.enableDisableJob(String.valueOf(jobId));

    repository.enableJob(jobId, enabled);
    return JsonBean.EMPTY_BEAN;
  }

  private JsonBean startJob(RequestContext ctx) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String[] elements = ctx.getUrlElements();
    String jobIdentifier = elements[elements.length - 2];
    long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);

    // Authorization check
    AuthorizationEngine.startJob(String.valueOf(jobId));

    AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
        ctx.getRequest().getRemoteAddr(), "submit", "job", String.valueOf(jobId));
    // TODO(SQOOP-1638): This should be outsourced somewhere more suitable than
    // here
    if (JobManager.getInstance().getNotificationBaseUrl() == null) {
      String url = ctx.getRequest().getRequestURL().toString();
      JobManager.getInstance().setNotificationBaseUrl(
          url.split("v1")[0] + "/v1/job/status/notification/");
    }

    MSubmission submission = JobManager.getInstance()
        .start(jobId, prepareRequestEventContext(ctx));
    return new SubmissionBean(submission);
  }

  private JsonBean stopJob(RequestContext ctx) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String[] elements = ctx.getUrlElements();
    String jobIdentifier = elements[elements.length - 2];
    long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);

    // Authorization check
    AuthorizationEngine.stopJob(String.valueOf(jobId));

    AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
        ctx.getRequest().getRemoteAddr(), "stop", "job", String.valueOf(jobId));
    MSubmission submission = JobManager.getInstance().stop(jobId, prepareRequestEventContext(ctx));
    return new SubmissionBean(submission);
  }

  private JsonBean getJobStatus(RequestContext ctx) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    String[] elements = ctx.getUrlElements();
    String jobIdentifier = elements[elements.length - 2];
    long jobId = HandlerUtils.getJobIdFromIdentifier(jobIdentifier, repository);

    // Authorization check
    AuthorizationEngine.statusJob(String.valueOf(jobId));

    AuditLoggerManager.getInstance().logAuditEvent(ctx.getUserName(),
        ctx.getRequest().getRemoteAddr(), "status", "job", String.valueOf(jobId));
    MSubmission submission = JobManager.getInstance().status(jobId);

    return new SubmissionBean(submission);
  }

  private HttpEventContext prepareRequestEventContext(RequestContext ctx) {
    HttpEventContext httpEventContext = new HttpEventContext();
    httpEventContext.setUsername(ctx.getUserName());
    return httpEventContext;
  }

}
