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
package org.apache.sqoop.framework;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.ConnectorType;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.framework.configuration.JobConfiguration;
import org.apache.sqoop.request.HttpEventContext;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.job.etl.*;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.utils.ClassUtils;
import org.json.simple.JSONValue;

import java.util.Date;
import java.util.List;

public class JobManager implements Reconfigurable {
  /**
   * Logger object.
   */
  private static final Logger LOG = Logger.getLogger(JobManager.class);

  /**
   * Private instance to singleton of this class.
   */
  private static JobManager instance;
  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance
   * issue.
   */
  static {
    instance = new JobManager();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static JobManager getInstance() {
    return instance;
  }

  /**
   * Allows to set instance in case that it's need.
   *
   * This method should not be normally used as the default instance should be
   * sufficient. One target user use case for this method are unit tests.
   *
   * @param newInstance
   *          New instance
   */
  public static void setInstance(JobManager newInstance) {
    instance = newInstance;
  }

  /**
   * Default interval for purging old submissions from repository.
   */
  private static final long DEFAULT_PURGE_THRESHOLD = 24 * 60 * 60 * 1000;

  /**
   * Default sleep interval for purge thread.
   */
  private static final long DEFAULT_PURGE_SLEEP = 24 * 60 * 60 * 1000;

  /**
   * Default interval for update thread.
   */
  private static final long DEFAULT_UPDATE_SLEEP = 60 * 5 * 1000;

  /**
   * Configured submission engine instance
   */
  private SubmissionEngine submissionEngine;

  /**
   * Configured execution engine instance
   */
  private ExecutionEngine executionEngine;

  /**
   * Purge thread that will periodically remove old submissions from repository.
   */
  private PurgeThread purgeThread = null;

  /**
   * Update thread that will periodically check status of running submissions.
   */
  private UpdateThread updateThread = null;

  /**
   * Synchronization variable between threads.
   */
  private boolean running = true;

  /**
   * Specifies how old submissions should be removed from repository.
   */
  private long purgeThreshold;

  /**
   * Number of milliseconds for purge thread to sleep.
   */
  private long purgeSleep;

  /**
   * Number of milliseconds for update thread to slepp.
   */
  private long updateSleep;

  /**
   * Base notification URL.
   *
   * Framework manager will always add job id.
   */
  private String notificationBaseUrl;

  /**
   * Set notification base URL.
   *
   * @param url
   *          Base URL
   */
  public void setNotificationBaseUrl(String url) {
    LOG.debug("Setting notification base URL to " + url);
    notificationBaseUrl = url;
  }

  /**
   * Get base notification url.
   *
   * @return String representation of the URL
   */
  public String getNotificationBaseUrl() {
    return notificationBaseUrl;
  }

  public synchronized void destroy() {
    LOG.trace("Begin submission engine manager destroy");

    running = false;

    try {
      purgeThread.interrupt();
      purgeThread.join();
    } catch (InterruptedException e) {
      // TODO(jarcec): Do I want to wait until it actually finish here?
      LOG.error("Interrupted joining purgeThread");
    }

    try {
      updateThread.interrupt();
      updateThread.join();
    } catch (InterruptedException e) {
      // TODO(jarcec): Do I want to wait until it actually finish here?
      LOG.error("Interrupted joining updateThread");
    }

    if (submissionEngine != null) {
      submissionEngine.destroy();
    }

    if (executionEngine != null) {
      executionEngine.destroy();
    }
  }

  public synchronized void initialize() {
    LOG.trace("Begin submission engine manager initialization");
    MapContext context = SqoopConfiguration.getInstance().getContext();

    // Let's load configured submission engine
    String submissionEngineClassName =
      context.getString(FrameworkConstants.SYSCFG_SUBMISSION_ENGINE);

    submissionEngine = (SubmissionEngine) ClassUtils
      .instantiate(submissionEngineClassName);
    if (submissionEngine == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0001,
        submissionEngineClassName);
    }

    submissionEngine.initialize(context,
      FrameworkConstants.PREFIX_SUBMISSION_ENGINE_CONFIG);

    // Execution engine
    String executionEngineClassName =
      context.getString(FrameworkConstants.SYSCFG_EXECUTION_ENGINE);

    executionEngine = (ExecutionEngine) ClassUtils
      .instantiate(executionEngineClassName);
    if (executionEngine == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0007,
        executionEngineClassName);
    }

    // We need to make sure that user has configured compatible combination of
    // submission engine and execution engine
    if (!submissionEngine
      .isExecutionEngineSupported(executionEngine.getClass())) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0008);
    }

    executionEngine.initialize(context,
      FrameworkConstants.PREFIX_EXECUTION_ENGINE_CONFIG);

    // Set up worker threads
    purgeThreshold = context.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_PURGE_THRESHOLD,
      DEFAULT_PURGE_THRESHOLD
      );
    purgeSleep = context.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_PURGE_SLEEP,
      DEFAULT_PURGE_SLEEP
      );

    purgeThread = new PurgeThread();
    purgeThread.start();

    updateSleep = context.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_UPDATE_SLEEP,
      DEFAULT_UPDATE_SLEEP
      );

    updateThread = new UpdateThread();
    updateThread.start();

    SqoopConfiguration.getInstance().getProvider()
      .registerListener(new CoreConfigurationListener(this));

    LOG.info("Submission manager initialized: OK");
  }

  public MSubmission submit(long jobId, HttpEventContext ctx) {
    String username = ctx.getUsername();

    Repository repository = RepositoryManager.getInstance().getRepository();

    MJob job = repository.findJob(jobId);
    if (job == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0004,
        "Unknown job id " + jobId);
    }

    if (!job.getEnabled()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0009,
        "Job id: " + job.getPersistenceId());
    }

    MConnection fromConnection = repository.findConnection(job.getConnectionId(ConnectorType.FROM));
    MConnection toConnection = repository.findConnection(job.getConnectionId(ConnectorType.TO));

    if (!fromConnection.getEnabled()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0010,
        "Connection id: " + fromConnection.getPersistenceId());
    }

    if (!toConnection.getEnabled()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0010,
          "Connection id: " + toConnection.getPersistenceId());
    }

    SqoopConnector fromConnector =
      ConnectorManager.getInstance().getConnector(job.getConnectorId(ConnectorType.FROM));
    SqoopConnector toConnector =
        ConnectorManager.getInstance().getConnector(job.getConnectorId(ConnectorType.TO));

    // Transform forms to fromConnector specific classes
    Object fromConnectorConnection = ClassUtils.instantiate(
        fromConnector.getConnectionConfigurationClass());
    FormUtils.fromForms(fromConnection.getConnectorPart().getForms(),
      fromConnectorConnection);

    Object fromJob = ClassUtils.instantiate(
      fromConnector.getJobConfigurationClass(ConnectorType.FROM));
    FormUtils.fromForms(
        job.getConnectorPart(ConnectorType.FROM).getForms(), fromJob);

    // Transform forms to toConnector specific classes
    Object toConnectorConnection = ClassUtils.instantiate(
        toConnector.getConnectionConfigurationClass());
    FormUtils.fromForms(toConnection.getConnectorPart().getForms(),
        toConnectorConnection);

    Object toJob = ClassUtils.instantiate(
        toConnector.getJobConfigurationClass(ConnectorType.TO));
    FormUtils.fromForms(job.getConnectorPart(ConnectorType.TO).getForms(), toJob);

    // Transform framework specific forms
    Object fromFrameworkConnection = ClassUtils.instantiate(
      FrameworkManager.getInstance().getConnectionConfigurationClass());
    Object toFrameworkConnection = ClassUtils.instantiate(
        FrameworkManager.getInstance().getConnectionConfigurationClass());
    FormUtils.fromForms(fromConnection.getFrameworkPart().getForms(),
      fromFrameworkConnection);
    FormUtils.fromForms(toConnection.getFrameworkPart().getForms(),
        toFrameworkConnection);

    Object frameworkJob = ClassUtils.instantiate(
      FrameworkManager.getInstance().getJobConfigurationClass());
    FormUtils.fromForms(job.getFrameworkPart().getForms(), frameworkJob);

    // Create request object
    MSubmission summary = new MSubmission(jobId);
    SubmissionRequest request = executionEngine.createSubmissionRequest();

    summary.setCreationUser(username);
    summary.setLastUpdateUser(username);

    // Save important variables to the submission request
    request.setSummary(summary);
    request.setConnector(ConnectorType.FROM, fromConnector);
    request.setConnector(ConnectorType.TO, toConnector);
    request.setConnectorConnectionConfig(ConnectorType.FROM, fromConnectorConnection);
    request.setConnectorConnectionConfig(ConnectorType.TO, toConnectorConnection);
    request.setConnectorJobConfig(ConnectorType.FROM, fromJob);
    request.setConnectorJobConfig(ConnectorType.TO, toJob);
    // @TODO(Abe): Should we actually have 2 different Framework Connection config objects?
    request.setFrameworkConnectionConfig(ConnectorType.FROM, fromFrameworkConnection);
    request.setFrameworkConnectionConfig(ConnectorType.TO, toFrameworkConnection);
    request.setConfigFrameworkJob(frameworkJob);
    request.setJobName(job.getName());
    request.setJobId(job.getPersistenceId());
    request.setNotificationUrl(notificationBaseUrl + jobId);
    Class<? extends IntermediateDataFormat<?>> dataFormatClass =
      fromConnector.getIntermediateDataFormat();
    request.setIntermediateDataFormat(fromConnector.getIntermediateDataFormat());
    // Create request object

    // Let's register all important jars
    // sqoop-common
    request.addJarForClass(MapContext.class);
    // sqoop-core
    request.addJarForClass(FrameworkManager.class);
    // sqoop-spi
    request.addJarForClass(SqoopConnector.class);
    // Execution engine jar
    request.addJarForClass(executionEngine.getClass());
    // Connectors in use
    request.addJarForClass(fromConnector.getClass());
    request.addJarForClass(toConnector.getClass());

    // Extra libraries that Sqoop code requires
    request.addJarForClass(JSONValue.class);

    // The IDF is used in the ETL process.
    request.addJarForClass(dataFormatClass);


    // Get callbacks
    request.setFromCallback(fromConnector.getFrom());
    request.setToCallback(toConnector.getTo());
    LOG.debug("Using callbacks: " + request.getFromCallback() + ", " + request.getToCallback());

    // Initialize submission from fromConnector perspective
    CallbackBase[] baseCallbacks = {
        request.getFromCallback(),
        request.getToCallback()
    };

    CallbackBase baseCallback;
    Class<? extends Initializer> initializerClass;
    Initializer initializer;
    InitializerContext initializerContext;

    // Initialize From Connector callback.
    baseCallback = request.getFromCallback();

    initializerClass = baseCallback
        .getInitializer();
    initializer = (Initializer) ClassUtils
        .instantiate(initializerClass);

    if (initializer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
          "Can't create initializer instance: " + initializerClass.getName());
    }

    // Initializer context
    initializerContext = new InitializerContext(request.getConnectorContext(ConnectorType.FROM));

    // Initialize submission from fromConnector perspective
    initializer.initialize(initializerContext,
        request.getConnectorConnectionConfig(ConnectorType.FROM),
        request.getConnectorJobConfig(ConnectorType.FROM));

    // Add job specific jars to
    request.addJars(initializer.getJars(initializerContext,
        request.getConnectorConnectionConfig(ConnectorType.FROM),
        request.getConnectorJobConfig(ConnectorType.FROM)));

    // @TODO(Abe): Alter behavior of Schema here. Need from Schema.
    // Retrieve and persist the schema
    request.getSummary().setConnectorSchema(initializer.getSchema(
        initializerContext,
        request.getConnectorConnectionConfig(ConnectorType.FROM),
        request.getConnectorJobConfig(ConnectorType.FROM)
    ));

    // Initialize To Connector callback.
    baseCallback = request.getToCallback();

    initializerClass = baseCallback
        .getInitializer();
    initializer = (Initializer) ClassUtils
        .instantiate(initializerClass);

    if (initializer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
          "Can't create initializer instance: " + initializerClass.getName());
    }

    // Initializer context
    initializerContext = new InitializerContext(request.getConnectorContext(ConnectorType.TO));

    // Initialize submission from fromConnector perspective
    initializer.initialize(initializerContext,
        request.getConnectorConnectionConfig(ConnectorType.TO),
        request.getConnectorJobConfig(ConnectorType.TO));

    // Add job specific jars to
    request.addJars(initializer.getJars(initializerContext,
        request.getConnectorConnectionConfig(ConnectorType.TO),
        request.getConnectorJobConfig(ConnectorType.TO)));

    // @TODO(Abe): Alter behavior of Schema here. Need To Schema.
    // Retrieve and persist the schema
//    request.getSummary().setConnectorSchema(initializer.getSchema(
//        initializerContext,
//        request.getConnectorConnectionConfig(ConnectorType.TO),
//        request.getConnectorJobConfig(ConnectorType.TO)
//    ));

    // Bootstrap job from framework perspective
    prepareSubmission(request);

    // Make sure that this job id is not currently running and submit the job
    // only if it's not.
    synchronized (getClass()) {
      MSubmission lastSubmission = repository.findSubmissionLastForJob(jobId);
      if (lastSubmission != null && lastSubmission.getStatus().isRunning()) {
        throw new SqoopException(FrameworkError.FRAMEWORK_0002,
          "Job with id " + jobId);
      }

      // @TODO(Abe): Call multiple destroyers.
      // TODO(jarcec): We might need to catch all exceptions here to ensure
      // that Destroyer will be executed in all cases.
      boolean submitted = submissionEngine.submit(request);
      if (!submitted) {
        destroySubmission(request);
        summary.setStatus(SubmissionStatus.FAILURE_ON_SUBMIT);
      }

      repository.createSubmission(summary);
    }

    // Return job status most recent
    return summary;
  }

  private void prepareSubmission(SubmissionRequest request) {
    JobConfiguration jobConfiguration = (JobConfiguration) request
        .getConfigFrameworkJob();

    // We're directly moving configured number of extractors and loaders to
    // underlying request object. In the future we might need to throttle this
    // count based on other running jobs to meet our SLAs.
    request.setExtractors(jobConfiguration.throttling.extractors);
    request.setLoaders(jobConfiguration.throttling.loaders);

    // Delegate rest of the job to execution engine
    executionEngine.prepareSubmission(request);
  }

  /**
   * Callback that will be called only if we failed to submit the job to the
   * remote cluster.
   */
  private void destroySubmission(SubmissionRequest request) {
    CallbackBase fromCallback = request.getFromCallback();
    CallbackBase toCallback = request.getToCallback();

    Class<? extends Destroyer> fromDestroyerClass = fromCallback.getDestroyer();
    Class<? extends Destroyer> toDestroyerClass = toCallback.getDestroyer();
    Destroyer fromDestroyer = (Destroyer) ClassUtils.instantiate(fromDestroyerClass);
    Destroyer toDestroyer = (Destroyer) ClassUtils.instantiate(toDestroyerClass);

    if (fromDestroyer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
        "Can't create toDestroyer instance: " + fromDestroyerClass.getName());
    }

    if (toDestroyer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
          "Can't create toDestroyer instance: " + toDestroyerClass.getName());
    }

    // @TODO(Abe): Update context to manage multiple connectors. As well as summary.
    DestroyerContext fromDestroyerContext = new DestroyerContext(
      request.getConnectorContext(ConnectorType.FROM), false, request.getSummary()
        .getConnectorSchema());
    DestroyerContext toDestroyerContext = new DestroyerContext(
        request.getConnectorContext(ConnectorType.TO), false, request.getSummary()
        .getConnectorSchema());

    // Initialize submission from connector perspective
    fromDestroyer.destroy(fromDestroyerContext, request.getConnectorConnectionConfig(ConnectorType.FROM),
        request.getConnectorJobConfig(ConnectorType.FROM));
    toDestroyer.destroy(toDestroyerContext, request.getConnectorConnectionConfig(ConnectorType.TO),
        request.getConnectorJobConfig(ConnectorType.TO));
  }

  public MSubmission stop(long jobId, HttpEventContext ctx) {
    String username = ctx.getUsername();

    Repository repository = RepositoryManager.getInstance().getRepository();
    MSubmission submission = repository.findSubmissionLastForJob(jobId);

    if (submission == null || !submission.getStatus().isRunning()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0003,
        "Job with id " + jobId + " is not running");
    }

    String externalId = submission.getExternalId();
    submissionEngine.stop(externalId);

    submission.setLastUpdateUser(username);

    // Fetch new information to verify that the stop command has actually worked
    update(submission);

    // Return updated structure
    return submission;
  }

  public MSubmission status(long jobId) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MSubmission submission = repository.findSubmissionLastForJob(jobId);

    if (submission == null) {
      return new MSubmission(jobId, new Date(), SubmissionStatus.NEVER_EXECUTED);
    }

    // If the submission is in running state, let's update it
    if (submission.getStatus().isRunning()) {
      update(submission);
    }

    return submission;
  }

  private void update(MSubmission submission) {
    double progress = -1;
    Counters counters = null;
    String externalId = submission.getExternalId();
    SubmissionStatus newStatus = submissionEngine.status(externalId);
    String externalLink = submissionEngine.externalLink(externalId);

    if (newStatus.isRunning()) {
      progress = submissionEngine.progress(externalId);
    } else {
      counters = submissionEngine.counters(externalId);
    }

    submission.setStatus(newStatus);
    submission.setProgress(progress);
    submission.setCounters(counters);
    submission.setExternalLink(externalLink);
    submission.setLastUpdateDate(new Date());

    RepositoryManager.getInstance().getRepository()
      .updateSubmission(submission);
  }

  @Override
  public synchronized void configurationChanged() {
    LOG.info("Begin submission engine manager reconfiguring");
    MapContext newContext = SqoopConfiguration.getInstance().getContext();
    MapContext oldContext = SqoopConfiguration.getInstance().getOldContext();

    String newSubmissionEngineClassName = newContext
      .getString(FrameworkConstants.SYSCFG_SUBMISSION_ENGINE);
    if (newSubmissionEngineClassName == null
      || newSubmissionEngineClassName.trim().length() == 0) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0001,
        newSubmissionEngineClassName);
    }

    String oldSubmissionEngineClassName = oldContext
      .getString(FrameworkConstants.SYSCFG_SUBMISSION_ENGINE);
    if (!newSubmissionEngineClassName.equals(oldSubmissionEngineClassName)) {
      LOG.warn("Submission engine cannot be replaced at the runtime. " +
        "You might need to restart the server.");
    }

    String newExecutionEngineClassName = newContext
      .getString(FrameworkConstants.SYSCFG_EXECUTION_ENGINE);
    if (newExecutionEngineClassName == null
      || newExecutionEngineClassName.trim().length() == 0) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0007,
        newExecutionEngineClassName);
    }

    String oldExecutionEngineClassName = oldContext
      .getString(FrameworkConstants.SYSCFG_EXECUTION_ENGINE);
    if (!newExecutionEngineClassName.equals(oldExecutionEngineClassName)) {
      LOG.warn("Execution engine cannot be replaced at the runtime. " +
        "You might need to restart the server.");
    }

    // Set up worker threads
    purgeThreshold = newContext.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_PURGE_THRESHOLD,
      DEFAULT_PURGE_THRESHOLD
      );
    purgeSleep = newContext.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_PURGE_SLEEP,
      DEFAULT_PURGE_SLEEP
      );
    purgeThread.interrupt();

    updateSleep = newContext.getLong(
      FrameworkConstants.SYSCFG_SUBMISSION_UPDATE_SLEEP,
      DEFAULT_UPDATE_SLEEP
      );
    updateThread.interrupt();

    LOG.info("Submission engine manager reconfigured.");
  }

  private class PurgeThread extends Thread {
    public PurgeThread() {
      super("PurgeThread");
    }

    public void run() {
      LOG.info("Starting submission manager purge thread");

      while (running) {
        try {
          LOG.info("Purging old submissions");
          Date threshold = new Date((new Date()).getTime() - purgeThreshold);
          RepositoryManager.getInstance().getRepository()
            .purgeSubmissions(threshold);
          Thread.sleep(purgeSleep);
        } catch (InterruptedException e) {
          LOG.debug("Purge thread interrupted", e);
        }
      }

      LOG.info("Ending submission manager purge thread");
    }
  }

  private class UpdateThread extends Thread {
    public UpdateThread() {
      super("UpdateThread");
    }

    public void run() {
      LOG.info("Starting submission manager update thread");

      while (running) {
        try {
          LOG.debug("Updating running submissions");

          // Let's get all running submissions from repository to check them out
          List<MSubmission> unfinishedSubmissions =
            RepositoryManager.getInstance().getRepository()
              .findSubmissionsUnfinished();

          for (MSubmission submission : unfinishedSubmissions) {
            update(submission);
          }

          Thread.sleep(updateSleep);
        } catch (InterruptedException e) {
          LOG.debug("Purge thread interrupted", e);
        }
      }

      LOG.info("Ending submission manager update thread");
    }
  }
}
