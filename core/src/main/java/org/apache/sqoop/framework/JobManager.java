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
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.request.HttpEventContext;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
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

    MConnection connection = repository.findConnection(job.getConnectionId());

    if (!connection.getEnabled()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0010,
        "Connection id: " + connection.getPersistenceId());
    }

    SqoopConnector connector =
      ConnectorManager.getInstance().getConnector(job.getConnectorId());

    // Transform forms to connector specific classes
    Object connectorConnection = ClassUtils.instantiate(
      connector.getConnectionConfigurationClass());
    FormUtils.fromForms(connection.getConnectorPart().getForms(),
      connectorConnection);

    Object connectorJob = ClassUtils.instantiate(
      connector.getJobConfigurationClass(job.getType()));
    FormUtils.fromForms(job.getConnectorPart().getForms(), connectorJob);

    // Transform framework specific forms
    Object frameworkConnection = ClassUtils.instantiate(
      FrameworkManager.getInstance().getConnectionConfigurationClass());
    FormUtils.fromForms(connection.getFrameworkPart().getForms(),
      frameworkConnection);

    Object frameworkJob = ClassUtils.instantiate(
      FrameworkManager.getInstance().getJobConfigurationClass(job.getType()));
    FormUtils.fromForms(job.getFrameworkPart().getForms(), frameworkJob);

    // Create request object
    MSubmission summary = new MSubmission(jobId);
    SubmissionRequest request = executionEngine.createSubmissionRequest();

    summary.setCreationUser(username);
    summary.setLastUpdateUser(username);

    // Save important variables to the submission request
    request.setSummary(summary);
    request.setConnector(connector);
    request.setConfigConnectorConnection(connectorConnection);
    request.setConfigConnectorJob(connectorJob);
    request.setConfigFrameworkConnection(frameworkConnection);
    request.setConfigFrameworkJob(frameworkJob);
    request.setJobType(job.getType());
    request.setJobName(job.getName());
    request.setJobId(job.getPersistenceId());
    request.setNotificationUrl(notificationBaseUrl + jobId);

    // Let's register all important jars
    // sqoop-common
    request.addJarForClass(MapContext.class);
    // sqoop-core
    request.addJarForClass(FrameworkManager.class);
    // sqoop-spi
    request.addJarForClass(SqoopConnector.class);
    // Execution engine jar
    request.addJarForClass(executionEngine.getClass());
    // Connector in use
    request.addJarForClass(connector.getClass());

    // Extra libraries that Sqoop code requires
    request.addJarForClass(JSONValue.class);

    // Get connector callbacks
    switch (job.getType()) {
      case IMPORT:
        request.setConnectorCallbacks(connector.getImporter());
        break;
      case EXPORT:
        request.setConnectorCallbacks(connector.getExporter());
        break;
      default:
        throw new SqoopException(FrameworkError.FRAMEWORK_0005,
          "Unsupported job type " + job.getType().name());
    }
    LOG.debug("Using callbacks: " + request.getConnectorCallbacks());

    // Initialize submission from connector perspective
    CallbackBase baseCallbacks = request.getConnectorCallbacks();

    Class<? extends Initializer> initializerClass = baseCallbacks
      .getInitializer();
    Initializer initializer = (Initializer) ClassUtils
      .instantiate(initializerClass);

    if (initializer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
        "Can't create initializer instance: " + initializerClass.getName());
    }

    // Initializer context
    InitializerContext initializerContext = new InitializerContext(
      request.getConnectorContext());

    // Initialize submission from connector perspective
    initializer.initialize(initializerContext,
      request.getConfigConnectorConnection(),
      request.getConfigConnectorJob());

    // Add job specific jars to
    request.addJars(initializer.getJars(initializerContext,
      request.getConfigConnectorConnection(),
      request.getConfigConnectorJob()));

    // Retrieve and persist the schema
    request.getSummary().setConnectorSchema(initializer.getSchema(
      initializerContext,
      request.getConfigConnectorConnection(),
      request.getConfigConnectorJob()
      ));

    // Bootstrap job from framework perspective
    switch (job.getType()) {
      case IMPORT:
        prepareImportSubmission(request);
        break;
      case EXPORT:
        prepareExportSubmission(request);
        break;
      default:
        throw new SqoopException(FrameworkError.FRAMEWORK_0005,
          "Unsupported job type " + job.getType().name());
    }

    // Make sure that this job id is not currently running and submit the job
    // only if it's not.
    synchronized (getClass()) {
      MSubmission lastSubmission = repository.findSubmissionLastForJob(jobId);
      if (lastSubmission != null && lastSubmission.getStatus().isRunning()) {
        throw new SqoopException(FrameworkError.FRAMEWORK_0002,
          "Job with id " + jobId);
      }

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

  private void prepareImportSubmission(SubmissionRequest request) {
    ImportJobConfiguration jobConfiguration = (ImportJobConfiguration) request
      .getConfigFrameworkJob();

    // Initialize the map-reduce part (all sort of required classes, ...)
    request.setOutputDirectory(jobConfiguration.output.outputDirectory);

    // We're directly moving configured number of extractors and loaders to
    // underlying request object. In the future we might need to throttle this
    // count based on other running jobs to meet our SLAs.
    request.setExtractors(jobConfiguration.throttling.extractors);
    request.setLoaders(jobConfiguration.throttling.loaders);

    // Delegate rest of the job to execution engine
    executionEngine.prepareImportSubmission(request);
  }

  private void prepareExportSubmission(SubmissionRequest request) {
    ExportJobConfiguration jobConfiguration = (ExportJobConfiguration) request
      .getConfigFrameworkJob();

    // We're directly moving configured number of extractors and loaders to
    // underlying request object. In the future we might need to throttle this
    // count based on other running jobs to meet our SLAs.
    request.setExtractors(jobConfiguration.throttling.extractors);
    request.setLoaders(jobConfiguration.throttling.loaders);

    // Delegate rest of the job to execution engine
    executionEngine.prepareExportSubmission(request);
  }

  /**
   * Callback that will be called only if we failed to submit the job to the
   * remote cluster.
   */
  private void destroySubmission(SubmissionRequest request) {
    CallbackBase baseCallbacks = request.getConnectorCallbacks();

    Class<? extends Destroyer> destroyerClass = baseCallbacks.getDestroyer();
    Destroyer destroyer = (Destroyer) ClassUtils.instantiate(destroyerClass);

    if (destroyer == null) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0006,
        "Can't create destroyer instance: " + destroyerClass.getName());
    }

    DestroyerContext destroyerContext = new DestroyerContext(
      request.getConnectorContext(), false, request.getSummary()
        .getConnectorSchema());

    // Initialize submission from connector perspective
    destroyer.destroy(destroyerContext, request.getConfigConnectorConnection(),
      request.getConfigConnectorJob());
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
