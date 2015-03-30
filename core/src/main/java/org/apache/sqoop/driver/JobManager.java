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
package org.apache.sqoop.driver;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.driver.configuration.JobConfiguration;
import org.apache.sqoop.error.code.DriverError;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.job.etl.Transferable;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.request.HttpEventContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.utils.ClassUtils;

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
   * Driver manager will always add job id.
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
      context.getString(DriverConstants.SYSCFG_SUBMISSION_ENGINE);

    submissionEngine = (SubmissionEngine) ClassUtils
      .instantiate(submissionEngineClassName);
    if (submissionEngine == null) {
      throw new SqoopException(DriverError.DRIVER_0001,
        submissionEngineClassName);
    }

    submissionEngine.initialize(context,
        DriverConstants.PREFIX_SUBMISSION_ENGINE_CONFIG);

    // Execution engine
    String executionEngineClassName =
      context.getString(DriverConstants.SYSCFG_EXECUTION_ENGINE);

    executionEngine = (ExecutionEngine) ClassUtils
      .instantiate(executionEngineClassName);
    if (executionEngine == null) {
      throw new SqoopException(DriverError.DRIVER_0007,
        executionEngineClassName);
    }

    // We need to make sure that user has configured compatible combination of
    // submission engine and execution engine
    if (!submissionEngine
      .isExecutionEngineSupported(executionEngine.getClass())) {
      throw new SqoopException(DriverError.DRIVER_0008);
    }

    executionEngine.initialize(context,
        DriverConstants.PREFIX_EXECUTION_ENGINE_CONFIG);

    // Set up worker threads
    purgeThreshold = context.getLong(
      DriverConstants.SYSCFG_SUBMISSION_PURGE_THRESHOLD,
      DEFAULT_PURGE_THRESHOLD
      );
    purgeSleep = context.getLong(
      DriverConstants.SYSCFG_SUBMISSION_PURGE_SLEEP,
      DEFAULT_PURGE_SLEEP
      );

    purgeThread = new PurgeThread();
    purgeThread.start();

    updateSleep = context.getLong(
      DriverConstants.SYSCFG_SUBMISSION_UPDATE_SLEEP,
      DEFAULT_UPDATE_SLEEP
      );

    updateThread = new UpdateThread();
    updateThread.start();

    SqoopConfiguration.getInstance().getProvider()
      .registerListener(new CoreConfigurationListener(this));

    LOG.info("Submission manager initialized: OK");
  }

  public MSubmission start(long jobId, HttpEventContext ctx) {

    MSubmission mSubmission = createJobSubmission(ctx, jobId);
    JobRequest jobRequest = createJobRequest(jobId, mSubmission);
    // Bootstrap job to execute in the configured execution engine
    prepareJob(jobRequest);
    // Make sure that this job id is not currently running and submit the job
    // only if it's not.
    synchronized (getClass()) {
      MSubmission lastSubmission = RepositoryManager.getInstance().getRepository()
          .findLastSubmissionForJob(jobId);
      if (lastSubmission != null && lastSubmission.getStatus().isRunning()) {
        throw new SqoopException(DriverError.DRIVER_0002, "Job with id " + jobId);
      }
      // NOTE: the following is a blocking call
      boolean success = submissionEngine.submit(jobRequest);
      if (!success) {
        invokeDestroyerOnJobFailure(jobRequest);
        mSubmission.setStatus(SubmissionStatus.FAILURE_ON_SUBMIT);
      }
      // persist submission record to repository.
      // on failure we persist the FAILURE status, on success it is the SUCCESS
      // status ( which is the default one)
      RepositoryManager.getInstance().getRepository().createSubmission(mSubmission);
    }
    return mSubmission;
  }

  private JobRequest createJobRequest(long jobId, MSubmission submission) {
    // get job
    MJob job = getJob(jobId);

    // get from/to connections for the job
    MLink fromConnection = getLink(job.getFromLinkId());
    MLink toConnection = getLink(job.getToLinkId());

    // get from/to connectors for the connection
    SqoopConnector fromConnector = getSqoopConnector(fromConnection.getConnectorId());
    validateSupportedDirection(fromConnector, Direction.FROM);
    SqoopConnector toConnector = getSqoopConnector(toConnection.getConnectorId());
    validateSupportedDirection(toConnector, Direction.TO);

    // link config for the FROM part of the job
    Object fromLinkConfig = ClassUtils.instantiate(fromConnector.getLinkConfigurationClass());
    ConfigUtils.fromConfigs(fromConnection.getConnectorLinkConfig().getConfigs(), fromLinkConfig);

    // link config for the TO part of the job
    Object toLinkConfig = ClassUtils.instantiate(toConnector.getLinkConfigurationClass());
    ConfigUtils.fromConfigs(toConnection.getConnectorLinkConfig().getConfigs(), toLinkConfig);

    // from config for the job
    Object fromJob = ClassUtils.instantiate(fromConnector.getJobConfigurationClass(Direction.FROM));
    ConfigUtils.fromConfigs(job.getFromJobConfig().getConfigs(), fromJob);

    // to config for the job
    Object toJob = ClassUtils.instantiate(toConnector.getJobConfigurationClass(Direction.TO));
    ConfigUtils.fromConfigs(job.getToJobConfig().getConfigs(), toJob);

    // the only driver config for the job
    Object driverConfig = ClassUtils
        .instantiate(Driver.getInstance().getDriverJobConfigurationClass());
    ConfigUtils.fromConfigs(job.getDriverConfig().getConfigs(), driverConfig);

    // Create a job request for submit/execution
    JobRequest jobRequest = executionEngine.createJobRequest();
    // Save important variables to the job request
    jobRequest.setJobSubmission(submission);
    jobRequest.setConnector(Direction.FROM, fromConnector);
    jobRequest.setConnector(Direction.TO, toConnector);

    jobRequest.setConnectorLinkConfig(Direction.FROM, fromLinkConfig);
    jobRequest.setConnectorLinkConfig(Direction.TO, toLinkConfig);

    jobRequest.setJobConfig(Direction.FROM, fromJob);
    jobRequest.setJobConfig(Direction.TO, toJob);

    jobRequest.setDriverConfig(driverConfig);
    jobRequest.setJobName(job.getName());
    jobRequest.setJobId(job.getPersistenceId());
    jobRequest.setNotificationUrl(notificationBaseUrl + jobId);
    jobRequest.setIntermediateDataFormat(fromConnector.getIntermediateDataFormat(), Direction.FROM);
    jobRequest.setIntermediateDataFormat(toConnector.getIntermediateDataFormat(), Direction.TO);

    jobRequest.setFrom(fromConnector.getFrom());
    jobRequest.setTo(toConnector.getTo());

    // set all the jars
    addStandardJars(jobRequest);
    addConnectorClass(jobRequest, fromConnector);
    addConnectorClass(jobRequest, toConnector);
    addConnectorIDFClass(jobRequest, fromConnector.getIntermediateDataFormat());
    addConnectorIDFClass(jobRequest, toConnector.getIntermediateDataFormat());

    addConnectorInitializerJars(jobRequest, Direction.FROM);
    addConnectorInitializerJars(jobRequest, Direction.TO);
    addIDFDependentJars(jobRequest, Direction.FROM);
    addIDFDependentJars(jobRequest, Direction.TO);

    // call the intialize method
    initializeConnector(jobRequest, Direction.FROM);
    initializeConnector(jobRequest, Direction.TO);

    jobRequest.getJobSubmission().setFromSchema(getSchemaForConnector(jobRequest, Direction.FROM));
    jobRequest.getJobSubmission().setToSchema(getSchemaForConnector(jobRequest, Direction.TO));

    LOG.debug("Using entities: " + jobRequest.getFrom() + ", " + jobRequest.getTo());
    return jobRequest;
  }

  private void addConnectorClass(final JobRequest jobRequest, final SqoopConnector connector) {
    jobRequest.addJarForClass(connector.getClass());
  }

  private void addConnectorIDFClass(final JobRequest jobRequest, Class<? extends IntermediateDataFormat<?>>  idfClass) {
    jobRequest.addJarForClass(idfClass);
  }

  private void addStandardJars(JobRequest jobRequest) {
    // Let's register all important jars
    // sqoop-common
    jobRequest.addJarForClass(MapContext.class);
    // sqoop-core
    jobRequest.addJarForClass(Driver.class);
    // sqoop-spi
    jobRequest.addJarForClass(SqoopConnector.class);
    // Execution engine jar
    jobRequest.addJarForClass(executionEngine.getClass());
  }

  MSubmission createJobSubmission(HttpEventContext ctx, long jobId) {
    MSubmission summary = new MSubmission(jobId);
    summary.setCreationUser(ctx.getUsername());
    summary.setLastUpdateUser(ctx.getUsername());
    return summary;
  }

  SqoopConnector getSqoopConnector(long connnectorId) {
    return ConnectorManager.getInstance().getSqoopConnector(connnectorId);
  }

  void validateSupportedDirection(SqoopConnector connector, Direction direction) {
    // Make sure that connector supports the given direction
    if (!connector.getSupportedDirections().contains(direction)) {
      throw new SqoopException(DriverError.DRIVER_0011, "Connector: "
          + connector.getClass().getCanonicalName());
    }
  }

  MLink getLink(long linkId) {
    MLink link = RepositoryManager.getInstance().getRepository()
        .findLink(linkId);
    if (!link.getEnabled()) {
      throw new SqoopException(DriverError.DRIVER_0010, "Connection id: "
          + link.getPersistenceId());
    }
    return link;
  }

  MJob getJob(long jobId) {
    MJob job = RepositoryManager.getInstance().getRepository().findJob(jobId);
    if (job == null) {
      throw new SqoopException(DriverError.DRIVER_0004, "Unknown job id: " + jobId);
    }

    if (!job.getEnabled()) {
      throw new SqoopException(DriverError.DRIVER_0009, "Job id: " + job.getPersistenceId());
    }
    return job;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void initializeConnector(JobRequest jobRequest, Direction direction) {
    Initializer initializer = getConnectorInitializer(jobRequest, direction);
    InitializerContext initializerContext = getConnectorInitializerContext(jobRequest, direction);

    // Initialize submission from the connector perspective
    initializer.initialize(initializerContext, jobRequest.getConnectorLinkConfig(direction),
        jobRequest.getJobConfig(direction));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Schema getSchemaForConnector(JobRequest jobRequest, Direction direction) {

    Initializer initializer = getConnectorInitializer(jobRequest, direction);
    InitializerContext initializerContext = getConnectorInitializerContext(jobRequest, direction);

    return initializer.getSchema(initializerContext, jobRequest.getConnectorLinkConfig(direction),
        jobRequest.getJobConfig(direction));
  }

  @SuppressWarnings("unchecked")
  private void addIDFDependentJars(JobRequest jobRequest, Direction direction) {
    Class<? extends IntermediateDataFormat<?>> idfClass = jobRequest.getIntermediateDataFormat(direction);
    IntermediateDataFormat<?> idf = ((IntermediateDataFormat<?>) ClassUtils.instantiate(idfClass));
    jobRequest.addJars(idf.getJars());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void addConnectorInitializerJars(JobRequest jobRequest, Direction direction) {

    Initializer initializer = getConnectorInitializer(jobRequest, direction);
    InitializerContext initializerContext = getConnectorInitializerContext(jobRequest, direction);
    // Add job specific jars to
    jobRequest.addJars(initializer.getJars(initializerContext,
        jobRequest.getConnectorLinkConfig(direction), jobRequest.getJobConfig(direction)));
  }

  @SuppressWarnings({ "rawtypes" })
  private Initializer getConnectorInitializer(JobRequest jobRequest, Direction direction) {
    Transferable transferable = direction.equals(Direction.FROM) ? jobRequest.getFrom() : jobRequest.getTo();
    Class<? extends Initializer> initializerClass = transferable.getInitializer();
    Initializer initializer = (Initializer) ClassUtils.instantiate(initializerClass);

    if (initializer == null) {
      throw new SqoopException(DriverError.DRIVER_0006,
          "Can't create connector initializer instance: " + initializerClass.getName());
    }
    return initializer;
  }

  private InitializerContext getConnectorInitializerContext(JobRequest jobRequest, Direction direction) {
    return new InitializerContext(jobRequest.getConnectorContext(direction));
  }

  void prepareJob(JobRequest request) {
    JobConfiguration jobConfiguration = (JobConfiguration) request.getDriverConfig();
    // We're directly moving configured number of extractors and loaders to
    // underlying request object. In the future we might need to throttle this
    // count based on other running jobs to meet our SLAs.
    request.setExtractors(jobConfiguration.throttlingConfig.numExtractors);
    request.setLoaders(jobConfiguration.throttlingConfig.numLoaders);

    // Delegate rest of the job to execution engine
    executionEngine.prepareJob(request);
  }

  /**
   * Callback that will be called only if we failed to submit the job to the
   * remote cluster.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  void invokeDestroyerOnJobFailure(JobRequest request) {
    Transferable from = request.getFrom();
    Transferable to = request.getTo();

    Class<? extends Destroyer> fromDestroyerClass = from.getDestroyer();
    Class<? extends Destroyer> toDestroyerClass = to.getDestroyer();
    Destroyer fromDestroyer = (Destroyer) ClassUtils.instantiate(fromDestroyerClass);
    Destroyer toDestroyer = (Destroyer) ClassUtils.instantiate(toDestroyerClass);

    if (fromDestroyer == null) {
      throw new SqoopException(DriverError.DRIVER_0006,
        "Can't create toDestroyer instance: " + fromDestroyerClass.getName());
    }

    if (toDestroyer == null) {
      throw new SqoopException(DriverError.DRIVER_0006,
          "Can't create toDestroyer instance: " + toDestroyerClass.getName());
    }

    DestroyerContext fromDestroyerContext = new DestroyerContext(
      request.getConnectorContext(Direction.FROM), false, request.getJobSubmission()
        .getFromSchema());
    DestroyerContext toDestroyerContext = new DestroyerContext(
        request.getConnectorContext(Direction.TO), false, request.getJobSubmission()
        .getToSchema());

    fromDestroyer.destroy(fromDestroyerContext, request.getConnectorLinkConfig(Direction.FROM),
        request.getJobConfig(Direction.FROM));
    toDestroyer.destroy(toDestroyerContext, request.getConnectorLinkConfig(Direction.TO),
        request.getJobConfig(Direction.TO));
  }

  public MSubmission stop(long jobId, HttpEventContext ctx) {

    Repository repository = RepositoryManager.getInstance().getRepository();
    MSubmission mSubmission = repository.findLastSubmissionForJob(jobId);

    if (mSubmission == null || !mSubmission.getStatus().isRunning()) {
      throw new SqoopException(DriverError.DRIVER_0003, "Job with id " + jobId
          + " is not running hence cannot stop");
    }
    submissionEngine.stop(mSubmission.getExternalJobId());

    mSubmission.setLastUpdateUser(ctx.getUsername());

    // Fetch new information to verify that the stop command has actually worked
    updateSubmission(mSubmission);

    // Return updated structure
    return mSubmission;
  }

  public MSubmission status(long jobId) {
    Repository repository = RepositoryManager.getInstance().getRepository();
    MSubmission mSubmission = repository.findLastSubmissionForJob(jobId);

    if (mSubmission == null) {
      return new MSubmission(jobId, new Date(), SubmissionStatus.NEVER_EXECUTED);
    }
    // If the submission is in running state, let's update it
    if (mSubmission.getStatus().isRunning()) {
      updateSubmission(mSubmission);
    }

    return mSubmission;
  }

  /**
   * Get latest status of the submission from execution engine and
   * persist that in the repository.
   *
   * @param submission Submission to update
   */
  public void updateSubmission(MSubmission submission) {
    submissionEngine.update(submission);
    RepositoryManager.getInstance().getRepository().updateSubmission(submission);
  }

  @Override
  public synchronized void configurationChanged() {
    LOG.info("Begin submission engine manager reconfiguring");
    MapContext newContext = SqoopConfiguration.getInstance().getContext();
    MapContext oldContext = SqoopConfiguration.getInstance().getOldContext();

    String newSubmissionEngineClassName = newContext
      .getString(DriverConstants.SYSCFG_SUBMISSION_ENGINE);
    if (newSubmissionEngineClassName == null
      || newSubmissionEngineClassName.trim().length() == 0) {
      throw new SqoopException(DriverError.DRIVER_0001,
        newSubmissionEngineClassName);
    }

    String oldSubmissionEngineClassName = oldContext
      .getString(DriverConstants.SYSCFG_SUBMISSION_ENGINE);
    if (!newSubmissionEngineClassName.equals(oldSubmissionEngineClassName)) {
      LOG.warn("Submission engine cannot be replaced at the runtime. " +
        "You might need to restart the server.");
    }

    String newExecutionEngineClassName = newContext
      .getString(DriverConstants.SYSCFG_EXECUTION_ENGINE);
    if (newExecutionEngineClassName == null
      || newExecutionEngineClassName.trim().length() == 0) {
      throw new SqoopException(DriverError.DRIVER_0007,
        newExecutionEngineClassName);
    }

    String oldExecutionEngineClassName = oldContext
      .getString(DriverConstants.SYSCFG_EXECUTION_ENGINE);
    if (!newExecutionEngineClassName.equals(oldExecutionEngineClassName)) {
      LOG.warn("Execution engine cannot be replaced at the runtime. " +
        "You might need to restart the server.");
    }

    // Set up worker threads
    purgeThreshold = newContext.getLong(
      DriverConstants.SYSCFG_SUBMISSION_PURGE_THRESHOLD,
      DEFAULT_PURGE_THRESHOLD
      );
    purgeSleep = newContext.getLong(
      DriverConstants.SYSCFG_SUBMISSION_PURGE_SLEEP,
      DEFAULT_PURGE_SLEEP
      );
    purgeThread.interrupt();

    updateSleep = newContext.getLong(
      DriverConstants.SYSCFG_SUBMISSION_UPDATE_SLEEP,
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
              .findUnfinishedSubmissions();

          for (MSubmission submission : unfinishedSubmissions) {
            updateSubmission(submission);
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
