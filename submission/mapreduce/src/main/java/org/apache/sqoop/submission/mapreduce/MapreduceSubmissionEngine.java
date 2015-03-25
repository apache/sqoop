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
package org.apache.sqoop.submission.mapreduce;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.error.code.MapreduceSubmissionError;
import org.apache.sqoop.execution.mapreduce.MRJobRequest;
import org.apache.sqoop.execution.mapreduce.MapreduceExecutionEngine;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;


/**
 * This is very simple and straightforward implementation of map-reduce based
 * submission engine.
 */
public class MapreduceSubmissionEngine extends SubmissionEngine {

  private static Logger LOG = Logger.getLogger(MapreduceSubmissionEngine.class);

  /**
   * Global configuration object that is build from hadoop configuration files
   * on engine initialization and cloned during each new submission creation.
   */
  private Configuration globalConfiguration;

  /**
   * Job client that is configured to talk to one specific Job tracker.
   */
  private JobClient jobClient;


  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(MapContext context, String prefix) {
    super.initialize(context, prefix);
    LOG.info("Initializing Map-reduce Submission Engine");

    // Build global configuration, start with empty configuration object
    globalConfiguration = new Configuration();
    globalConfiguration.clear();

    // Load configured hadoop configuration directory
    String configDirectory = context.getString(prefix + Constants.CONF_CONFIG_DIR);

    // Git list of files ending with "-site.xml" (configuration files)
    File dir = new File(configDirectory);
    String [] files = dir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("-site.xml");
      }
    });

    if(files == null) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0002,
        "Invalid Hadoop configuration directory (not a directory or permission issues): " + configDirectory);
    }

    // Add each such file to our global configuration object
    for (String file : files) {
      LOG.info("Found hadoop configuration file " + file);
      try {
        globalConfiguration.addResource(new File(configDirectory, file).toURI().toURL());
      } catch (MalformedURLException e) {
        LOG.error("Can't load configuration file: " + file, e);
      }
    }

    // Save our own property inside the job to easily identify Sqoop jobs
    globalConfiguration.setBoolean(Constants.SQOOP_JOB, true);

    // Create job client
    try {
      jobClient = new JobClient(new JobConf(globalConfiguration));
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0002, e);
    }

    if(isLocal()) {
      LOG.info("Detected MapReduce local mode, some methods might not work correctly.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    super.destroy();
    LOG.info("Destroying Mapreduce Submission Engine");

    // Closing job client
    try {
      jobClient.close();
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0005, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isExecutionEngineSupported(Class<?> executionEngineClass) {
    return executionEngineClass == MapreduceExecutionEngine.class;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean submit(JobRequest mrJobRequest) {
    // We're supporting only map reduce jobs
    MRJobRequest request = (MRJobRequest) mrJobRequest;

    // Clone global configuration
    Configuration configuration = new Configuration(globalConfiguration);

    // Serialize driver context into job configuration
    for(Map.Entry<String, String> entry: request.getDriverContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null driver context value for key " + entry.getKey());
        continue;
      }
      configuration.set(entry.getKey(), entry.getValue());
    }

    // Serialize connector context as a sub namespace
    for(Map.Entry<String, String> entry : request.getConnectorContext(Direction.FROM)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      configuration.set(
        MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT + entry.getKey(),
        entry.getValue());
    }

    for(Map.Entry<String, String> entry : request.getConnectorContext(Direction.TO)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      configuration.set(
          MRJobConstants.PREFIX_CONNECTOR_TO_CONTEXT + entry.getKey(),
          entry.getValue());
    }

    // Set up notification URL if it's available
    if(request.getNotificationUrl() != null) {
      configuration.set("job.end.notification.url", request.getNotificationUrl());
    }

    // Turn off speculative execution
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    // Promote all required jars to the job
    configuration.set("tmpjars", StringUtils.join(request.getJars(), ","));

    try {
      Job job = new Job(configuration);

      // link configs
      MRConfigurationUtils.setConnectorLinkConfig(Direction.FROM, job, request.getConnectorLinkConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorLinkConfig(Direction.TO, job, request.getConnectorLinkConfig(Direction.TO));

      // from and to configs
      MRConfigurationUtils.setConnectorJobConfig(Direction.FROM, job, request.getJobConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorJobConfig(Direction.TO, job, request.getJobConfig(Direction.TO));

      MRConfigurationUtils.setDriverConfig(job, request.getDriverConfig());
      MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, request.getJobSubmission().getFromSchema());
      MRConfigurationUtils.setConnectorSchema(Direction.TO, job, request.getJobSubmission().getToSchema());

      if(request.getJobName() != null) {
        job.setJobName("Sqoop: " + request.getJobName());
      } else {
        job.setJobName("Sqoop job with id: " + request.getJobId());
      }

      job.setInputFormatClass(request.getInputFormatClass());

      job.setMapperClass(request.getMapperClass());
      job.setMapOutputKeyClass(request.getMapOutputKeyClass());
      job.setMapOutputValueClass(request.getMapOutputValueClass());

      // Set number of reducers as number of configured loaders  or suppress
      // reduce phase entirely if loaders are not set at all.
      if(request.getLoaders() != null) {
        job.setNumReduceTasks(request.getLoaders());
      } else {
        job.setNumReduceTasks(0);
      }

      job.setOutputFormatClass(request.getOutputFormatClass());
      job.setOutputKeyClass(request.getOutputKeyClass());
      job.setOutputValueClass(request.getOutputValueClass());

      // If we're in local mode than wait on completion. Local job runner do not
      // seems to be exposing API to get previously submitted job which makes
      // other methods of the submission engine quite useless.
      // NOTE: The minicluster mode is not local. It runs similar to a real MR cluster but
      // only that it is in the same JVM
      if (isLocal()) {
        submitToLocalRunner(request, job);
      } else {
        submitToCluster(request, job);
      }
      LOG.debug("Executed new map-reduce job with id " + job.getJobID().toString());
    } catch (Exception e) {
      SubmissionError error = new SubmissionError();
      error.setErrorSummary(e.toString());
      StringWriter writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      writer.flush();
      error.setErrorDetails(writer.toString());

      request.getJobSubmission().setError(error);
      LOG.error("Error in submitting job", e);
      return false;
    }
    return true;
  }

  private void submitToCluster(MRJobRequest request, Job job) throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
    request.getJobSubmission().setExternalJobId(job.getJobID().toString());
    request.getJobSubmission().setExternalLink(job.getTrackingURL());
  }

  private void submitToLocalRunner(MRJobRequest request, Job job) throws IOException, InterruptedException,
      ClassNotFoundException {
    boolean successful = job.waitForCompletion(true);
    if (successful) {
      request.getJobSubmission().setStatus(SubmissionStatus.SUCCEEDED);
    } else {
      // treat any other state as failed
      request.getJobSubmission().setStatus(SubmissionStatus.FAILED);
    }
    request.getJobSubmission().setExternalJobId(job.getJobID().toString());
    request.getJobSubmission().setExternalLink(job.getTrackingURL());

    request.getJobSubmission().setStatus(convertMapreduceState(job.getJobState().getValue()));
    // there is no failure info in this job api, unlike the running job
    request.getJobSubmission().setError(null);
    request.getJobSubmission().setProgress((job.mapProgress() + job.reduceProgress()) / 2);
    request.getJobSubmission().setCounters(convertHadoop2MapreduceCounters(job.getCounters()));
    request.getJobSubmission().setLastUpdateDate(new Date());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(String externalJobId) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(externalJobId));
      if(runningJob == null) {
        return;
      }

      runningJob.killJob();
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  private SubmissionStatus status(RunningJob runningJob) {
    try {
      if(runningJob == null) {
        return SubmissionStatus.UNKNOWN;
      }

      int status = runningJob.getJobState();
      return convertMapreduceState(status);

    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }


  private SubmissionError error(RunningJob runningJob) {
    try {
      if (runningJob == null) {
        return null;
      }
      int status = runningJob.getJobState();
      if (status == JobStatus.FAILED || status == JobStatus.KILLED) {
        SubmissionError error = new SubmissionError();
        error.setErrorSummary("Job Failed with status:" + status);
        error.setErrorDetails(runningJob.getFailureInfo());
        return error;
      }
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
    return null;
  }

  private double progress(RunningJob runningJob) {
    try {
      if(runningJob == null) {
        // Return default value
        return -1;
      }
      return (runningJob.mapProgress() + runningJob.reduceProgress()) / 2;
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }


  private Counters counters(RunningJob runningJob) {
    try {
      if(runningJob == null) {
        // Return default value
        return null;
      }

      return convertHadoop1MapreduceCounters(runningJob.getCounters());
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  /**
   * Convert map-reduce specific job status constants to Sqoop job status
   * constants.
   *
   * @param status Map-reduce job constant
   * @return Equivalent submission status
   */
  private SubmissionStatus convertMapreduceState(int status) {
    if(status == JobStatus.PREP) {
      return SubmissionStatus.BOOTING;
    } else if (status == JobStatus.RUNNING) {
      return SubmissionStatus.RUNNING;
    } else if (status == JobStatus.FAILED) {
      return SubmissionStatus.FAILED;
    } else if (status == JobStatus.KILLED) {
      return SubmissionStatus.FAILED;
    } else if (status == JobStatus.SUCCEEDED) {
      return SubmissionStatus.SUCCEEDED;
    }

    throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0004,
      "Unknown status " + status);
  }


  /**
   * Convert Hadoop1 counters to Sqoop counters.
   *
   * @param hadoopCounters Hadoop counters
   * @return Appropriate Sqoop counters
   */

  private Counters convertHadoop1MapreduceCounters(org.apache.hadoop.mapred.Counters hadoopCounters) {
    Counters sqoopCounters = new Counters();

    if (hadoopCounters == null) {
      return sqoopCounters;
    }

    for (org.apache.hadoop.mapred.Counters.Group counterGroup : hadoopCounters) {
      CounterGroup sqoopGroup = new CounterGroup(counterGroup.getName());
      for (org.apache.hadoop.mapred.Counters.Counter hadoopCounter : counterGroup) {
        Counter sqoopCounter = new Counter(hadoopCounter.getName(), hadoopCounter.getValue());
        sqoopGroup.addCounter(sqoopCounter);
      }
      sqoopCounters.addCounterGroup(sqoopGroup);
    }

    return sqoopCounters;
  }

  /**
   * Convert Hadoop2 counters to Sqoop counters.
   *
   * @param hadoopCounters Hadoop counters
   * @return Appropriate Sqoop counters
   */
  private Counters convertHadoop2MapreduceCounters(org.apache.hadoop.mapreduce.Counters hadoopCounters) {
    Counters sqoopCounters = new Counters();

    if (hadoopCounters == null) {
      return sqoopCounters;
    }

    for (org.apache.hadoop.mapreduce.CounterGroup counterGroup : hadoopCounters) {
      CounterGroup sqoopGroup = new CounterGroup(counterGroup.getName());
      for (org.apache.hadoop.mapreduce.Counter hadoopCounter : counterGroup) {
        Counter sqoopCounter = new Counter(hadoopCounter.getName(), hadoopCounter.getValue());
        sqoopGroup.addCounter(sqoopCounter);
      }
      sqoopCounters.addCounterGroup(sqoopGroup);
    }

    return sqoopCounters;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void update(MSubmission submission) {
    double progress = -1;
    Counters counters = null;
    String externalJobId = submission.getExternalJobId();
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(externalJobId));

      SubmissionStatus newStatus = status(runningJob);
      SubmissionError error = error(runningJob);

      if (newStatus.isRunning()) {
        progress = progress(runningJob);
      } else {
        counters = counters(runningJob);
      }
      // these properties change as the job runs, rest of the submission attributes
      // do not change as job runs
      submission.setStatus(newStatus);
      submission.setError(error);
      submission.setProgress(progress);
      submission.setCounters(counters);
      submission.setLastUpdateDate(new Date());
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }
  /**
   * Detect MapReduce local mode.
   *
   * @return True if we're running in local mode
   */
  private boolean isLocal() {
    // If framework is set to YARN, then we can't be running in local mode
    if("yarn".equals(globalConfiguration.get("mapreduce.framework.name"))) {
      return false;
    }

    // If job tracker address is "local" then we're running in local mode
    return "local".equals(globalConfiguration.get("mapreduce.jobtracker.address"))
        || "local".equals(globalConfiguration.get("mapred.job.tracker"));
  }

}
