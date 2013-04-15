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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.execution.mapreduce.MRSubmissionRequest;
import org.apache.sqoop.execution.mapreduce.MapreduceExecutionEngine;
import org.apache.sqoop.framework.SubmissionRequest;
import org.apache.sqoop.framework.SubmissionEngine;
import org.apache.sqoop.job.JobConstants;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.submission.SubmissionStatus;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;


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
  public boolean isExecutionEngineSupported(Class executionEngineClass) {
    return executionEngineClass == MapreduceExecutionEngine.class;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean submit(SubmissionRequest generalRequest) {
    // We're supporting only map reduce jobs
    MRSubmissionRequest request = (MRSubmissionRequest) generalRequest;

    // Clone global configuration
    Configuration configuration = new Configuration(globalConfiguration);

    // Serialize job type as it will be needed by underlying execution engine
    configuration.set(JobConstants.JOB_TYPE, request.getJobType().name());

    // Serialize framework context into job configuration
    for(Map.Entry<String, String> entry: request.getFrameworkContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null framework context value for key " + entry.getKey());
        continue;
      }
      configuration.set(entry.getKey(), entry.getValue());
    }

    // Serialize connector context as a sub namespace
    for(Map.Entry<String, String> entry :request.getConnectorContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      configuration.set(
        JobConstants.PREFIX_CONNECTOR_CONTEXT + entry.getKey(),
        entry.getValue());
    }

    // Serialize configuration objects - Firstly configuration classes
    configuration.set(JobConstants.JOB_CONFIG_CLASS_CONNECTOR_CONNECTION,
      request.getConfigConnectorConnection().getClass().getName());
    configuration.set(JobConstants.JOB_CONFIG_CLASS_CONNECTOR_JOB,
      request.getConfigConnectorJob().getClass().getName());
    configuration.set(JobConstants.JOB_CONFIG_CLASS_FRAMEWORK_CONNECTION,
      request.getConfigFrameworkConnection().getClass().getName());
    configuration.set(JobConstants.JOB_CONFIG_CLASS_FRAMEWORK_JOB,
      request.getConfigFrameworkJob().getClass().getName());

    // Set up notification URL if it's available
    if(request.getNotificationUrl() != null) {
      configuration.set("job.end.notification.url", request.getNotificationUrl());
    }

    // Turn off speculative execution
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    // Promote all required jars to the job
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for(String jar : request.getJars()) {
      if(first) {
        first = false;
      } else {
        sb.append(",");
      }
      LOG.debug("Adding jar to the job: " + jar);
      sb.append(jar);
    }
    configuration.set("tmpjars", sb.toString());

    try {
      Job job = new Job(configuration);

      // And finally put all configuration objects to credentials cache
      Credentials credentials = job.getCredentials();
      credentials.addSecretKey(JobConstants.JOB_CONFIG_CONNECTOR_CONNECTION_KEY,
        FormUtils.toJson(request.getConfigConnectorConnection()).getBytes());
      credentials.addSecretKey(JobConstants.JOB_CONFIG_CONNECTOR_JOB_KEY,
        FormUtils.toJson(request.getConfigConnectorJob()).getBytes());
      credentials.addSecretKey(JobConstants.JOB_CONFIG_FRAMEWORK_CONNECTION_KEY,
        FormUtils.toJson(request.getConfigFrameworkConnection()).getBytes());
      credentials.addSecretKey(JobConstants.JOB_CONFIG_FRAMEWORK_JOB_KEY,
        FormUtils.toJson(request.getConfigFrameworkConnection()).getBytes());

      if(request.getJobName() != null) {
        job.setJobName("Sqoop: " + request.getJobName());
      } else {
        job.setJobName("Sqoop job with id: " + request.getJobId());
      }

      job.setInputFormatClass(request.getInputFormatClass());

      job.setMapperClass(request.getMapperClass());
      job.setMapOutputKeyClass(request.getMapOutputKeyClass());
      job.setMapOutputValueClass(request.getMapOutputValueClass());

      String outputDirectory = request.getOutputDirectory();
      if(outputDirectory != null) {
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
      }

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
      if(isLocal()) {
        job.waitForCompletion(true);
      } else {
        job.submit();
      }

      String jobId = job.getJobID().toString();
      request.getSummary().setExternalId(jobId);
      request.getSummary().setExternalLink(job.getTrackingURL());

      LOG.debug("Executed new map-reduce job with id " + jobId);
    } catch (Exception e) {
      request.getSummary().setException(e);
      LOG.error("Error in submitting job", e);
      return false;
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(String submissionId) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(submissionId));
      if(runningJob == null) {
        return;
      }

      runningJob.killJob();
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SubmissionStatus status(String submissionId) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(submissionId));
      if(runningJob == null) {
        return SubmissionStatus.UNKNOWN;
      }

      int status = runningJob.getJobState();
      return convertMapreduceState(status);

    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double progress(String submissionId) {
    try {
      // Get some reasonable approximation of map-reduce job progress
      // TODO(jarcec): What if we're running without reducers?
      RunningJob runningJob = jobClient.getJob(JobID.forName(submissionId));
      if(runningJob == null) {
        // Return default value
        return super.progress(submissionId);
      }

      return (runningJob.mapProgress() + runningJob.reduceProgress()) / 2;
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Counters counters(String submissionId) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(submissionId));
      if(runningJob == null) {
        // Return default value
        return super.counters(submissionId);
      }

      return convertMapreduceCounters(runningJob.getCounters());
    } catch (IOException e) {
      throw new SqoopException(MapreduceSubmissionError.MAPREDUCE_0003, e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String externalLink(String submissionId) {
    try {
      RunningJob runningJob = jobClient.getJob(JobID.forName(submissionId));
      if(runningJob == null) {
        return null;
      }

      return runningJob.getTrackingURL();
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
  protected SubmissionStatus convertMapreduceState(int status) {
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
   * Convert Hadoop counters to Sqoop counters.
   *
   * @param hadoopCounters Hadoop counters
   * @return Appropriate Sqoop counters
   */
  private Counters convertMapreduceCounters(org.apache.hadoop.mapred.Counters hadoopCounters) {
    Counters sqoopCounters = new Counters();

    for(org.apache.hadoop.mapred.Counters.Group hadoopGroup : hadoopCounters) {
      CounterGroup sqoopGroup = new CounterGroup(hadoopGroup.getName());
      for(org.apache.hadoop.mapred.Counters.Counter hadoopCounter : hadoopGroup) {
        Counter sqoopCounter = new Counter(hadoopCounter.getName(), hadoopCounter.getValue());
        sqoopGroup.addCounter(sqoopCounter);
      }
      sqoopCounters.addCounterGroup(sqoopGroup);
    }

    return sqoopCounters;
  }

  /**
   * Detect MapReduce local mode.
   *
   * @return True if we're running in local mode
   */
  private boolean isLocal() {
    return "local".equals(globalConfiguration.get("mapreduce.jobtracker.address"))
        || "local".equals(globalConfiguration.get("mapred.job.tracker"));
  }
}
