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

import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.CallbackBase;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.utils.ClassUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Submission details class is used when creating new submission and contains
 * all information that we need to create a new submission (including mappers,
 * reducers, ...).
 */
public class SubmissionRequest {

  /**
   * Submission summary
   */
  MSubmission summary;

  /**
   * Original job name
   */
  String jobName;

  /**
   * Associated job (from metadata perspective) id
   */
  long jobId;

  /**
   * Job type
   */
  MJob.Type jobType;

  /**
   * Connector instance associated with this submission request
   */
  SqoopConnector connector;

  /**
   * List of required local jars for the job
   */
  List<String> jars;

  /**
   * Base callbacks that are independent on job type
   */
  CallbackBase connectorCallbacks;

  /**
   * All 4 configuration objects
   */
  Object configConnectorConnection;
  Object configConnectorJob;
  Object configFrameworkConnection;
  Object configFrameworkJob;

  /**
   * Connector context (submission specific configuration)
   */
  MutableMapContext connectorContext;

  /**
   * Framework context (submission specific configuration)
   */
  MutableMapContext frameworkContext;

  /**
   * HDFS output directory
   */
  String outputDirectory;

  /**
   * Optional notification URL for job progress
   */
  String notificationUrl;

  /**
   * Number of extractors
   */
  Integer extractors;

  /**
   * Number of loaders
   */
  Integer loaders;

  public SubmissionRequest() {
    this.jars = new LinkedList<String>();
    this.connectorContext = new MutableMapContext();
    this.frameworkContext = new MutableMapContext();
  }

  public MSubmission getSummary() {
    return summary;
  }

  public void setSummary(MSubmission summary) {
    this.summary = summary;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public MJob.Type getJobType() {
    return jobType;
  }

  public void setJobType(MJob.Type jobType) {
    this.jobType = jobType;
  }

  public SqoopConnector getConnector() {
    return connector;
  }

  public void setConnector(SqoopConnector connector) {
    this.connector = connector;
  }

  public List<String> getJars() {
    return jars;
  }

  public void addJar(String jar) {
    jars.add(jar);
  }

  public void addJarForClass(Class klass) {
    jars.add(ClassUtils.jarForClass(klass));
  }

  public void addJars(List<String> jars) {
    this.jars.addAll(jars);
  }

  public CallbackBase getConnectorCallbacks() {
    return connectorCallbacks;
  }

  public void setConnectorCallbacks(CallbackBase connectorCallbacks) {
    this.connectorCallbacks = connectorCallbacks;
  }

  public Object getConfigConnectorConnection() {
    return configConnectorConnection;
  }

  public void setConfigConnectorConnection(Object config) {
    configConnectorConnection = config;
  }

  public Object getConfigConnectorJob() {
    return configConnectorJob;
  }

  public void setConfigConnectorJob(Object config) {
    configConnectorJob = config;
  }

  public Object getConfigFrameworkConnection() {
    return configFrameworkConnection;
  }

  public void setConfigFrameworkConnection(Object config) {
    configFrameworkConnection = config;
  }

  public Object getConfigFrameworkJob() {
    return configFrameworkJob;
  }

  public void setConfigFrameworkJob(Object config) {
    configFrameworkJob = config;
  }

  public MutableMapContext getConnectorContext() {
    return connectorContext;
  }

  public MutableMapContext getFrameworkContext() {
    return frameworkContext;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

  public String getNotificationUrl() {
    return notificationUrl;
  }

  public void setNotificationUrl(String url) {
    this.notificationUrl = url;
  }

  public Integer getExtractors() {
    return extractors;
  }

  public void setExtractors(Integer extractors) {
    this.extractors = extractors;
  }

  public Integer getLoaders() {
    return loaders;
  }

  public void setLoaders(Integer loaders) {
    this.loaders = loaders;
  }
}
