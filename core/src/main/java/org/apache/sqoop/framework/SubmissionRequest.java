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

import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.CallbackBase;
import org.apache.sqoop.job.io.Data;
import org.apache.sqoop.job.mr.SqoopFileOutputFormat;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopMapper;
import org.apache.sqoop.model.MSubmission;

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
   * Map-reduce specific options.
   *
   * I'm using strings so that this class won't have direct dependency on
   * hadoop libraries.
   */
  Class inputFormatClass;
  Class mapperClass;
  Class mapOutputKeyClass;
  Class mapOutputValueClass;
  Class outputFormatClass;
  Class outputKeyClass;
  Class outputValueClass;


  public SubmissionRequest(MSubmission submission,
                           SqoopConnector connector,
                           Object configConnectorConnection,
                           Object configConnectorJob,
                           Object configFrameworkConnection,
                           Object configFrameworkJob) {
    this.summary = submission;
    this.connector = connector;
    this.jars = new LinkedList<String>();
    this.connectorContext = new MutableMapContext();
    this.frameworkContext = new MutableMapContext();
    this.configConnectorConnection = configConnectorConnection;
    this.configConnectorJob = configConnectorJob;
    this.configFrameworkConnection = configFrameworkConnection;
    this.configFrameworkJob = configFrameworkJob;

    // TODO(Jarcec): Move this to job execution engine
    this.inputFormatClass = SqoopInputFormat.class;
    this.mapperClass = SqoopMapper.class;
    this.mapOutputKeyClass = Data.class;
    this.mapOutputValueClass = NullWritable.class;
    this.outputFormatClass = SqoopFileOutputFormat.class;
    this.outputKeyClass = Data.class;
    this.outputValueClass = NullWritable.class;
  }

  public MSubmission getSummary() {
    return summary;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public SqoopConnector getConnector() {
    return connector;
  }

  public List<String> getJars() {
    return jars;
  }

  public void addJar(String jar) {
    jars.add(jar);
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

  public Object getConfigConnectorJob() {
    return configConnectorJob;
  }

  public Object getConfigFrameworkConnection() {
    return configFrameworkConnection;
  }

  public Object getConfigFrameworkJob() {
    return configFrameworkJob;
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
  public Class getInputFormatClass() {
    return inputFormatClass;
  }

  public Class getMapperClass() {
    return mapperClass;
  }

  public Class getMapOutputKeyClass() {
    return mapOutputKeyClass;
  }

  public Class getMapOutputValueClass() {
    return mapOutputValueClass;
  }

  public Class getOutputFormatClass() {
    return outputFormatClass;
  }

  public Class getOutputKeyClass() {
    return outputKeyClass;
  }

  public Class getOutputValueClass() {
    return outputValueClass;
  }
}
