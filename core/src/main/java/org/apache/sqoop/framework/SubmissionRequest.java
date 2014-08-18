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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.CallbackBase;
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
   * Connector instances associated with this submission request
   */
  SqoopConnector fromConnector;
  SqoopConnector toConnector;

  /**
   * List of required local jars for the job
   */
  List<String> jars;

  /**
   * From connector callback
   */
  CallbackBase fromCallback;

  /**
   * To connector callback
   */
  CallbackBase toCallback;

  /**
   * All configuration objects
   */
  Object fromConnectorConnectionConfig;
  Object toConnectorConnectionConfig;
  Object fromConnectorJobConfig;
  Object toConnectorJobConfig;
  Object fromFrameworkConnectionConfig;
  Object toFrameworkConnectionConfig;
  Object configFrameworkJob;

  /**
   * Connector context (submission specific configuration)
   */
  MutableMapContext fromConnectorContext;
  MutableMapContext toConnectorContext;

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

  /**
   * The intermediate data format this submission should use.
   */
  Class<? extends IntermediateDataFormat> intermediateDataFormat;

  public SubmissionRequest() {
    this.jars = new LinkedList<String>();
    this.fromConnectorContext = new MutableMapContext();
    this.toConnectorContext = new MutableMapContext();
    this.frameworkContext = new MutableMapContext();
    this.fromConnector = null;
    this.toConnector = null;
    this.fromConnectorConnectionConfig = null;
    this.toConnectorConnectionConfig = null;
    this.fromConnectorJobConfig = null;
    this.toConnectorJobConfig = null;
    this.fromFrameworkConnectionConfig = null;
    this.toFrameworkConnectionConfig = null;
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

  public SqoopConnector getConnector(Direction type) {
    switch(type) {
      case FROM:
        return fromConnector;

      case TO:
        return toConnector;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public void setConnector(Direction type, SqoopConnector connector) {
    switch(type) {
      case FROM:
        fromConnector = connector;

      case TO:
        toConnector = connector;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public List<String> getJars() {
    return jars;
  }

  public void addJar(String jar) {
    if(!jars.contains(jar)) {
      jars.add(jar);
    }
  }

  public void addJarForClass(Class klass) {
    addJar(ClassUtils.jarForClass(klass));
  }

  public void addJars(List<String> jars) {
    for(String j : jars) {
      addJar(j);
    }
  }

  public CallbackBase getFromCallback() {
    return fromCallback;
  }

  public void setFromCallback(CallbackBase fromCallback) {
    this.fromCallback = fromCallback;
  }

  public CallbackBase getToCallback() {
    return toCallback;
  }

  public void setToCallback(CallbackBase toCallback) {
    this.toCallback = toCallback;
  }

  public Object getConnectorConnectionConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorConnectionConfig;

      case TO:
        return toConnectorConnectionConfig;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public void setConnectorConnectionConfig(Direction type, Object config) {
    switch(type) {
      case FROM:
        fromConnectorConnectionConfig = config;

      case TO:
        toConnectorConnectionConfig = config;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public Object getConnectorJobConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorJobConfig;

      case TO:
        return toConnectorJobConfig;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public void setConnectorJobConfig(Direction type, Object config) {
    switch(type) {
      case FROM:
        fromConnectorJobConfig = config;

      case TO:
        toConnectorJobConfig = config;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public Object getFrameworkConnectionConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromFrameworkConnectionConfig;

      case TO:
        return toFrameworkConnectionConfig;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public void setFrameworkConnectionConfig(Direction type, Object config) {
    switch(type) {
      case FROM:
        fromFrameworkConnectionConfig = config;

      case TO:
        toFrameworkConnectionConfig = config;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public Object getConfigFrameworkJob() {
    return configFrameworkJob;
  }

  public void setConfigFrameworkJob(Object config) {
    configFrameworkJob = config;
  }

  public MutableMapContext getConnectorContext(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorContext;

      case TO:
        return toConnectorContext;

      default:
        throw new SqoopException(DirectionError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public MutableMapContext getFrameworkContext() {
    return frameworkContext;
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

  public Class<? extends IntermediateDataFormat> getIntermediateDataFormat() {
    return intermediateDataFormat;
  }

  public void setIntermediateDataFormat(Class<? extends IntermediateDataFormat> intermediateDataFormat) {
    this.intermediateDataFormat = intermediateDataFormat;
  }

}
