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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.Transferable;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.utils.ClassUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Submission details class is used when creating new submission and contains
 * all information that we need to create a new submission (including mappers,
 * reducers, ...).
 */
public class JobRequest {

  /**
   * Job Submission
   */
  MSubmission jobSubmission;

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
   * Set of required local jars for the job
   */
  Set<String> jars;

  /**
   * From entity
   */
  Transferable from;

  /**
   * To entity
   */
  Transferable to;

  /**
   * All configuration objects
   */
  Object fromConnectorLinkConfig;
  Object toConnectorLinkConfig;

  Object fromConfig;
  Object toConfig;

  Object driverConfig;

  /**
   * Connector context (submission specific configuration)
   */
  MutableMapContext fromConnectorContext;
  MutableMapContext toConnectorContext;

  /**
   * Driver context (submission specific configuration)
   */
  MutableMapContext driverContext;

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
   * The intermediate data format this submission should use to read/extract.
   */
  Class<? extends IntermediateDataFormat<?>> fromIDF;

  /**
   * The intermediate data format this submission should use to write/load.
   */
  Class<? extends IntermediateDataFormat<?>> toIDF;

  public JobRequest() {
    this.jars = new HashSet<String>();
    this.fromConnectorContext = new MutableMapContext();
    this.toConnectorContext = new MutableMapContext();
    this.driverContext = new MutableMapContext();
    this.fromConnector = null;
    this.toConnector = null;
    this.fromConnectorLinkConfig = null;
    this.toConnectorLinkConfig = null;
    this.fromConfig = null;
    this.toConfig = null;
    this.driverConfig = null;
  }

  public MSubmission getJobSubmission() {
    return jobSubmission;
  }

  public void setJobSubmission(MSubmission submission) {
    this.jobSubmission = submission;
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
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public void setConnector(Direction type, SqoopConnector connector) {
    switch(type) {
      case FROM:
        fromConnector = connector;
        break;

      case TO:
        toConnector = connector;
        break;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public Set<String> getJars() {
    return jars;
  }

  public void addJar(String jar) {
    if(!jars.contains(jar)) {
      jars.add(jar);
    }
  }

  public void addJarForClass(Class<?> klass) {
    addJar(ClassUtils.jarForClass(klass));
  }

  public void addJars(Set<String> jars) {
    for(String j : jars) {
      addJar(j);
    }
  }

  public Transferable getFrom() {
    return from;
  }

  public void setFrom(Transferable from) {
    this.from = from;
  }

  public Transferable getTo() {
    return to;
  }

  public void setTo(Transferable to) {
    this.to = to;
  }

  public Object getConnectorLinkConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorLinkConfig;

      case TO:
        return toConnectorLinkConfig;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public void setConnectorLinkConfig(Direction type, Object config) {
    switch(type) {
      case FROM:
        fromConnectorLinkConfig = config;
        break;
      case TO:
        toConnectorLinkConfig = config;
        break;
      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public Object getJobConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromConfig;

      case TO:
        return toConfig;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public void setJobConfig(Direction type, Object config) {
    switch(type) {
      case FROM:
        fromConfig = config;
        break;
      case TO:
        toConfig = config;
        break;
      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public Object getDriverConfig() {
    return driverConfig;
  }

  public void setDriverConfig(Object config) {
    driverConfig = config;
  }

  public MutableMapContext getConnectorContext(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorContext;

      case TO:
        return toConnectorContext;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public MutableMapContext getDriverContext() {
    return driverContext;
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

  public Class<? extends IntermediateDataFormat<?>> getIntermediateDataFormat(Direction direction) {
    return direction.equals(Direction.FROM) ? fromIDF : toIDF;
  }

  public void setIntermediateDataFormat(Class<? extends IntermediateDataFormat<? extends Object>> intermediateDataFormat, Direction direction) {
    if (direction.equals(Direction.FROM)) {
      fromIDF = intermediateDataFormat;
    } else if (direction.equals(Direction.TO)) {
      toIDF = intermediateDataFormat;
    }
  }

}