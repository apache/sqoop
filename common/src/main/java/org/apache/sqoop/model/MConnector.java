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
package org.apache.sqoop.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.sqoop.common.ConnectorType;

/**
 * Connector metadata.
 *
 * Includes unique id that identifies connector in metadata store, unique human
 * readable name, corresponding name and all forms for all supported job types.
 */
public final class MConnector extends MPersistableEntity implements MClonable {

  private final String uniqueName;
  private final String className;
  private final MConnectionForms connectionForms;
  private final Map<ConnectorType, MJobForms> jobForms;
  String version;

  public MConnector(String uniqueName, String className,
                    String version, MConnectionForms connectionForms,
                    MJobForms fromJobForms, MJobForms toJobForms) {
    this.jobForms = new HashMap<ConnectorType, MJobForms>();

    this.version = version;
    this.connectionForms = connectionForms;
    this.jobForms.put(ConnectorType.FROM, fromJobForms);
    this.jobForms.put(ConnectorType.TO, toJobForms);

    if (uniqueName == null || className == null) {
      throw new NullPointerException();
    }

    this.uniqueName = uniqueName;
    this.className = className;
  }

  public String getUniqueName() {
    return uniqueName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connector-");
    sb.append(uniqueName).append(":").append(getPersistenceId()).append(":");
    sb.append(className);
    sb.append(", ").append(getConnectionForms().toString());
    sb.append(", ").append(getJobForms(ConnectorType.FROM).toString());
    sb.append(", ").append(getJobForms(ConnectorType.TO).toString());
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MConnector)) {
      return false;
    }

    MConnector mc = (MConnector) other;
    return uniqueName.equals(mc.uniqueName)
        && className.equals(mc.className)
        && version.equals(mc.version)
        && connectionForms.equals(mc.getConnectionForms())
        && jobForms.get(ConnectorType.FROM).equals(mc.getJobForms(ConnectorType.FROM))
        && jobForms.get(ConnectorType.TO).equals(mc.getJobForms(ConnectorType.TO));
  }

  @Override
  public int hashCode() {
    int result = getConnectionForms().hashCode();
    result = 31 * result + getJobForms(ConnectorType.FROM).hashCode();
    result = 31 * result + getJobForms(ConnectorType.TO).hashCode();
    result = 31 * result + version.hashCode();
    result = 31 * result + uniqueName.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  public MConnector clone(boolean cloneWithValue) {
    //Connector never have any values filled
    cloneWithValue = false;
    MConnector copy = new MConnector(
        this.getUniqueName(),
        this.getClassName(),
        this.getVersion(),
        this.getConnectionForms().clone(cloneWithValue),
        this.getJobForms(ConnectorType.FROM).clone(cloneWithValue),
        this.getJobForms(ConnectorType.TO).clone(cloneWithValue));
    copy.setPersistenceId(this.getPersistenceId());
    return copy;
  }

  public MConnectionForms getConnectionForms() {
    return connectionForms;
  }

  public MJobForms getJobForms(ConnectorType type) {
    return jobForms.get(type);
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
