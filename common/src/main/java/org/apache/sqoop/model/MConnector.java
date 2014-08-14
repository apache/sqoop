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

import org.apache.sqoop.common.ConnectorType;
import org.apache.sqoop.common.ConnectorTypeError;
import org.apache.sqoop.common.SqoopException;

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
  private final MJobForms fromJobForms;
  private final MJobForms toJobForms;
  String version;

  public MConnector(String uniqueName, String className,
                    String version, MConnectionForms connectionForms,
                    MJobForms fromJobForms, MJobForms toJobForms) {
    this.version = version;
    this.connectionForms = connectionForms;
    this.fromJobForms = fromJobForms;
    this.toJobForms = toJobForms;

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
        && fromJobForms.equals(mc.getJobForms(ConnectorType.FROM))
        && toJobForms.equals(mc.getJobForms(ConnectorType.TO));
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
    switch(type) {
      case FROM:
        return fromJobForms;

      case TO:
        return toJobForms;

      default:
        throw new SqoopException(ConnectorTypeError.CONNECTOR_TYPE_0000, "Connector type: " + type);
    }
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
