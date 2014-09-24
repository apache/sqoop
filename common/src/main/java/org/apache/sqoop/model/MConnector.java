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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.common.SupportedDirections;

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
    MJobForms fromJobForms = this.getJobForms(Direction.FROM);
    MJobForms toJobForms = this.getJobForms(Direction.TO);
    StringBuilder sb = new StringBuilder("connector-");
    sb.append(uniqueName).append(":").append(getPersistenceId()).append(":");
    sb.append(className);
    sb.append(", ").append(getConnectionForms().toString());
    if (fromJobForms != null) {
      sb.append(", ").append(fromJobForms.toString());
    }
    if (toJobForms != null) {
      sb.append(", ").append(toJobForms.toString());
    }
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
    SupportedDirections supportedDirections = this.getSupportedDirections();
    SupportedDirections mcSupportedDirections = mc.getSupportedDirections();

    if (supportedDirections.isDirectionSupported(Direction.FROM)
        && mcSupportedDirections.isDirectionSupported(Direction.FROM)
        && !getJobForms(Direction.FROM).equals(mc.getJobForms(Direction.FROM))) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.FROM)
        != mcSupportedDirections.isDirectionSupported(Direction.FROM)) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.TO)
        && mcSupportedDirections.isDirectionSupported(Direction.TO)
        && !getJobForms(Direction.TO).equals(mc.getJobForms(Direction.TO))) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.TO)
        != mcSupportedDirections.isDirectionSupported(Direction.TO)) {
      return false;
    }

    return uniqueName.equals(mc.uniqueName)
        && className.equals(mc.className)
        && version.equals(mc.version)
        && connectionForms.equals(mc.getConnectionForms());
  }

  @Override
  public int hashCode() {
    SupportedDirections supportedDirections = getSupportedDirections();
    int result = getConnectionForms().hashCode();
    if (supportedDirections.isDirectionSupported(Direction.FROM)) {
      result = 31 * result + getJobForms(Direction.FROM).hashCode();
    }
    if (supportedDirections.isDirectionSupported(Direction.TO)) {
      result = 31 * result + getJobForms(Direction.TO).hashCode();
    }
    result = 31 * result + version.hashCode();
    result = 31 * result + uniqueName.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  public MConnector clone(boolean cloneWithValue) {
    //Connector never have any values filled
    cloneWithValue = false;

    MJobForms fromJobForms = this.getJobForms(Direction.FROM);
    MJobForms toJobForms = this.getJobForms(Direction.TO);

    if (fromJobForms != null) {
      fromJobForms = fromJobForms.clone(cloneWithValue);
    }

    if (toJobForms != null) {
      toJobForms = toJobForms.clone(cloneWithValue);
    }

    MConnector copy = new MConnector(
        this.getUniqueName(),
        this.getClassName(),
        this.getVersion(),
        this.getConnectionForms().clone(cloneWithValue),
        fromJobForms,
        toJobForms);
    copy.setPersistenceId(this.getPersistenceId());
    return copy;
  }

  public MConnectionForms getConnectionForms() {
    return connectionForms;
  }

  public MJobForms getJobForms(Direction type) {
    switch(type) {
      case FROM:
        return fromJobForms;

      case TO:
        return toJobForms;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public SupportedDirections getSupportedDirections() {
    return new SupportedDirections(this.getJobForms(Direction.FROM) != null,
        this.getJobForms(Direction.TO) != null);
  }
}
