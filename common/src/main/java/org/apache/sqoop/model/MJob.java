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

import org.apache.sqoop.common.SqoopException;

/**
 * Model describing entire job object including both connector and
 * framework part.
 */
public class MJob extends MAccountableEntity implements MClonable {

  public static enum Type {
    IMPORT,
    EXPORT,
  }

  /**
   * Connector reference.
   *
   * Job object do not immediately depend on connector as there is indirect
   * dependency through connection object, but having this dependency explicitly
   * carried along helps a lot.
   */
  private final long connectorId;

  /**
   * Corresponding connection object.
   */
  private final long connectionId;

  /**
   * User name for this object
   */
  private String name;

  /**
   * Job type
   */
  private final Type type;

  private final MJobForms connectorPart;
  private final MJobForms frameworkPart;

  /**
   * Default constructor to build  new MJob model.
   *
   * @param connectorId Connector id
   * @param connectionId Connection id
   * @param type Job type
   * @param connectorPart Connector forms
   * @param frameworkPart Framework forms
   */
  public MJob(long connectorId,
              long connectionId,
              Type type,
              MJobForms connectorPart,
              MJobForms frameworkPart) {
    this.connectorId = connectorId;
    this.connectionId = connectionId;
    this.type = type;
    this.connectorPart = connectorPart;
    this.frameworkPart = frameworkPart;
    verifyFormsOfSameType();
  }

  /**
   * Constructor to create deep copy of another MJob model.
   *
   * @param other MConnection model to copy
   */
  public MJob(MJob other) {
    this(other, other.connectorPart.clone(true), other.frameworkPart.clone(true));
  }

  /**
   * Construct new MJob model as a copy of another with replaced forms.
   *
   * This method is suitable only for metadata upgrade path and should not be
   * used otherwise.
   *
   * @param other MJob model to copy
   * @param connectorPart Connector forms
   * @param frameworkPart Framework forms
   */
  public MJob(MJob other, MJobForms connectorPart, MJobForms frameworkPart) {
    super(other);
    this.connectionId = other.connectionId;
    this.connectorId = other.connectorId;
    this.type = other.type;
    this.name = other.name;
    this.connectorPart = connectorPart;
    this.frameworkPart = frameworkPart;
    verifyFormsOfSameType();
  }

  private void verifyFormsOfSameType() {
    if (type != connectorPart.getType() || type != frameworkPart.getType()) {
      throw new SqoopException(ModelError.MODEL_002,
        "Incompatible types, job: " + type.name()
          + ", connector part: " + connectorPart.getType().name()
          + ", framework part: " + frameworkPart.getType().name()
      );
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("job connector-part: ");
    sb.append(connectorPart).append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public long getConnectorId() {
    return connectorId;
  }

  public MJobForms getConnectorPart() {
    return connectorPart;
  }

  public MJobForms getFrameworkPart() {
    return frameworkPart;
  }

  public Type getType() {
    return type;
  }

  @Override
  public MJob clone(boolean cloneWithValue) {
    if(cloneWithValue) {
      return new MJob(this);
    } else {
      return new MJob(connectorId, connectionId, type, connectorPart.clone(false), frameworkPart.clone(false));
    }
  }

  @Override
  public boolean equals(Object object) {
    if(object == this) {
      return true;
    }

    if(!(object instanceof MJob)) {
      return false;
    }

    MJob job = (MJob)object;
    return (job.connectorId == this.connectorId)
        && (job.connectionId == this.connectionId)
        && (job.getPersistenceId() == this.getPersistenceId())
        && (job.type.equals(this.type))
        && (job.connectorPart.equals(this.connectorPart))
        && (job.frameworkPart.equals(this.frameworkPart));
  }
}
