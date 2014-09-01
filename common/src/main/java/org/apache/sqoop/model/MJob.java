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

/**
 * Model describing entire job object including both connector and
 * framework part.
 */
public class MJob extends MAccountableEntity implements MClonable {
  /**
   * Connector reference.
   *
   * Job object do not immediately depend on connector as there is indirect
   * dependency through connection object, but having this dependency explicitly
   * carried along helps a lot.
   */
  private final long fromConnectorId;
  private final long toConnectorId;

  /**
   * Corresponding connection objects for connector.
   */
  private final long fromConnectionId;
  private final long toConnectionId;

  private final MJobForms fromConnectorPart;
  private final MJobForms toConnectorPart;
  private final MJobForms frameworkPart;

  /**
   * Default constructor to build  new MJob model.
   *
   * @param fromConnectorId FROM Connector id
   * @param toConnectorId TO Connector id
   * @param fromConnectionId FROM Connection id
   * @param toConnectionId TO Connection id
   * @param fromPart FROM Connector forms
   * @param toPart TO Connector forms
   * @param frameworkPart Framework forms
   */
  public MJob(long fromConnectorId,
              long toConnectorId,
              long fromConnectionId,
              long toConnectionId,
              MJobForms fromPart,
              MJobForms toPart,
              MJobForms frameworkPart) {
    this.fromConnectorId = fromConnectorId;
    this.toConnectorId = toConnectorId;
    this.fromConnectionId = fromConnectionId;
    this.toConnectionId = toConnectionId;
    this.fromConnectorPart = fromPart;
    this.toConnectorPart = toPart;
    this.frameworkPart = frameworkPart;
  }

  /**
   * Constructor to create deep copy of another MJob model.
   *
   * @param other MConnection model to copy
   */
  public MJob(MJob other) {
    this(other,
        other.getConnectorPart(Direction.FROM).clone(true),
        other.getConnectorPart(Direction.TO).clone(true),
        other.frameworkPart.clone(true));
  }

  /**
   * Construct new MJob model as a copy of another with replaced forms.
   *
   * This method is suitable only for metadata upgrade path and should not be
   * used otherwise.
   *
   * @param other MJob model to copy
   * @param fromPart FROM Connector forms
   * @param toPart TO Connector forms
   * @param frameworkPart Framework forms
   */
  public MJob(MJob other, MJobForms fromPart, MJobForms toPart, MJobForms frameworkPart) {
    super(other);

    this.fromConnectorId = other.getConnectorId(Direction.FROM);
    this.toConnectorId = other.getConnectorId(Direction.TO);
    this.fromConnectionId = other.getConnectionId(Direction.FROM);
    this.toConnectionId = other.getConnectionId(Direction.TO);
    this.fromConnectorPart = fromPart;
    this.toConnectorPart = toPart;
    this.frameworkPart = frameworkPart;
    this.setPersistenceId(other.getPersistenceId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("job");
    sb.append(" connector-from-part: ").append(getConnectorPart(Direction.FROM));
    sb.append(", connector-to-part: ").append(getConnectorPart(Direction.TO));
    sb.append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }

  public long getConnectionId(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectionId;

      case TO:
        return toConnectionId;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public long getConnectorId(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorId;

      case TO:
        return toConnectorId;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public MJobForms getConnectorPart(Direction type) {
    switch(type) {
      case FROM:
        return fromConnectorPart;

      case TO:
        return toConnectorPart;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public MJobForms getFrameworkPart() {
    return frameworkPart;
  }

  @Override
  public MJob clone(boolean cloneWithValue) {
    if(cloneWithValue) {
      return new MJob(this);
    } else {
      return new MJob(
          getConnectorId(Direction.FROM),
          getConnectorId(Direction.TO),
          getConnectionId(Direction.FROM),
          getConnectionId(Direction.TO),
          getConnectorPart(Direction.FROM).clone(false),
          getConnectorPart(Direction.TO).clone(false),
          frameworkPart.clone(false));
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
    return (job.getConnectorId(Direction.FROM) == this.getConnectorId(Direction.FROM))
        && (job.getConnectorId(Direction.TO) == this.getConnectorId(Direction.TO))
        && (job.getConnectionId(Direction.FROM) == this.getConnectionId(Direction.FROM))
        && (job.getConnectionId(Direction.TO) == this.getConnectionId(Direction.TO))
        && (job.getPersistenceId() == this.getPersistenceId())
        && (job.getConnectorPart(Direction.FROM).equals(this.getConnectorPart(Direction.FROM)))
        && (job.getConnectorPart(Direction.TO).equals(this.getConnectorPart(Direction.TO)))
        && (job.frameworkPart.equals(this.frameworkPart));
  }
}
