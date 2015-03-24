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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.DirectionError;
import org.apache.sqoop.common.SqoopException;

/**
 * Model describing entire job object including the from/to and driver config information
 * to execute the job
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MJob extends MAccountableEntity implements MClonable {
  /**
   * NOTE :  Job object do not immediately depend on connector as there is indirect
   * dependency through link object, but having this dependency explicitly
   * carried along helps with not having to make the DB call everytime
   */
  private final long fromConnectorId;
  private final long toConnectorId;
  private final long fromLinkId;
  private final long toLinkId;

  private final MFromConfig fromConfig;
  private final MToConfig toConfig;
  private final MDriverConfig driverConfig;

  /**
   * Default constructor to build  new MJob model.
   *
   * @param fromConnectorId FROM Connector id
   * @param toConnectorId TO Connector id
   * @param fromLinkId FROM Link id
   * @param toLinkId TO Link id
   * @param fromConfig FROM job config
   * @param toConfig TO job config
   * @param driverConfig driver config
   */
  public MJob(long fromConnectorId,
              long toConnectorId,
              long fromLinkId,
              long toLinkId,
              MFromConfig fromConfig,
              MToConfig toConfig,
              MDriverConfig driverConfig) {
    this.fromConnectorId = fromConnectorId;
    this.toConnectorId = toConnectorId;
    this.fromLinkId = fromLinkId;
    this.toLinkId = toLinkId;
    this.fromConfig = fromConfig;
    this.toConfig = toConfig;
    this.driverConfig = driverConfig;
  }

  /**
   * Constructor to create deep copy of another MJob model.
   *
   * @param other MConnection model to copy
   */
  public MJob(MJob other) {
    this(other,
        other.getFromJobConfig().clone(true),
        other.getToJobConfig().clone(true),
        other.driverConfig.clone(true));
  }

  /**
   * Construct new MJob model as a copy of another with replaced forms.
   *
   * This method is suitable only for metadata upgrade path and should not be
   * used otherwise.
   *
   * @param other MJob model to copy
   * @param fromConfig FROM Job config
   * @param toConfig TO Job config
   * @param driverConfig driverConfig
   */
  public MJob(MJob other, MFromConfig fromConfig, MToConfig toConfig, MDriverConfig driverConfig) {
    super(other);

    this.fromConnectorId = other.getConnectorId(Direction.FROM);
    this.toConnectorId = other.getConnectorId(Direction.TO);
    this.fromLinkId = other.getLinkId(Direction.FROM);
    this.toLinkId = other.getLinkId(Direction.TO);
    this.fromConfig = fromConfig;
    this.toConfig = toConfig;
    this.driverConfig = driverConfig;
    this.setPersistenceId(other.getPersistenceId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("job");
    sb.append("From job config: ").append(getJobConfig(Direction.FROM));
    sb.append(", To job config: ").append(getJobConfig(Direction.TO));
    sb.append(", Driver config: ").append(driverConfig);

    return sb.toString();
  }

  public long getLinkId(Direction type) {
    switch(type) {
      case FROM:
        return fromLinkId;

      case TO:
        return toLinkId;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public long getFromLinkId() {
    return fromLinkId;
  }

  public long getToLinkId() {
    return toLinkId;
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

  public long getFromConnectorId() {
    return fromConnectorId;
  }

  public long getToConnectorId() {
    return toConnectorId;
  }

  public MConfigList getJobConfig(Direction type) {
    switch(type) {
      case FROM:
        return fromConfig;

      case TO:
        return toConfig;

      default:
        throw new SqoopException(DirectionError.DIRECTION_0000, "Direction: " + type);
    }
  }

  public MFromConfig getFromJobConfig() {
    return fromConfig;
  }

  public MToConfig getToJobConfig() {
    return toConfig;
  }

  public MDriverConfig getDriverConfig() {
    return driverConfig;
  }

  @Override
  public MJob clone(boolean cloneWithValue) {
    if(cloneWithValue) {
      return new MJob(this);
    } else {
      return new MJob(
          getConnectorId(Direction.FROM),
          getConnectorId(Direction.TO),
          getLinkId(Direction.FROM),
          getLinkId(Direction.TO),
          getFromJobConfig().clone(false),
          getToJobConfig().clone(false),
          getDriverConfig().clone(false));
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
        && (job.getLinkId(Direction.FROM) == this.getLinkId(Direction.FROM))
        && (job.getLinkId(Direction.TO) == this.getLinkId(Direction.TO))
        && (job.getPersistenceId() == this.getPersistenceId())
        && (job.getFromJobConfig().equals(this.getJobConfig(Direction.FROM)))
        && (job.getToJobConfig().equals(this.getJobConfig(Direction.TO)))
        && (job.getDriverConfig().equals(this.driverConfig));
  }
}
