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

import java.util.HashMap;
import java.util.Map;

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
  private final Map<ConnectorType, Long> connectorIds;

  /**
   * Corresponding connection objects for connector.
   */
  private final Map<ConnectorType, Long> connectionIds;

  /**
   * User name for this object
   */
  private String name;

  private final Map<ConnectorType, MJobForms> connectorParts;
  private final MJobForms frameworkPart;

  /**
   * Default constructor to build  new MJob model.
   *
   * @param fromConnectorId Connector id
   * @param fromConnectionId Connection id
   * @param fromPart From Connector forms
   * @param toPart To Connector forms
   * @param frameworkPart Framework forms
   */
  public MJob(long fromConnectorId,
              long toConnectorId,
              long fromConnectionId,
              long toConnectionId,
              MJobForms fromPart,
              MJobForms toPart,
              MJobForms frameworkPart) {
    connectorIds = new HashMap<ConnectorType, Long>();
    connectorIds.put(ConnectorType.FROM, fromConnectorId);
    connectorIds.put(ConnectorType.TO, toConnectorId);
    connectionIds = new HashMap<ConnectorType, Long>();
    connectionIds.put(ConnectorType.FROM, fromConnectionId);
    connectionIds.put(ConnectorType.TO, toConnectionId);
    connectorParts = new HashMap<ConnectorType, MJobForms>();
    connectorParts.put(ConnectorType.FROM, fromPart);
    connectorParts.put(ConnectorType.TO, toPart);
    this.frameworkPart = frameworkPart;
  }

  /**
   * Constructor to create deep copy of another MJob model.
   *
   * @param other MConnection model to copy
   */
  public MJob(MJob other) {
    this(other,
        other.getConnectorPart(ConnectorType.FROM).clone(true),
        other.getConnectorPart(ConnectorType.TO).clone(true),
        other.frameworkPart.clone(true));
  }

  /**
   * Construct new MJob model as a copy of another with replaced forms.
   *
   * This method is suitable only for metadata upgrade path and should not be
   * used otherwise.
   *
   * @param other MJob model to copy
   * @param fromPart From Connector forms
   * @param frameworkPart Framework forms
   * @param toPart To Connector forms
   */
  public MJob(MJob other, MJobForms fromPart, MJobForms frameworkPart, MJobForms toPart) {
    super(other);
    connectorIds = new HashMap<ConnectorType, Long>();
    connectorIds.put(ConnectorType.FROM, other.getConnectorId(ConnectorType.FROM));
    connectorIds.put(ConnectorType.TO, other.getConnectorId(ConnectorType.TO));
    connectionIds = new HashMap<ConnectorType, Long>();
    connectorIds.put(ConnectorType.FROM, other.getConnectionId(ConnectorType.FROM));
    connectorIds.put(ConnectorType.TO, other.getConnectionId(ConnectorType.TO));
    connectorParts = new HashMap<ConnectorType, MJobForms>();
    connectorParts.put(ConnectorType.FROM, fromPart);
    connectorParts.put(ConnectorType.TO, toPart);
    this.name = other.name;
    this.frameworkPart = frameworkPart;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("job");
    sb.append(" connector-from-part: ").append(getConnectorPart(ConnectorType.FROM));
    sb.append(", connector-to-part: ").append(getConnectorPart(ConnectorType.TO));
    sb.append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getConnectionId(ConnectorType type) {
    return connectionIds.get(type);
  }

  public long getConnectorId(ConnectorType type) {
    return connectorIds.get(type);
  }

  public MJobForms getConnectorPart(ConnectorType type) {
    return connectorParts.get(type);
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
          getConnectorId(ConnectorType.FROM),
          getConnectorId(ConnectorType.TO),
          getConnectionId(ConnectorType.FROM),
          getConnectionId(ConnectorType.TO),
          getConnectorPart(ConnectorType.FROM).clone(false),
          getConnectorPart(ConnectorType.TO).clone(false),
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
    return (job.getConnectorId(ConnectorType.FROM) == this.getConnectorId(ConnectorType.FROM))
        && (job.getConnectorId(ConnectorType.TO) == this.getConnectorId(ConnectorType.TO))
        && (job.getConnectionId(ConnectorType.FROM) == this.getConnectionId(ConnectorType.FROM))
        && (job.getConnectionId(ConnectorType.TO) == this.getConnectionId(ConnectorType.TO))
        && (job.getPersistenceId() == this.getPersistenceId())
        && (job.getConnectorPart(ConnectorType.FROM).equals(this.getConnectorPart(ConnectorType.FROM)))
        && (job.getConnectorPart(ConnectorType.TO).equals(this.getConnectorPart(ConnectorType.TO)))
        && (job.frameworkPart.equals(this.frameworkPart));
  }
}
