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

/**
 * Model describing entire connection object including both connector and
 * framework part.
 */
public class MConnection extends MAccountableEntity implements MClonable {
  private long connectorId;

  private final MConnectionForms connectorPart;
  private final MConnectionForms frameworkPart;

  /**
   * Default constructor to build new MConnection model.
   *
   * @param connectorId Connector id
   * @param connectorPart Connector forms
   * @param frameworkPart Framework forms
   */
  public MConnection(long connectorId,
                     MConnectionForms connectorPart,
                     MConnectionForms frameworkPart) {
    this.connectorId = connectorId;
    this.connectorPart = connectorPart;
    this.frameworkPart = frameworkPart;
  }

  /**
   * Constructor to create deep copy of another MConnection model.
   *
   * @param other MConnection model to copy
   */
  public MConnection(MConnection other) {
    this(other, other.connectorPart.clone(true), other.frameworkPart.clone(true));
  }

  /**
   * Construct new MConnection model as a copy of another with replaced forms.
   *
   * This method is suitable only for metadata upgrade path and should not be
   * used otherwise.
   *
   * @param other MConnection model to copy
   * @param connectorPart Connector forms
   * @param frameworkPart Framework forms
   */
  public MConnection(MConnection other, MConnectionForms connectorPart, MConnectionForms frameworkPart) {
    super(other);
    this.connectorId = other.connectorId;
    this.connectorPart = connectorPart;
    this.frameworkPart = frameworkPart;
    this.setPersistenceId(other.getPersistenceId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connection: ").append(getName());
    sb.append(" connector-part: ").append(connectorPart);
    sb.append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }

  public long getConnectorId() {
    return connectorId;
  }

  public void setConnectorId(long connectorId) {
    this.connectorId = connectorId;
  }

  public MConnectionForms getConnectorPart() {
    return connectorPart;
  }

  public MConnectionForms getFrameworkPart() {
    return frameworkPart;
  }

  public MForm getConnectorForm(String formName) {
    return connectorPart.getForm(formName);
  }

  public MForm getFrameworkForm(String formName) {
    return frameworkPart.getForm(formName);
  }

  @Override
  public MConnection clone(boolean cloneWithValue) {
    if(cloneWithValue) {
      return new MConnection(this);
    } else {
      return new MConnection(connectorId, connectorPart.clone(false), frameworkPart.clone(false));
    }
  }

  @Override
  public boolean equals(Object object) {
    if(object == this) {
      return true;
    }

    if(!(object instanceof MConnection)) {
      return false;
    }

    MConnection mc = (MConnection)object;
    return (mc.connectorId == this.connectorId)
        && (mc.getPersistenceId() == this.getPersistenceId())
        && (mc.connectorPart.equals(this.connectorPart))
        && (mc.frameworkPart.equals(this.frameworkPart));
  }
}
