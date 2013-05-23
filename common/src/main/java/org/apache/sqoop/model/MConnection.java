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
  private String name;

  private MConnectionForms connectorPart;
  private MConnectionForms frameworkPart;

  public MConnection(long connectorId,
                     MConnectionForms connectorPart,
                     MConnectionForms frameworkPart) {
    this.connectorId = connectorId;
    this.connectorPart = connectorPart;
    this.frameworkPart = frameworkPart;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("connection: ").append(name);
    sb.append(" connector-part: ").append(connectorPart);
    sb.append(", framework-part: ").append(frameworkPart);

    return sb.toString();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getConnectorId() {
    return connectorId;
  }

  public void setConnectorPart(MConnectionForms connectorPart) {
    this.connectorPart = connectorPart;
  }

  public void setFrameworkPart(MConnectionForms frameworkPart) {
    this.frameworkPart = frameworkPart;
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
    MConnection copy = new MConnection(this.getConnectorId(),
        this.getConnectorPart().clone(cloneWithValue),
        this.getFrameworkPart().clone(cloneWithValue));
    if(cloneWithValue) {
      copy.setPersistenceId(this.getPersistenceId());
      copy.setName(this.getName());
    }
    return copy;
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
