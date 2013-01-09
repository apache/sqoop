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
public class MJob extends MAccountableEntity {

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
  private long connectorId;

  /**
   * Corresponding connection object.
   */
  private long connectionId;

  /**
   * User name for this object
   */
  private String name;

  /**
   * Job type
   */
  private Type type;

  private MJobForms connectorPart;
  private MJobForms frameworkPart;

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

    // Check that we're operating on forms with same type
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
}
