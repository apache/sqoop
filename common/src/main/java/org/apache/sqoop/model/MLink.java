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

/**
 * Model describing the link object and its corresponding configs
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MLink extends MAccountableEntity implements MClonable {
  private long connectorId;
  // NOTE: we hold this in the model for easy access to the link config object, it might as well be retrieved on the fly using the connectorId
  private final MLinkConfig connectorLinkConfig;

  /**
   * Default constructor to build new MLink model.
   *
   * @param connectorId Connector id
   * @param linkConfig Connector forms
   */
  public MLink(long connectorId, MLinkConfig linkConfig) {
    this.connectorId = connectorId;
    this.connectorLinkConfig = linkConfig;
  }

  /**
   * Constructor to create deep copy of another MLink model.
   *
   * @param other MLink model to copy
   */
  public MLink(MLink other) {
    this(other, other.connectorLinkConfig.clone(true));
  }

  /**
   * Construct new MLink model as a copy of another with replaced forms.
   *
   * This method is suitable only for link upgrade path and should not be
   * used otherwise.
   *
   * @param other MLink model to copy
   * @param linkConfig link config
   */
  public MLink(MLink other, MLinkConfig linkConfig) {
    super(other);
    this.connectorId = other.connectorId;
    this.connectorLinkConfig = linkConfig;
    this.setPersistenceId(other.getPersistenceId());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("link: ").append(getName());
    sb.append(" link-config: ").append(connectorLinkConfig);

    return sb.toString();
  }

  public long getConnectorId() {
    return connectorId;
  }

  public MLinkConfig getConnectorLinkConfig() {
    return connectorLinkConfig;
  }
  public MConfig getConnectorLinkConfig(String configName) {
    return connectorLinkConfig.getConfig(configName);
  }

  @Override
  public MLink clone(boolean cloneWithValue) {
    if(cloneWithValue) {
      return new MLink(this);
    } else {
      return new MLink(connectorId, connectorLinkConfig.clone(false));
    }
  }

  @Override
  public boolean equals(Object object) {
    if(object == this) {
      return true;
    }

    if(!(object instanceof MLink)) {
      return false;
    }

    MLink mLink = (MLink)object;
    return (mLink.connectorId == this.connectorId)
        && (mLink.getPersistenceId() == this.getPersistenceId())
        && (mLink.connectorLinkConfig.equals(this.connectorLinkConfig));
    }
}