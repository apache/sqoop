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
import org.apache.sqoop.common.SupportedDirections;

/**
 * Connector entity supports the FROM/TO {@link  org.apache.sqoop.job.etl.Transfereable} Includes unique id
 * that identifies connector in the repository, unique human readable name,
 * corresponding name and all configs to support the from and to data sources
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class MConnector extends Configurable {

  private final String uniqueName;
  private final String className;
  private final String version;
  private final MLinkConfig linkConfig;
  private final MFromConfig fromConfig;
  private final MToConfig toConfig;

  public MConnector(String uniqueName, String className, String version, MLinkConfig linkConfig,
      MFromConfig fromConfig, MToConfig toConfig) {
    this.version = version;
    this.linkConfig = linkConfig;
    this.fromConfig = fromConfig;
    this.toConfig = toConfig;

    // Why are we abusing NPE?
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
    sb.append(", ").append(getLinkConfig().toString());
    if (getFromConfig() != null) {
      sb.append(", ").append(getFromConfig().toString());
    }
    if (getToConfig() != null) {
      sb.append(", ").append(getToConfig().toString());
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
        && !getFromConfig().equals(mc.getFromConfig())) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.FROM) != mcSupportedDirections
        .isDirectionSupported(Direction.FROM)) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.TO)
        && mcSupportedDirections.isDirectionSupported(Direction.TO)
        && !getToConfig().equals(mc.getToConfig())) {
      return false;
    }

    if (supportedDirections.isDirectionSupported(Direction.TO) != mcSupportedDirections
        .isDirectionSupported(Direction.TO)) {
      return false;
    }

    return uniqueName.equals(mc.uniqueName) && className.equals(mc.className)
        && version.equals(mc.version) && linkConfig.equals((mc.getLinkConfig()));
  }

  @Override
  public int hashCode() {
    SupportedDirections supportedDirections = getSupportedDirections();
    int result = getLinkConfig().hashCode();
    if (supportedDirections.isDirectionSupported(Direction.FROM)) {
      result = 31 * result + getFromConfig().hashCode();
    }
    if (supportedDirections.isDirectionSupported(Direction.TO)) {
      result = 31 * result + getToConfig().hashCode();
    }
    result = 31 * result + version.hashCode();
    result = 31 * result + uniqueName.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  public MConnector clone(boolean cloneWithValue) {
    // Connector never have any values filled
    cloneWithValue = false;

    MFromConfig fromConfig = this.getFromConfig();
    MToConfig toConfig = this.getToConfig();

    if (fromConfig != null) {
      fromConfig = fromConfig.clone(cloneWithValue);
    }

    if (toConfig != null) {
      toConfig = toConfig.clone(cloneWithValue);
    }

    MConnector copy = new MConnector(this.getUniqueName(), this.getClassName(), this.getVersion(),
        this.getLinkConfig().clone(cloneWithValue), fromConfig, toConfig);
    copy.setPersistenceId(this.getPersistenceId());
    return copy;
  }

  public MLinkConfig getLinkConfig() {
    return linkConfig;
  }

  public MFromConfig getFromConfig() {
    return fromConfig;
  }

  public MToConfig getToConfig() {
    return toConfig;
  }

  public String getVersion() {
    return version;
  }

  public MConfigurableType getType() {
    return MConfigurableType.CONNECTOR;
  }

  public SupportedDirections getSupportedDirections() {
    return new SupportedDirections(this.getFromConfig() != null,
        this.getToConfig() != null);
  }
}
