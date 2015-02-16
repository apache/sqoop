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
import org.apache.sqoop.common.SqoopException;

import java.util.ArrayList;
import java.util.List;

/**
 * Arbitrary list of config objects.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MConfigList implements MClonable {

  private final List<MConfig> configObjects;
  private final MConfigType type;

  public MConfigList(List<MConfig> configObjects, MConfigType type) {
    this.configObjects = configObjects;
    this.type = type;
  }

  public List<MConfig> getConfigs() {
    return configObjects;
  }

  public MConfig getConfig(String configName) {
    for (MConfig config : configObjects) {
      if (configName.equals(config.getName())) {
        return config;
      }
    }
    throw new SqoopException(ModelError.MODEL_010, "config name: " + configName);
  }

  public MConfigType getType() {
    return type;
  }

  public MInput getInput(String name) {
    String[] parts = name.split("\\.");
    if (parts.length != 2) {
      throw new SqoopException(ModelError.MODEL_009, name);
    }

    return getConfig(parts[0]).getInput(name);
  }

  public MStringInput getStringInput(String name) {
    return (MStringInput) getInput(name);
  }

  public MEnumInput getEnumInput(String name) {
    return (MEnumInput) getInput(name);
  }

  public MIntegerInput getIntegerInput(String name) {
    return (MIntegerInput) getInput(name);
  }

  public MLongInput getLongInput(String name) {
    return (MLongInput) getInput(name);
  }

  public MMapInput getMapInput(String name) {
    return (MMapInput) getInput(name);
  }

  public MBooleanInput getBooleanInput(String name) {
    return (MBooleanInput) getInput(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MConfigList)) {
      return false;
    }

    MConfigList mConfigList = (MConfigList) o;
    if (!configObjects.equals(mConfigList.configObjects) || !type.equals(mConfigList.type)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + type.hashCode();
    for (MConfig config : configObjects) {
      result = 31 * result + config.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Configs: ");
    for (MConfig config : configObjects) {
      sb.append(config.toString());
    }
    sb.append("Type: " + type);
    return sb.toString();
  }

  @Override
  public MConfigList clone(boolean cloneWithValue) {
    List<MConfig> copyConfigs = null;
    if (this.getConfigs() != null) {
      copyConfigs = new ArrayList<MConfig>();
      for (MConfig itr : this.getConfigs()) {
        MConfig newConfig = itr.clone(cloneWithValue);
        newConfig.setPersistenceId(itr.getPersistenceId());
        copyConfigs.add(newConfig);
      }
    }
    MConfigList copyConfigList = new MConfigList(copyConfigs, type);
    return copyConfigList;
  }
}
