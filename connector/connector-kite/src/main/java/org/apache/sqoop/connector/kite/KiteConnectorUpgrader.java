/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.kite;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.configurable.ConfigurableUpgradeUtil;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KiteConnectorUpgrader extends ConnectorConfigurableUpgrader {
  private static final Logger LOG = Logger.getLogger(KiteConnectorUpgrader.class);

  private static final Map<String, String> LINK_CONFIG_MAP;

  static {
    LINK_CONFIG_MAP = new HashMap<String, String>();
    LINK_CONFIG_MAP.put("linkConfig.authority", "linkConfig.hdfsHostAndPort");
  }

  @Override
  public void upgradeLinkConfig(MLinkConfig original, MLinkConfig upgradeTarget) {
    Map<String, MConfig> configMap = new HashMap<String, MConfig>();
    for (MConfig config : original.getConfigs()) {
      configMap.put(config.getName(), config);
    }
    for (MConfig config : upgradeTarget.getConfigs()) {
      List<MInput<?>> inputs = config.getInputs();
      MConfig originalConfig = configMap.get(config.getName());
      if (originalConfig == null) {
        LOG.warn("Config: '" + config.getName() + "' not present in old " +
            "configurable. So it and its inputs will not be transferred by the upgrader.");
        continue;
      }
      for (MInput input : inputs) {
        try {
          if (LINK_CONFIG_MAP.containsKey(input.getName())) {
            input.setValue(originalConfig.getInput(LINK_CONFIG_MAP.get(input.getName())).getValue());
          } else {
            input.setValue(originalConfig.getInput(input.getName()).getValue());
          }
        } catch (SqoopException ex) {
          LOG.warn("Input: '" + input.getName() + "' not present in old " +
              "configurable. So it will not be transferred by the upgrader.");
        }
      }
    }
  }

  @Override
  public void upgradeFromJobConfig(MFromConfig original, MFromConfig upgradeTarget) {
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }

  @Override
  public void upgradeToJobConfig(MToConfig original, MToConfig upgradeTarget) {
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }

}
