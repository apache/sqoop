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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.configurable.ConfigurableUpgradeUtil;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GenericJdbcConnectorUpgrader extends ConnectorConfigurableUpgrader {

  @Override
  public void upgradeLinkConfig(MLinkConfig original, MLinkConfig upgradeTarget) {
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }

  @Override
  public void upgradeFromJobConfig(MFromConfig original, MFromConfig upgradeTarget) {
    // Move all configuration options that did not change
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());

    // We've changed "String columns" to "List<String> columnList" as it better represents the type
    migrateColumnsToColumnList(original, upgradeTarget, "fromJobConfig");
  }

  @Override
  public void upgradeToJobConfig(MToConfig original, MToConfig upgradeTarget) {
    // Move all configuration options that did not change
    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());

    // We've changed "String columns" to "List<String> columnList" as it better represents the type
    migrateColumnsToColumnList(original, upgradeTarget, "toJobConfig");
  }

  private void migrateColumnsToColumnList(MConfigList original, MConfigList upgradeTarget, String configName) {
    String oldInputName = configName + ".columns";
    String newInputName = configName + ".columnList";

    if(original.getConfig(configName).getInputNames().contains(oldInputName)) {
      String columnString = original.getStringInput(oldInputName).getValue();
      String[] columns = columnString.split(","); // Our code has expected comma separated list in the past
      upgradeTarget.getListInput(newInputName).setValue(Arrays.asList(columns));
    }
  }
}
