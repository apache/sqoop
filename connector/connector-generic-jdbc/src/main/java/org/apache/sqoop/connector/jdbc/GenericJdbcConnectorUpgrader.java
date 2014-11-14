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
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;

import java.util.HashMap;
import java.util.Map;

// NOTE: All config types have the similar upgrade path at this point
public class GenericJdbcConnectorUpgrader extends ConnectorConfigurableUpgrader {

  private static final String JOB_CONFIGURATION_FORM_NAME = "table";
  private static final String CONNECTION_CONFIGURATION_FORM_NAME = "connection";
  private static final Map<String, String> CONNECTION_TO_LINK_CONFIG_INPUT_MAP;
  private static final Map<String, String> IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP;
  private static final Map<String, String> EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP;

  static {
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP = new HashMap<String, String>();
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".jdbcDriver", "linkConfig.jdbcDriver");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".connectionString", "linkConfig.connectionString");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".username", "linkConfig.username");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".password", "linkConfig.password");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".jdbcProperties", "linkConfig.jdbcProperties");

    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP = new HashMap<String, String>();
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".schemaName", "fromJobConfig.schemaName");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".tableName", "fromJobConfig.tableName");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".sql", "fromJobConfig.sql");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".columns", "fromJobConfig.columns");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".partitionColumn", "fromJobConfig.partitionColumn");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".partitionColumnNull", "fromJobConfig.allowNullValueInPartitionColumn");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".boundaryQuery", "fromJobConfig.boundaryQuery");

    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP = new HashMap<String, String>();
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".schemaName", "toJobConfig.schemaName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".tableName", "toJobConfig.tableName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".sql", "toJobConfig.sql");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".columns", "toJobConfig.columns");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".stageTableName", "toJobConfig.stageTableName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".clearStageTable", "toJobConfig.shouldClearStageTable");
  }

  @Override
  public void upgradeLinkConfig(MLinkConfig original, MLinkConfig upgradeTarget) {
    // Upgrade from 1.99.3 to 1.99.4
    for (MConfig config : original.getConfigs()) {
      if (config.getName().equals(CONNECTION_CONFIGURATION_FORM_NAME)) {
        for (MInput originalInput : config.getInputs()) {
          String inputName = CONNECTION_TO_LINK_CONFIG_INPUT_MAP.get(originalInput.getName());
          MInput input = upgradeTarget.getInput(inputName);
          input.setValue(originalInput.getValue());
        }
      }
    }

    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }

  @Override
  public void upgradeFromJobConfig(MFromConfig original, MFromConfig upgradeTarget) {
    // Upgrade from 1.99.3 to 1.99.4
    for (MConfig config : original.getConfigs()) {
      if (config.getName().equals(JOB_CONFIGURATION_FORM_NAME)) {
        for (MInput originalInput : config.getInputs()) {
          String inputName = IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.get(originalInput.getName());
          MInput input = upgradeTarget.getInput(inputName);
          input.setValue(originalInput.getValue());
        }
      }
    }

    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }

  @Override
  public void upgradeToJobConfig(MToConfig original, MToConfig upgradeTarget) {
    // Upgrade from 1.99.3 to 1.99.4
    for (MConfig config : original.getConfigs()) {
      if (config.getName().equals(JOB_CONFIGURATION_FORM_NAME)) {
        for (MInput originalInput : config.getInputs()) {
          String inputName = EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.get(originalInput.getName());
          MInput input = upgradeTarget.getInput(inputName);
          input.setValue(originalInput.getValue());
        }
      }
    }

    ConfigurableUpgradeUtil.doUpgrade(original.getConfigs(), upgradeTarget.getConfigs());
  }
}
