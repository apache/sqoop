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
package org.apache.sqoop.repository.derby;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.DerbyRepoError;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sqoop 1.99.4 release included the following changes:
 * 1. FROM/TO refactoring
 * 2. Nomenclature improvements
 *
 * With the above improvements, the Generic JDBC Connector
 * and it's configurations need to be updated.
 *
 * This class is intended to handle the updates to the Generic JDBC Connector.
 */
public class DerbyUpgradeGenericJdbcConnectorConfigAndInputNames {
  private static final Logger LOG =
      Logger.getLogger(DerbyUpgradeGenericJdbcConnectorConfigAndInputNames.class);

  private static final String JOB_CONFIGURATION_FORM_NAME = "table";
  private static final String CONNECTION_CONFIGURATION_FORM_NAME = "connection";
  private static final String LINK_CONFIG_NAME = "linkConfig";
  private static final String FROM_JOB_CONFIG_NAME = "fromJobConfig";
  private static final String TO_JOB_CONFIG_NAME = "toJobConfig";
  private static final Map<String, String> CONNECTION_TO_LINK_CONFIG_INPUT_MAP;
  private static final Map<String, String> IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP;
  private static final Map<String, String> EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP;

  static {
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP = new HashMap<String, String>();
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".jdbcDriver", LINK_CONFIG_NAME + ".jdbcDriver");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".connectionString", LINK_CONFIG_NAME + ".connectionString");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".username", LINK_CONFIG_NAME + ".username");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".password", LINK_CONFIG_NAME + ".password");
    CONNECTION_TO_LINK_CONFIG_INPUT_MAP.put(CONNECTION_CONFIGURATION_FORM_NAME + ".jdbcProperties", LINK_CONFIG_NAME + ".jdbcProperties");

    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP = new HashMap<String, String>();
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".schemaName", FROM_JOB_CONFIG_NAME + ".schemaName");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".tableName", FROM_JOB_CONFIG_NAME + ".tableName");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".sql", FROM_JOB_CONFIG_NAME + ".sql");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".columns", FROM_JOB_CONFIG_NAME + ".columns");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".partitionColumn", FROM_JOB_CONFIG_NAME + ".partitionColumn");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".partitionColumnNull", FROM_JOB_CONFIG_NAME + ".allowNullValueInPartitionColumn");
    IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".boundaryQuery", FROM_JOB_CONFIG_NAME + ".boundaryQuery");

    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP = new HashMap<String, String>();
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".schemaName", TO_JOB_CONFIG_NAME + ".schemaName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".tableName", TO_JOB_CONFIG_NAME + ".tableName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".sql", TO_JOB_CONFIG_NAME + ".sql");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".columns", TO_JOB_CONFIG_NAME + ".columns");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".stageTableName", TO_JOB_CONFIG_NAME + ".stageTableName");
    EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP.put(JOB_CONFIGURATION_FORM_NAME + ".clearStageTable", TO_JOB_CONFIG_NAME + ".shouldClearStageTable");
  }

  private Connection connection;
  private DerbyRepositoryHandler handler;

  public DerbyUpgradeGenericJdbcConnectorConfigAndInputNames(DerbyRepositoryHandler handler, Connection connection) {
    this.handler = handler;
    this.connection = connection;
  }

  public void execute() {
    LOG.info("Renaming Generic JDBC Connector configs and inputs.");

    Long linkConfigId = getConfigId(false, CONNECTION_CONFIGURATION_FORM_NAME);
    Long fromJobConfigId = getConfigId(true, JOB_CONFIGURATION_FORM_NAME, Direction.FROM.toString());
    Long toJobConfigId = getConfigId(true, JOB_CONFIGURATION_FORM_NAME, Direction.TO.toString());

    if (linkConfigId != null) {
      LOG.info("Renaming LINK config (" + linkConfigId + ") and inputs.");
      renameConfig(linkConfigId, LINK_CONFIG_NAME);
      renameConfigInputs(linkConfigId, CONNECTION_TO_LINK_CONFIG_INPUT_MAP);
    } else {
      LOG.info("Renaming LINK config and inputs skipped.");
    }

    if (fromJobConfigId != null) {
      LOG.info("Renaming FROM config (" + fromJobConfigId + ") and inputs.");
      renameConfig(fromJobConfigId, FROM_JOB_CONFIG_NAME);
      renameConfigInputs(fromJobConfigId, IMPORT_JOB_TABLE_TO_FROM_CONFIG_INPUT_MAP);
    } else {
      LOG.info("Renaming FROM config and inputs skipped.");
    }

    if (toJobConfigId != null) {
      LOG.info("Renaming TO config (" + fromJobConfigId + ") and inputs.");
      renameConfig(toJobConfigId, TO_JOB_CONFIG_NAME);
      renameConfigInputs(toJobConfigId, EXPORT_JOB_TABLE_TO_TO_CONFIG_INPUT_MAP);
    } else {
      LOG.info("Renaming TO config and inputs skipped.");
    }

    LOG.info("Done Generic JDBC Connector configs and inputs.");
  }

  private Long getConfigId(boolean direction, String ... args) {
    PreparedStatement statement = null;
    String configIdQuery = (direction) ?
        DerbySchemaUpgradeQuery.QUERY_SELECT_CONFIG_ID_BY_NAME_AND_DIRECTION : DerbySchemaUpgradeQuery.QUERY_SELECT_CONFIG_ID_BY_NAME;

    try {
      statement = connection.prepareStatement(configIdQuery);

      for (int i = 0; i < args.length; ++i) {
        statement.setString(i + 1, args[i]);
      }

      ResultSet configIdResultSet = statement.executeQuery();

      LOG.debug("QUERY(" + configIdQuery + ") with args [" + StringUtils.join(args, ",") + "] fetch size: " + configIdResultSet.getFetchSize());

      if (!configIdResultSet.next() || configIdResultSet.getFetchSize() != 1) {
        return null;
      }

      return configIdResultSet.getLong(1);
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0002, e);
    } finally {
      handler.closeStatements(statement);
    }
  }

  private void renameConfig(long configId, String configName) {
    PreparedStatement statement = null;
    String query = DerbySchemaUpgradeQuery.QUERY_UPDATE_TABLE_SQ_CONFIG_NAME;

    try {
      statement = connection.prepareStatement(query);
      statement.setString(1, configName);
      statement.setLong(2, configId);

      int updateCount = statement.executeUpdate();
      LOG.debug("QUERY(" + query + ") with args [" + configName + ", " + configId + "] update count: " + updateCount);
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0002, e);
    } finally {
      handler.closeStatements(statement);
    }
  }

  private void renameConfigInputs(long configId, Map<String, String> inputNameMap) {
    PreparedStatement statement = null;

    try {
      statement = connection.prepareStatement(DerbySchemaUpgradeQuery.QUERY_UPDATE_TABLE_SQ_INPUT_SQI_NAME);

      for (String inputName : inputNameMap.keySet()) {
        statement.setString(1, inputNameMap.get(inputName));
        statement.setString(2, inputName);
        statement.setLong(3, configId);
        statement.addBatch();

        LOG.debug("QUERY(" + DerbySchemaUpgradeQuery.QUERY_UPDATE_TABLE_SQ_INPUT_SQI_NAME + ") args ["
            + inputNameMap.get(inputName) + "," + inputName + "," + configId + "]");
      }

      int[] updateCounts = statement.executeBatch();
      LOG.debug("QUERY(" + DerbySchemaUpgradeQuery.QUERY_UPDATE_TABLE_SQ_INPUT_SQI_NAME + ") update count: "
          + StringUtils.join(ArrayUtils.toObject(updateCounts), ","));
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0002, e);
    } finally {
      handler.closeStatements(statement);
    }
  }
}
