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

import static org.apache.sqoop.repository.derby.DerbySchemaUpgradeQuery.*;
import static org.apache.sqoop.repository.derby.DerbySchemaCreateQuery.*;
import static org.apache.sqoop.repository.derby.DerbySchemaInsertUpdateDeleteSelectQuery.*;
import static org.testng.AssertJUnit.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MToConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Abstract class with convenience methods for testing derby repository.
 */
abstract public class DerbyTestCase {

  public static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

  public static final String JDBC_URL = "jdbc:derby:memory:myDB";

  private Connection connection;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    // Create link to the database
    Class.forName(DERBY_DRIVER).newInstance();
    connection = DriverManager.getConnection(getStartJdbcUrl());
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    // Close active link
    if (connection != null) {
      connection.close();
    }

    try {
      // Drop in memory database
      DriverManager.getConnection(getStopJdbcUrl());
    } catch (SQLException ex) {
      // Dropping Derby database leads always to exception
    }
  }

  void renameEntitiesForConnectionAndForm() throws Exception {
    // SQ_LINK schema upgrades
    // drop the constraint before rename and add it back later
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_1);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_2);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_3);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONSTRAINT_4);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_TO_SQ_LINK);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_1);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_2);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_3);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_4);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_5);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_6);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_7);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_COLUMN_8);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTION_CONNECTOR_CONSTRAINT);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONNECTOR_CONSTRAINT);

    // SQ_LINK_INPUT schema upgrades
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_TO_SQ_LINK_INPUT);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_1);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_2);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTION_INPUT_COLUMN_3);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT);

    // SQ_CONFIG schema upgrades
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONSTRAINT);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_TO_SQ_CONFIG);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_1);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_2);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_3);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_4);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_5);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_FORM_COLUMN_6);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_FORM_CONNECTOR_CONSTRAINT);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT);

    // SQ_INPUT schema upgrades
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_INPUT_FORM_COLUMN);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_INPUT_CONSTRAINT);

    // SQ_JOB schema upgrades
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_1);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_COLUMN_2);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_FROM);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_JOB_CONSTRAINT_TO);
  }

  void renameConnectorToConfigurable() throws Exception {

    // SQ_CONNECTOR to SQ_CONFIGURABLE upgrade
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONFIG_CONNECTOR_CONSTRAINT);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_LINK_CONSTRAINT);
    runQuery(QUERY_UPGRADE_DROP_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT);

    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONNECTOR_TO_SQ_CONFIGURABLE);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_CONFIG_COLUMN_1);
    runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_LINK_COLUMN_1);
    runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_COLUMN_SQC_TYPE);

    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONFIG_CONFIGURABLE_CONSTRAINT);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_CONFIGURABLE_CONSTRAINT);
    runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_CONNECTOR_DIRECTION_CONSTRAINT);

  }

  /**
   *Create derby schema. FIX(SQOOP-1583): This code needs heavy refactoring.
   *Details are in the ticket.
   *
   *@throws Exception
   */
  protected void createOrUpgradeSchema(int version) throws Exception {
    if (version > 0) {
      runQuery(QUERY_CREATE_SCHEMA_SQOOP);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR);
      runQuery(QUERY_CREATE_TABLE_SQ_FORM);
      runQuery(QUERY_CREATE_TABLE_SQ_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTION_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_JOB_INPUT);
      runQuery(QUERY_CREATE_TABLE_SQ_SUBMISSION);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_GROUP);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER);
      runQuery(QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION);
    }

    if (version > 1) {
      runQuery(QUERY_CREATE_TABLE_SQ_SYSTEM);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_ENABLED);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_ENABLED);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONNECTION_ADD_COLUMN_UPDATE_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_UPDATE_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_CREATION_USER);
      runQuery(QUERY_UPGRADE_TABLE_SQ_SUBMISSION_ADD_COLUMN_UPDATE_USER);
    }

    if (version > 3) {
      runQuery(QUERY_CREATE_TABLE_SQ_DIRECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_FORM_RENAME_COLUMN_SQF_OPERATION_TO_SQF_DIRECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_RENAME_COLUMN_SQB_CONNECTION_TO_SQB_FROM_CONNECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_COLUMN_SQB_TO_CONNECTION);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_CONSTRAINT_SQB_SQN);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_FROM);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_CONSTRAINT_SQB_SQN_TO);
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_REMOVE_COLUMN_SQB_TYPE);
      renameEntitiesForConnectionAndForm();
      // add the name constraint for job
      runQuery(QUERY_UPGRADE_TABLE_SQ_JOB_ADD_UNIQUE_CONSTRAINT_NAME);
      // add the name constraint for link
      runQuery(QUERY_UPGRADE_TABLE_SQ_LINK_ADD_UNIQUE_CONSTRAINT_NAME);

      runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_DROP_COLUMN_SQ_CFG_DIRECTION_VARCHAR);
      runQuery(QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS);
      runQuery(QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS);

      for (Direction direction : Direction.values()) {
        runQuery(STMT_INSERT_DIRECTION, direction.toString());
      }
      renameConnectorToConfigurable();
      // add the name constraint for configurable
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ADD_UNIQUE_CONSTRAINT_NAME);
      // add sq_config uniqueness constraint
      runQuery(QUERY_UPGRADE_TABLE_SQ_CONFIG_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIGURABLE_ID);
      // add sq_input uniqueness constraint
      runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_UNIQUE_CONSTRAINT_NAME_TYPE_AND_CONFIG_ID);
      // add submission table column name renames
      runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_1);
      runQuery(QUERY_UPGRADE_RENAME_TABLE_SQ_JOB_SUBMISSION_COLUMN_2);
      // SQOOP-1804
      runQuery(QUERY_UPGRADE_TABLE_SQ_INPUT_ADD_COLUMN_SQI_EDITABLE);
      runQuery(QUERY_CREATE_TABLE_SQ_INPUT_RELATION);
      runQuery(QUERY_UPGRADE_ADD_TABLE_SQ_LINK_INPUT_CONSTRAINT_2);

    }

    // deprecated repository version
    runQuery("INSERT INTO SQOOP.SQ_SYSTEM(SQM_KEY, SQM_VALUE) VALUES('version', '" + version + "')");
    // new repository version
    runQuery("INSERT INTO SQOOP.SQ_SYSTEM(SQM_KEY, SQM_VALUE) VALUES('repository.version', '"
        + version + "')");

  }

  protected void createOrUpgradeSchemaForLatestVersion() throws Exception {
    createOrUpgradeSchema(DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION);
  }

  /**
   *Run arbitrary query on derby memory repository.
   *
   *@param query
   *         Query to execute
   *@throws Exception
   */
  protected void runQuery(String query, Object... args) throws Exception {
    PreparedStatement stmt = null;
    try {
      stmt = getDerbyDatabaseConnection().prepareStatement(query);

      for (int i = 0; i < args.length; ++i) {
        if (args[i] instanceof String) {
          stmt.setString(i + 1, (String) args[i]);
        } else if (args[i] instanceof Long) {
          stmt.setLong(i + 1, (Long) args[i]);
        } else {
          stmt.setString(i + 1, args[i].toString());
        }
      }

      stmt.execute();
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  /**
   *Run single, arbitrary insert query on derby memory repository.
   *
   *@param query
   *         Query to execute
   *@return Long id of newly inserted row (-1 if none).
   *@throws Exception
   */
  protected Long runInsertQuery(String query, Object... args) throws Exception {
    PreparedStatement stmt = null;
    try {
      stmt = getDerbyDatabaseConnection().prepareStatement(query,
          PreparedStatement.RETURN_GENERATED_KEYS);

      for (int i = 0; i < args.length; ++i) {
        if (args[i] instanceof String) {
          stmt.setString(i + 1, (String) args[i]);
        } else if (args[i] instanceof Long) {
          stmt.setLong(i + 1, (Long) args[i]);
        } else {
          stmt.setString(i + 1, args[i].toString());
        }
      }

      if (!stmt.execute()) {
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();
        return rs.getLong(1);
      }
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }

    return -1L;
  }

  protected Connection getDerbyDatabaseConnection() {
    return connection;
  }

  protected String getJdbcUrl() {
    return JDBC_URL;
  }

  protected String getStartJdbcUrl() {
    return JDBC_URL + ";create=true";
  }

  protected String getStopJdbcUrl() {
    return JDBC_URL + ";drop=true";
  }

  protected void loadConnectorAndDriverConfigVersion2() throws Exception {
    // Connector entry
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR(SQC_NAME, SQC_CLASS, SQC_VERSION)"
        + "VALUES('A', 'org.apache.sqoop.test.A', '1.0-test')");

    String connector = "1";
    int index = 0;
    // Connector config entries
    for (String operation : new String[] { "null", "'IMPORT'", "'EXPORT'" }) {

      String type;
      if (operation.equals("null")) {
        type = "LINK";
      } else {
        type = "JOB";
      }
      String configName = "C1" + type + index;
      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) " + "VALUES("
          + connector + ", " + operation + ",'" + configName + "'," + "'" + type + "'," + " 0)");
      configName = "C2" + type + index;
      runQuery("INSERT INTO SQOOP.SQ_FORM"
          + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) " + "VALUES("
          + connector + ", " + operation + ",'" + configName + "'," + "'" + type + "'," + " 1)");
      index++;
    }

    // Driver config entries
    runQuery("INSERT INTO SQOOP.SQ_FORM"
        + "(SQF_CONNECTOR, SQF_OPERATION, SQF_NAME, SQF_TYPE, SQF_INDEX) VALUES"
        + "(NULL, 'IMPORT', 'output', 'JOB', 0)," + "(NULL, 'IMPORT', 'throttling', 'JOB', 1),"
        + "(NULL, 'EXPORT', 'input', 'JOB', 0)," + "(NULL, 'EXPORT', 'throttling', 'JOB', 1),"
        + "(NULL, NULL, 'security', 'LINK', 0)");

    // Connector input entries
    for (int i = 0; i < 3; i++) {
      // First config
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I1', " + (i * 2 + 1) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I2', " + (i * 2 + 1) + ", 1, 'MAP', false, 30)");

      // Second config
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I3', " + (i * 2 + 2) + ", 0, 'STRING', false, 30)");
      runQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_FORM, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH)"
          + " VALUES('I4', " + (i * 2 + 2) + ", 1, 'MAP', false, 30)");
    }

    // Driver input entries.
    runQuery("INSERT INTO SQOOP.SQ_INPUT (SQI_NAME, SQI_FORM, SQI_INDEX,"
        + " SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_ENUMVALS)"
        + " VALUES ('security.maxConnections',11,0,'INTEGER','false',NULL,NULL),"
        + "('input.inputDirectory',9,0,'STRING','false',255,NULL),"
        + "('throttling.extractors',8,0,'INTEGER','false',NULL,NULL),"
        + "('throttling.loaders',8,1,'INTEGER','false',NULL,NULL),"
        + "('output.storageType',7,0,'ENUM','false',NULL,'HDFS'),"
        + "('output.outputFormat',7,1,'ENUM','false',NULL,'TEXT_FILE,SEQUENCE_FILE'),"
        + "('output.compression',7,2,'ENUM','false',NULL,'NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY'),"
        + "('output.outputDirectory',7,3,'STRING','false',255,NULL),"
        + "('throttling.extractors',10,0,'INTEGER','false',NULL,NULL),"
        + "('throttling.loaders',10,1,'INTEGER','false',NULL,NULL)");
  }

  protected void addConnectorA() throws Exception {
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('A', 'org.apache.sqoop.test.A', '1.0-test', 'CONNECTOR')");

    // insert 2 directions for connector A
    runInsertQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS(SQCD_CONNECTOR, SQCD_DIRECTION)"
        + "VALUES(1, 1)");// FROM
    runInsertQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS(SQCD_CONNECTOR, SQCD_DIRECTION)"
        + "VALUES(1, 2)");// TO

    // insert the 2 link configs
    // NOTE: link config has no direction
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'l1', 'LINK', 0)");// SQ_CFG_INDEX holds the ordering of
                                        // the config for each type
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'l2', 'LINK', 1)");

    // insert 2 FROM JOB configs
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'from1', 'JOB', 0)");
    // insert the config direction for the from1-config
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
        + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(3, 1)");

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'from2', 'JOB', 1)");
    // insert the config direction for the from2-config
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
        + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(4, 1)");

    // insert 2 TO JOB configs
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'to1', 'JOB', 0)");
    // insert the config direction for the to1-config
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
        + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(5, 2)");

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'to2', 'JOB', 1)");
    // insert the config direction for the to2-config
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
        + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(6, 2)");

    for (int i = 0; i < 3; i++) {
      // First config
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I1', " + (i * 2 + 1) + ", 0, 'STRING', false, 30, 'CONNECTOR_ONLY')");
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I2', " + (i * 2 + 1) + ", 1, 'MAP', false, 30, 'CONNECTOR_ONLY')");

      // Second config
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I3', " + (i * 2 + 2) + ", 0, 'STRING', false, 30, 'CONNECTOR_ONLY')");
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I4', " + (i * 2 + 2) + ", 1, 'MAP', false, 30, 'CONNECTOR_ONLY')");
    }

  }

  protected void loadConnectorAndDriverConfigVersion4() throws Exception {
    Long configId;
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('A', 'org.apache.sqoop.test.A', '1.0-test', 'CONNECTOR')");

    for (String connector : new String[] { "1" }) {
      // Directions
      runInsertQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS(SQCD_CONNECTOR, SQCD_DIRECTION)"
          + "VALUES(" + connector + ", 1)");
      runInsertQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS(SQCD_CONNECTOR, SQCD_DIRECTION)"
          + "VALUES(" + connector + ", 2)");

      // connector configs with connectorId as 1
      // all config names have to be unique per type
      int index = 0;
      for (String direction : new String[] { null, "1", "2" }) {

        String type;
        if (direction == null) {
          type = "LINK";
        } else {
          type = "JOB";
        }
        String configName = "C1" + type + index;
        configId = runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
            + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) " + "VALUES("
            + connector + ", '" + configName + "', '" + type + "', 0)");

        if (direction != null) {
          runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
              + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(" + configId + ", "
              + direction + ")");
        }
        configName = "C2" + type + index;
        configId = runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
            + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) " + "VALUES("
            + connector + ", '" + configName + "', '" + type + "', 1)");

        if (direction != null) {
          runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG_DIRECTIONS"
              + "(SQ_CFG_DIR_CONFIG, SQ_CFG_DIR_DIRECTION) " + "VALUES(" + configId + ", "
              + direction + ")");
        }
        index++;
      }
    }

    // insert a driver
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('SqoopDriver', 'org.apache.sqoop.driver.Driver', '1.0-test', 'DRIVER')");

    // driver config with driverId as 2
    for (String type : new String[] { "JOB" }) {
      runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
          + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) " + "VALUES(2"
          + ", 'd1', '" + type + "', 0)");
      runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
          + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) " + "VALUES(2"
          + ", 'd2', '" + type + "', 1)");
    }

    // Input entries
    // Connector LINK config: 0-3
    // Connector job (FROM) config: 4-7
    // Connector job (TO) config: 8-11
    // Driver JOB config: 12-15
    for (int i = 0; i < 4; i++) {
      // First config inputs with config ids 1,3, 5, 7
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I1', " + (i * 2 + 1) + ", 0, 'STRING', false, 30, 'USER_ONLY')");
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I2', " + (i * 2 + 1) + ", 1, 'MAP', false, 30, 'CONNECTOR_ONLY')");

      // Second config inputs with config ids 2, 4,,6 8
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I3', " + (i * 2 + 2) + ", 0, 'STRING', false, 30, 'USER_ONLY')");
      runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
          + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
          + " VALUES('I4', " + (i * 2 + 2) + ", 1, 'MAP', false, 30, 'USER_ONLY')");
    }
  }

  /**
   *Load testing connector and driver config into repository.
   *
   *@param version
   *         system version (2 or 4)
   *@throws Exception
   */
  protected void loadConnectorAndDriverConfig(int version) throws Exception {
    switch (version) {
    case 2:
      loadConnectorAndDriverConfigVersion2();
      break;

    case 4:
    case 5:
    case 6:
      loadConnectorAndDriverConfigVersion4();
      break;

    default:
      throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  protected void loadConnectorAndDriverConfig() throws Exception {
    loadConnectorAndDriverConfig(DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION);
  }

  /**
   *Load testing link objects into repository.
   *
   *@param version
   *         system version (2 or 4)
   *@throws Exception
   */
  public void loadConnectionsOrLinks(int version) throws Exception {
    switch (version) {
    case 2:
      // Insert two connections - CA and CB
      runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) " + "VALUES('CA', 1)");
      runQuery("INSERT INTO SQOOP.SQ_CONNECTION(SQN_NAME, SQN_CONNECTOR) " + "VALUES('CB', 1)");

      for (String ci : new String[] { "1", "2" }) {
        for (String i : new String[] { "1", "3", "15" }) {
          runQuery("INSERT INTO SQOOP.SQ_CONNECTION_INPUT"
              + "(SQNI_CONNECTION, SQNI_INPUT, SQNI_VALUE) " + "VALUES(" + ci + ", " + i
              + ", 'Value" + i + "')");
        }
      }
      break;

    case 4:
    case 5:
    case 6:
      // Insert two links - CA and CB
      // Connector 1 has one link config
      runQuery("INSERT INTO SQOOP.SQ_LINK(SQ_LNK_NAME, SQ_LNK_CONFIGURABLE) " + "VALUES('CA', 1)");
      runQuery("INSERT INTO SQOOP.SQ_LINK(SQ_LNK_NAME, SQ_LNK_CONFIGURABLE) " + "VALUES('CB', 1)");

      for (String ci : new String[] { "1", "2" }) {
        for (String i : new String[] { "1", "3", "13", "15" }) {
          runQuery("INSERT INTO SQOOP.SQ_LINK_INPUT"
              + "(SQ_LNKI_LINK, SQ_LNKI_INPUT, SQ_LNKI_VALUE) " + "VALUES(" + ci + ", " + i
              + ", 'Value" + i + "')");
        }
      }
      break;

    default:
      throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  public void loadLinksForLatestVersion() throws Exception {
    loadConnectionsOrLinks(DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION);
  }

  /**
   *Load testing job objects into repository.
   *
   *@param version
   *         system version (2 or 4)
   *@throws Exception
   */
  public void loadJobs(int version) throws Exception {
    int index = 0;
    switch (version) {
    case 2:
      for (String type : new String[] { "IMPORT", "EXPORT" }) {
        for (String name : new String[] { "JA", "JB" }) {
          runQuery("INSERT INTO SQOOP.SQ_JOB(SQB_NAME, SQB_CONNECTION, SQB_TYPE)" + " VALUES('"
              + type + "_" + name + "', 1, '" + type + "')");
        }
      }

      // Import inputs
      for (String ci : new String[] { "1", "2" }) {
        for (String i : new String[] { "5", "7", "17", "19" }) {
          runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT" + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
              + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
        }
      }

      // Export inputs
      for (String ci : new String[] { "3", "4" }) {
        for (String i : new String[] { "9", "11" }) {
          runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT" + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
              + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
        }
      }
      break;

    case 4:
    case 5:
    case 6:
      for (String name : new String[] { "JA", "JB", "JC", "JD" }) {
        runQuery("INSERT INTO SQOOP.SQ_JOB(SQB_NAME, SQB_FROM_LINK, SQB_TO_LINK)" + " VALUES('"
            + name + index + "', 1, 1)");
      }

      // Odd IDs inputs have values
      for (String ci : new String[] { "1", "2", "3", "4" }) {
        for (String i : new String[] { "5", "9", "13" }) {
          runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT" + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
              + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
        }

        for (String i : new String[] { "7", "11", "15" }) {
          runQuery("INSERT INTO SQOOP.SQ_JOB_INPUT" + "(SQBI_JOB, SQBI_INPUT, SQBI_VALUE) "
              + "VALUES(" + ci + ", " + i + ", 'Value" + i + "')");
        }
      }
      break;

    default:
      throw new AssertionError("Invalid connector and framework version: " + version);
    }
  }

  /**
   *testing job with non unique name objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueJobsInVersion4() throws Exception {
    int index = 0;
    // insert JAs
    for (String name : new String[] { "JA", "JA", "JA", "JA" }) {
      runQuery("INSERT INTO SQOOP.SQ_JOB(SQB_NAME, SQB_FROM_LINK, SQB_TO_LINK)" + " VALUES('"
          + name + index + "', 1, 1)");
    }
  }

  /**
   *testing link with non unique name objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueLinksInVersion4() throws Exception {

    // Insert two links - CA and CA
    runInsertQuery("INSERT INTO SQOOP.SQ_LINK(SQ_LNK_NAME, SQ_LNK_CONFIGURABLE) "
        + "VALUES('CA', 1)");
    runQuery("INSERT INTO SQOOP.SQ_LINK(SQ_LNK_NAME, SQ_LNK_CONFIGURABLE) " + "VALUES('CA', 1)");
  }

  /**
   *testing configurable with non unique name objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueConfigurablesInVersion4() throws Exception {

    // Insert two configurable - CB and CB
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('CB', 'org.apache.sqoop.test.B', '1.0-test', 'CONNECTOR')");
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('CB', 'org.apache.sqoop.test.B', '1.0-test', 'CONNECTOR')");

  }

  /**
   *testing config with non unique name/type objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueConfigNameTypeInVersion4() throws Exception {

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
  }

  /**
   *testing input with non unique name/type objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueInputNameTypeInVersion4() throws Exception {

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
    runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
        + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH,SQI_EDITABLE)"
        + " VALUES('I1', 1, 0, 'STRING', false, 30, 'CONNECTOR_ONLY')");
    runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
        + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
        + " VALUES('I1', 1, 0, 'STRING', false, 30, 'CONNECTOR_ONLY')");
  }

  /**
   *testing config with non unique name/type objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueConfigNameButUniqueTypeInVersion4() throws Exception {

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'JOB', 0)");
  }

  /**
   *testing config with non unique name/type objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueConfigNameAndTypeButUniqueConfigurableInVersion4() throws Exception {

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(2, 'C1', 'LINK', 1)");
  }

  /**
   *testing input with non unique name/type objects into repository.
   *
   *@throws Exception
   */
  public void loadNonUniqueInputNameAndTypeButUniqueConfigInVersion4() throws Exception {

    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(1, 'C1', 'LINK', 0)");
    runInsertQuery("INSERT INTO SQOOP.SQ_CONFIG"
        + "(SQ_CFG_CONFIGURABLE, SQ_CFG_NAME, SQ_CFG_TYPE, SQ_CFG_INDEX) "
        + "VALUES(2, 'C2', 'LINK', 0)");

    runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
        + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
        + " VALUES('C1.A', 1, 0, 'STRING', false, 30, 'ANY')");
    runInsertQuery("INSERT INTO SQOOP.SQ_INPUT"
        + "(SQI_NAME, SQI_CONFIG, SQI_INDEX, SQI_TYPE, SQI_STRMASK, SQI_STRLENGTH, SQI_EDITABLE)"
        + " VALUES('C1.A', 2, 1, 'STRING', false, 30, 'ANY')");
  }

  public void loadJobsForLatestVersion() throws Exception {
    loadJobs(DerbyRepoConstants.LATEST_DERBY_REPOSITORY_VERSION);
  }

  /**
   *Add a second connector for testing with multiple connectors
   */
  public void addConnectorB() throws Exception {
    // Connector entry
    Long connectorId = runInsertQuery("INSERT INTO SQOOP.SQ_CONFIGURABLE(SQC_NAME, SQC_CLASS, SQC_VERSION, SQC_TYPE)"
        + "VALUES('B', 'org.apache.sqoop.test.B', '1.0-test', 'CONNECTOR')");
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS (SQCD_CONNECTOR, SQCD_DIRECTION) VALUES ("
        + connectorId + ", 1)");
    runQuery("INSERT INTO SQOOP.SQ_CONNECTOR_DIRECTIONS (SQCD_CONNECTOR, SQCD_DIRECTION) VALUES ("
        + connectorId + ", 2)");
  }

  /**
   *Load testing submissions into the repository.
   *
   *@throws Exception
   */
  public void loadSubmissions() throws Exception {
    runQuery("INSERT INTO SQOOP.SQ_COUNTER_GROUP " + "(SQG_NAME) " + "VALUES" + "('gA'), ('gB')");

    runQuery("INSERT INTO SQOOP.SQ_COUNTER " + "(SQR_NAME) " + "VALUES" + "('cA'), ('cB')");

    runQuery("INSERT INTO SQOOP.SQ_SUBMISSION"
        + "(SQS_JOB, SQS_STATUS, SQS_CREATION_DATE, SQS_UPDATE_DATE,"
        + " SQS_EXTERNAL_ID, SQS_EXTERNAL_LINK, SQS_ERROR_SUMMARY," + " SQS_ERROR_DETAILS)"
        + "VALUES " + "(1, 'RUNNING', '2012-01-01 01:01:01', '2012-01-01 01:01:01', 'job_1',"
        + "NULL, NULL, NULL),"
        + "(2, 'SUCCEEDED', '2012-01-01 01:01:01', '2012-01-02 01:01:01', 'job_2',"
        + " NULL, NULL, NULL),"
        + "(3, 'FAILED', '2012-01-01 01:01:01', '2012-01-03 01:01:01', 'job_3',"
        + " NULL, NULL, NULL),"
        + "(4, 'UNKNOWN', '2012-01-01 01:01:01', '2012-01-04 01:01:01', 'job_4',"
        + " NULL, NULL, NULL),"
        + "(1, 'RUNNING', '2012-01-01 01:01:01', '2012-01-05 01:01:01', 'job_5',"
        + " NULL, NULL, NULL)");

    runQuery("INSERT INTO SQOOP.SQ_COUNTER_SUBMISSION "
        + "(SQRS_GROUP, SQRS_COUNTER, SQRS_SUBMISSION, SQRS_VALUE) " + "VALUES" + "(1, 1, 4, 300)");

  }

  protected MConnector getConnector() {
    return getConnector(true, true);
  }

  protected MConnector getConnectorWithIncorrectOverridesAttribute() {
    return getBadConnector(true, true);
  }

  protected MConnector getConnectorWithIncorrectOverridesAttribute2() {
    return getBadConnector2(true, true);
  }

  protected MConnector getConnectorWithMultipleOverridesAttribute() {
    return getConnectorWithMultipleOverrides(true, true);
  }

  protected MConnector getConnectorWithMultipleOverrides(boolean from, boolean to) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = getMultipleOverridesFromConfig();
    }
    if (to) {
      toConfig = getToConfig();
    }
    return new MConnector("A", "org.apache.sqoop.test.A", "1.0-test", getLinkConfig(), fromConfig,
        toConfig);
  }

  protected MConnector getBadConnector(boolean from, boolean to) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = getBadFromConfig();
    }
    if (to) {
      toConfig = getToConfig();
    }
    return new MConnector("A", "org.apache.sqoop.test.A", "1.0-test", getLinkConfig(), fromConfig,
        toConfig);
  }

  protected MConnector getBadConnector2(boolean from, boolean to) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = getNonExistentOverridesFromConfig();
    }
    if (to) {
      toConfig = getToConfig();
    }
    return new MConnector("A", "org.apache.sqoop.test.A", "1.0-test", getLinkConfig(), fromConfig,
        toConfig);
  }

  protected MConnector getConnector(boolean from, boolean to) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = getFromConfig();
    }
    if (to) {
      toConfig = getToConfig();
    }
    return new MConnector("A", "org.apache.sqoop.test.A", "1.0-test", getLinkConfig(), fromConfig,
        toConfig);
  }

  protected MDriver getDriver() {
    return new MDriver(getDriverConfig(), DriverBean.CURRENT_DRIVER_VERSION);
  }

  protected MDriver getBadDriver() {
    return new MDriver(getBadDriverConfig(), DriverBean.CURRENT_DRIVER_VERSION);
  }

  protected void fillLink(MLink link) {
    List<MConfig> configs = link.getConnectorLinkConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");
  }

  protected void fillJob(MJob job) {
    List<MConfig> configs = job.getFromJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getToJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value13");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value15");
  }

  protected MLinkConfig getLinkConfig() {
    return new MLinkConfig(getConfigs("l1", "l2"));
  }

  protected MFromConfig getFromConfig() {
    return new MFromConfig(getConfigs("from1", "from2"));
  }

  protected MFromConfig getBadFromConfig() {
    return new MFromConfig(getBadConfigs("from1", "from2"));
  }

  protected MFromConfig getMultipleOverridesFromConfig() {
    return new MFromConfig(getMultipleOverrideConfigs("from1", "from2"));
  }

  protected MFromConfig getNonExistentOverridesFromConfig() {
    return new MFromConfig(getBadConfigsWithNonExistingInputOverrides("from1", "from2"));
  }

  protected MToConfig getToConfig() {
    return new MToConfig(getConfigs("to1", "to2"));
  }

  protected MDriverConfig getDriverConfig() {
    return new MDriverConfig(getConfigs("d1", "d2"));
  }

  protected MDriverConfig getBadDriverConfig() {
    return new MDriverConfig(getBadConfigsWithSelfOverrides("d1", "d2"));
  }

  protected List<MConfig> getConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    MInput input = new MStringInput("I1", false, InputEditable.ANY, "I2", (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, "I4", (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.USER_ONLY, "I3");
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }

  protected List<MConfig> getBadConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    // I1 overrides another user_only attribute, hence a bad config
    MInput input = new MStringInput("I1", false, InputEditable.USER_ONLY, "I2", (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.USER_ONLY, "I1");
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, StringUtils.EMPTY, (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }

  protected List<MConfig> getBadConfigsWithSelfOverrides(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    // I1 overrides another user_only attribute, hence a bad config
    MInput input = new MStringInput("I1", false, InputEditable.USER_ONLY, "I2", (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.USER_ONLY, "I2");
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, StringUtils.EMPTY, (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }

  protected List<MConfig> getMultipleOverrideConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    // I1 overrides another user_only attribute, hence a bad config
    MInput input = new MStringInput("I1", false, InputEditable.USER_ONLY, "I2", (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.ANY, "I1,I3");
    inputs.add(input);
    input = new MMapInput("I3", false, InputEditable.CONNECTOR_ONLY, "I1");
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, StringUtils.EMPTY, (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }

  protected List<MConfig> getBadConfigsWithNonExistingInputOverrides(String configName1,
      String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    // I1 overrides another user_only attribute, hence a bad config
    MInput input = new MStringInput("I1", false, InputEditable.USER_ONLY, "I2", (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.USER_ONLY, "Foo");
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, StringUtils.EMPTY, (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, StringUtils.EMPTY);
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }

  /**
   *Find out number of entries in given table.
   *
   *@param table
   *         Table name
   *@return Number of rows in the table
   *@throws Exception
   */
  protected long countForTable(String table) throws Exception {
    Statement stmt = null;
    ResultSet rs = null;

    try {
      stmt = getDerbyDatabaseConnection().createStatement();

      rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
      rs.next();

      return rs.getLong(1);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (rs != null) {
        rs.close();
      }
    }
  }

  /**
   *Assert row count for given table.
   *
   *@param table
   *         Table name
   *@param expected
   *         Expected number of rows
   *@throws Exception
   */
  protected void assertCountForTable(String table, long expected) throws Exception {
    long count = countForTable(table);
    assertEquals(expected, count);
  }

  /**
   *Printout repository content for advance debugging.
   *
   *This method is currently unused, but might be helpful in the future, so I'm
   *letting it here.
   *
   *@throws Exception
   */
  protected void generateDatabaseState() throws Exception {
    for (String tbl : new String[] { "SQ_CONNECTOR", "SQ_CONFIG", "SQ_INPUT", "SQ_LINK",
        "SQ_LINK_INPUT", "SQ_JOB", "SQ_JOB_INPUT" }) {
      generateTableState("SQOOP." + tbl);
    }
  }

  /**
   *Printout one single table.
   *
   *@param table
   *         Table name
   *@throws Exception
   */
  protected void generateTableState(String table) throws Exception {
    PreparedStatement ps = null;
    ResultSet rs = null;
    ResultSetMetaData rsmt = null;

    try {
      ps = getDerbyDatabaseConnection().prepareStatement("SELECT * FROM " + table);
      rs = ps.executeQuery();

      rsmt = rs.getMetaData();

      StringBuilder sb = new StringBuilder();
      System.out.println("Table " + table + ":");

      for (int i = 1; i <= rsmt.getColumnCount(); i++) {
        sb.append("| ").append(rsmt.getColumnName(i)).append(" ");
      }
      sb.append("|");
      System.out.println(sb.toString());

      while (rs.next()) {
        sb = new StringBuilder();
        for (int i = 1; i <= rsmt.getColumnCount(); i++) {
          sb.append("| ").append(rs.getString(i)).append(" ");
        }
        sb.append("|");
        System.out.println(sb.toString());
      }

      System.out.println("");

    } finally {
      if (rs != null) {
        rs.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
  }
}
