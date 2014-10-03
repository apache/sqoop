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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ConfigValidator;
import org.apache.sqoop.validation.Validator;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Validator to ensure that user is supplying valid input
 */
public class GenericJdbcValidator extends Validator {

  @Override
  public ConfigValidator validateConfigForLink(Object configuration) {
    ConfigValidator validation = new ConfigValidator(LinkConfiguration.class);
    LinkConfiguration linkConfig = (LinkConfiguration)configuration;

    if(linkConfig.linkConfig.jdbcDriver == null) {
      validation.addMessage(Status.UNACCEPTABLE, "link", "jdbcDriver", "Driver can't be empty");
    } else {
      try {
        Class.forName(linkConfig.linkConfig.jdbcDriver);
      } catch (ClassNotFoundException e) {
        validation.addMessage(Status.UNACCEPTABLE, "link", "jdbcDriver", "Can't load specified driver");
      }
    }

    if(linkConfig.linkConfig.connectionString == null) {
      validation.addMessage(Status.UNACCEPTABLE, "link", "connectionString", "JDBC URL can't be empty");
    } else if(!linkConfig.linkConfig.connectionString.startsWith("jdbc:")) {
      validation.addMessage(Status.UNACCEPTABLE, "link", "connectionString", "This do not seem as a valid JDBC URL");
    }

    // See if we can connect to the database
    try {
      DriverManager.getConnection(linkConfig.linkConfig.connectionString,
        linkConfig.linkConfig.username, linkConfig.linkConfig.password);
    } catch (SQLException e) {
      validation.addMessage(Status.ACCEPTABLE, "link", "Can't connect to the database with given credentials: " + e.getMessage());
    }

    // Return final validation object
    return validation;
  }

  @Override
  public ConfigValidator validateConfigForJob(Object jobConfiguration) {
    if (jobConfiguration instanceof FromJobConfiguration) {
      return validateFromJobConfiguration((FromJobConfiguration)jobConfiguration);
    } else if (jobConfiguration instanceof ToJobConfiguration) {
      return validateToJobConfiguration((ToJobConfiguration)jobConfiguration);
    } else {
      throw new SqoopException(GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0020,
          "Configuration object for unknown direction.");
    }
  }

  private ConfigValidator validateToJobConfiguration(ToJobConfiguration configuration) {
    ConfigValidator validation = new ConfigValidator(FromJobConfiguration.class);

    if(configuration.toJobConfig.tableName == null && configuration.toJobConfig.sql == null) {
      validation.addMessage(Status.UNACCEPTABLE, "toJobConfig", "Either table name or SQL must be specified");
    }
    if(configuration.toJobConfig.tableName != null && configuration.toJobConfig.sql != null) {
      validation.addMessage(Status.UNACCEPTABLE, "toJobConfig", "Both table name and SQL cannot be specified");
    }
    if(configuration.toJobConfig.tableName == null &&
        configuration.toJobConfig.stageTableName != null) {
      validation.addMessage(Status.UNACCEPTABLE, "toJobConfig",
        "Stage table name cannot be specified without specifying table name");
    }
    if(configuration.toJobConfig.stageTableName == null &&
        configuration.toJobConfig.clearStageTable != null) {
      validation.addMessage(Status.UNACCEPTABLE, "toJobConfig",
        "Clear stage table cannot be specified without specifying name of " +
        "the stage table.");
    }

    return validation;
  }

  private ConfigValidator validateFromJobConfiguration(FromJobConfiguration configuration) {
    ConfigValidator validation = new ConfigValidator(FromJobConfiguration.class);

    if(configuration.fromJobConfig.tableName == null && configuration.fromJobConfig.sql == null) {
      validation.addMessage(Status.UNACCEPTABLE, "fromJobConfig", "Either table name or SQL must be specified");
    }
    if(configuration.fromJobConfig.tableName != null && configuration.fromJobConfig.sql != null) {
      validation.addMessage(Status.UNACCEPTABLE, "fromJobConfig", "Both table name and SQL cannot be specified");
    }
    if(configuration.fromJobConfig.schemaName != null && configuration.fromJobConfig.sql != null) {
      validation.addMessage(Status.UNACCEPTABLE, "fromJobConfig", "Both schema name and SQL cannot be specified");
    }

    if(configuration.fromJobConfig.sql != null && !configuration.fromJobConfig.sql.contains(GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN)) {
      validation.addMessage(Status.UNACCEPTABLE, "fromJobConfig", "sql", "SQL statement must contain placeholder for auto generated "
        + "conditions - " + GenericJdbcConnectorConstants.SQL_CONDITIONS_TOKEN);
    }

    return validation;
  }
}
