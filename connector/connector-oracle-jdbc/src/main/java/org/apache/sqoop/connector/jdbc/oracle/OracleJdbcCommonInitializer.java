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
package org.apache.sqoop.connector.jdbc.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleActiveInstance;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleConnectionFactory;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleJdbcUrl;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleTable;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.JdbcOracleThinConnectionParsingError;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleVersion;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;

public abstract class OracleJdbcCommonInitializer<JobConfiguration> extends Initializer<LinkConfiguration, JobConfiguration> {

  private static final Logger LOG =
      Logger.getLogger(OracleJdbcCommonInitializer.class);

  protected Connection connection;
  protected OracleTable table;
  protected int numMappers = 8;

  public void connect(InitializerContext context,
      LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) throws SQLException {
    connection = OracleConnectionFactory.makeConnection(
        linkConfiguration.connectionConfig);
  }

  @Override
  public void initialize(InitializerContext context,
      LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) {
    showUserTheOraOopWelcomeMessage();

    try {
      connect(context, linkConfiguration, jobConfiguration);
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\nError:%s",
          linkConfiguration.connectionConfig.connectionString, ex
              .getMessage()), ex);
    }

    // Generate the "action" name that we'll assign to our Oracle sessions
    // so that the user knows which Oracle sessions belong to OraOop...
    //TODO: Get the job name
    context.getContext().setString(
        OracleJdbcConnectorConstants.ORACLE_SESSION_ACTION_NAME,
        getOracleSessionActionName(
            linkConfiguration.connectionConfig.username));

    //TODO: Don't think this can be done anymore
    //OraOopUtilities.appendJavaSecurityEgd(sqoopOptions.getConf());

    // Get the Oracle database version...
    try {
      OracleVersion oracleVersion =
          OracleQueries.getOracleVersion(connection);
      LOG.info(String.format("Oracle Database version: %s",
          oracleVersion.getBanner()));
    } catch (SQLException ex) {
      LOG.error("Unable to obtain the Oracle database version.", ex);
    }

    // Generate the JDBC URLs to be used by each mapper...
    setMapperConnectionDetails(linkConfiguration.connectionConfig,
        context.getContext());

    // Show the user the Oracle command that can be used to kill this
    // OraOop
    // job via Oracle...
    showUserTheOracleCommandToKillOraOop(context.getContext());
  }

  protected abstract List<String>
      getColumnNames(JobConfiguration jobConfiguration) throws SQLException;

  @Override
  public Schema getSchema(InitializerContext context,
      LinkConfiguration linkConfiguration,
      JobConfiguration jobConfiguration) {
    try {
      connect(context, linkConfiguration, jobConfiguration);
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\n"
              + "Error:%s", linkConfiguration.connectionConfig.connectionString,
              ex.getMessage()), ex);
    }

    Schema schema = new Schema(table.toString());

    try {
      List<String> colNames = getColumnNames(jobConfiguration);

      List<Column> columnTypes =
            OracleQueries.getColDataTypes(connection, table, colNames);

      for(Column column : columnTypes) {
        schema.addColumn(column);
      }

      return schema;
    } catch(Exception e) {
      throw new RuntimeException(
          "Could not determine columns in Oracle Table.", e);
    }
  }

  private void showUserTheOraOopWelcomeMessage() {

    String msg1 =
        String.format("Using %s",
            OracleJdbcConnectorConstants.ORACLE_SESSION_MODULE_NAME);

    int longestMessage = msg1.length();

    msg1 = StringUtils.rightPad(msg1, longestMessage);

    char[] asterisks = new char[longestMessage + 8];
    Arrays.fill(asterisks, '*');

    String msg =
        String.format("\n" + "%1$s\n" + "*** %2$s ***\n" + "%1$s", new String(
            asterisks), msg1);
    LOG.info(msg);
  }

  private String getOracleSessionActionName(String jobName) {

    String timeStr =
        (new SimpleDateFormat("yyyyMMddHHmmsszzz")).format(new Date());

    String result = String.format("%s %s", jobName, timeStr);

    // NOTE: The "action" column of v$session is only a 32 character column.
    // Therefore we need to ensure that the string returned by this
    // method does not exceed 32 characters...
    if (result.length() > 32) {
      result = result.substring(0, 32).trim();
    }

    return result;
  }

  private void setMapperConnectionDetails(ConnectionConfig connectionConfig,
      MutableContext context) {

    // Query v$active_instances to get a list of all instances in the Oracle RAC
    // (assuming this *could* be a RAC)...
    List<OracleActiveInstance> activeInstances = null;
    try {
      activeInstances =
          OracleQueries.getOracleActiveInstances(connection);
    } catch (SQLException ex) {
      throw new RuntimeException(
          "An error was encountered when attempting to determine the "
        + "configuration of the Oracle RAC.",
        ex);
    }

    if (activeInstances == null) {
      LOG.info("This Oracle database is not a RAC.");
    } else {
      LOG.info("This Oracle database is a RAC.");
    }

    // Is dynamic JDBC URL generation disabled?...
    if (OracleUtilities.oracleJdbcUrlGenerationDisabled(connectionConfig)) {
      LOG.info(String
          .format(
              "%s will not use dynamically generated JDBC URLs - this feature "
            + "has been disabled.",
            OracleJdbcConnectorConstants.CONNECTOR_NAME));
      return;
    }

    boolean generateRacBasedJdbcUrls = false;

    // Decide whether this is a multi-instance RAC, and whether we need to do
    // anything more...
    if (activeInstances != null) {
      generateRacBasedJdbcUrls = true;

      if (activeInstances.size() < OracleJdbcConnectorConstants.
          MIN_NUM_RAC_ACTIVE_INSTANCES_FOR_DYNAMIC_JDBC_URLS) {
        LOG.info(String.format(
            "There are only %d active instances in the Oracle RAC. "
          + "%s will not bother utilizing dynamically generated JDBC URLs.",
          activeInstances.size(),
          OracleJdbcConnectorConstants.CONNECTOR_NAME));
        generateRacBasedJdbcUrls = false;
      }
    }

    // E.g. jdbc:oracle:thin:@localhost.localdomain:1521:orcl
    String jdbcConnectStr = connectionConfig.connectionString;

    // Parse the JDBC URL to obtain the port number for the TNS listener...
    String jdbcHost = "";
    int jdbcPort = 0;
    String jdbcSid = "";
    String jdbcService = "";
    String jdbcTnsName = "";
    try {

      OracleJdbcUrl oraOopJdbcUrl = new OracleJdbcUrl(jdbcConnectStr);
      OracleUtilities.JdbcOracleThinConnection jdbcConnection =
          oraOopJdbcUrl.parseJdbcOracleThinConnectionString();
      jdbcHost = jdbcConnection.getHost();
      jdbcPort = jdbcConnection.getPort();
      jdbcSid = jdbcConnection.getSid();
      jdbcService = jdbcConnection.getService();
      jdbcTnsName = jdbcConnection.getTnsName();
    } catch (JdbcOracleThinConnectionParsingError ex) {
      LOG.info(String.format(
          "Unable to parse the JDBC connection URL \"%s\" as a connection "
        + "that uses the Oracle 'thin' JDBC driver.\n"
        + "This problem prevents %s from being able to dynamically generate "
        + "JDBC URLs that specify 'dedicated server connections' or spread "
        + "mapper sessions across multiple Oracle instances.\n"
        + "If the JDBC driver-type is 'OCI' (instead of 'thin'), then "
        + "load-balancing should be appropriately managed automatically.",
              jdbcConnectStr, OracleJdbcConnectorConstants.CONNECTOR_NAME, ex));
      return;
    }

    if (generateRacBasedJdbcUrls) {

      // Retrieve the Oracle service name to use when connecting to the RAC...
      String oracleServiceName = connectionConfig.racServiceName;

      // Generate JDBC URLs for each of the mappers...
      if (!oracleServiceName.isEmpty()) {
        if (!generateRacJdbcConnectionUrlsByServiceName(jdbcHost, jdbcPort,
            oracleServiceName, connectionConfig, context)) {
          throw new RuntimeException(String.format(
              "Unable to connect to the Oracle database at %s "
                  + "via the service name \"%s\".", jdbcConnectStr,
                  oracleServiceName));
        }
      } else {
        generateJdbcConnectionUrlsByActiveInstance(activeInstances, jdbcPort,
            connectionConfig, context);
      }
    } else {
      generateJdbcConnectionUrlsByTnsnameSidOrService(jdbcHost, jdbcPort,
        jdbcSid, jdbcService, jdbcTnsName, connectionConfig, context);
    }

  }

  private boolean generateRacJdbcConnectionUrlsByServiceName(String hostName,
      int port, String serviceName, ConnectionConfig connectionConfig,
      MutableContext context) {

    boolean result = false;
    String jdbcUrl =
        OracleUtilities.generateOracleServiceNameJdbcUrl(hostName, port,
            serviceName);

    if (testDynamicallyGeneratedOracleRacInstanceConnection(jdbcUrl,
        connectionConfig.username, connectionConfig.password,
        connectionConfig.jdbcProperties
        , false // <- ShowInstanceSysTimestamp
        , "" // <- instanceDescription
    )) {

      LOG.info(String.format(
          "%s will load-balance sessions across the Oracle RAC instances "
              + "by connecting each mapper to the Oracle Service \"%s\".",
          OracleJdbcConnectorConstants.CONNECTOR_NAME, serviceName));

      // Now store these connection strings in such a way that each mapper knows
      // which one to use...
      for (int idxMapper = 0; idxMapper < numMappers; idxMapper++) {
        storeJdbcUrlForMapper(idxMapper, jdbcUrl, context);
      }
      result = true;
    }
    return result;
  }

  private void generateJdbcConnectionUrlsByTnsnameSidOrService(String hostName,
      int port, String sid, String serviceName, String tnsName,
      ConnectionConfig connectionConfig, MutableContext context) {

      String jdbcUrl = null;
      if (tnsName != null && !tnsName.isEmpty()) {
        jdbcUrl = OracleUtilities.generateOracleTnsNameJdbcUrl(tnsName);
      } else if (sid != null && !sid.isEmpty()) {
        jdbcUrl = OracleUtilities.generateOracleSidJdbcUrl(hostName, port, sid);
      } else {
        jdbcUrl =
            OracleUtilities.generateOracleServiceNameJdbcUrl(hostName, port,
                serviceName);
      }

      // Now store these connection strings in such a way that each mapper knows
      // which one to use...
      for (int idxMapper = 0; idxMapper < numMappers; idxMapper++) {
        storeJdbcUrlForMapper(idxMapper, jdbcUrl, context);
      }
    }

    private void
        generateJdbcConnectionUrlsByActiveInstance(
            List<OracleActiveInstance> activeInstances, int jdbcPort,
            ConnectionConfig connectionConfig, MutableContext context) {

      // Generate JDBC URLs for each of the instances in the RAC...
      ArrayList<OracleUtilities.JdbcOracleThinConnection>
          jdbcOracleActiveThinConnections =
              new ArrayList<OracleUtilities.JdbcOracleThinConnection>(
                  activeInstances.size());

      for (OracleActiveInstance activeInstance : activeInstances) {

        OracleUtilities.JdbcOracleThinConnection
            jdbcActiveInstanceThinConnection =
                new OracleUtilities.JdbcOracleThinConnection(
                    activeInstance.getHostName(),
                    jdbcPort, activeInstance.getInstanceName(), "", "");

        if (testDynamicallyGeneratedOracleRacInstanceConnection(
            jdbcActiveInstanceThinConnection.toString(),
            connectionConfig.username,
            connectionConfig.password, connectionConfig.jdbcProperties,
            true, activeInstance.getInstanceName())) {
          jdbcOracleActiveThinConnections.add(jdbcActiveInstanceThinConnection);
        }
      }

      // If there are multiple JDBC URLs that work okay for the RAC, then we'll
      // make use of them...
      if (jdbcOracleActiveThinConnections.size() < OracleJdbcConnectorConstants.
          MIN_NUM_RAC_ACTIVE_INSTANCES_FOR_DYNAMIC_JDBC_URLS) {
        LOG.info(String
            .format(
                "%s will not attempt to load-balance sessions across instances "
              + "of an Oracle RAC - as multiple JDBC URLs to the "
              + "Oracle RAC could not be dynamically generated.",
              OracleJdbcConnectorConstants.CONNECTOR_NAME));
        return;
      } else {
        StringBuilder msg = new StringBuilder();
        msg.append(String
            .format(
                "%s will load-balance sessions across the following instances of"
              + "the Oracle RAC:\n",
              OracleJdbcConnectorConstants.CONNECTOR_NAME));

        for (OracleUtilities.JdbcOracleThinConnection thinConnection
                 : jdbcOracleActiveThinConnections) {
          msg.append(String.format("\tInstance: %s \t URL: %s\n",
              thinConnection.getSid(), thinConnection.toString()));
        }
        LOG.info(msg.toString());
      }

      // Now store these connection strings in such a way that each mapper knows
      // which one to use...
      int racInstanceIdx = 0;
      OracleUtilities.JdbcOracleThinConnection thinUrl;
      for (int idxMapper = 0; idxMapper < numMappers; idxMapper++) {
        if (racInstanceIdx > jdbcOracleActiveThinConnections.size() - 1) {
          racInstanceIdx = 0;
        }
        thinUrl = jdbcOracleActiveThinConnections.get(racInstanceIdx);
        racInstanceIdx++;
        storeJdbcUrlForMapper(idxMapper, thinUrl.toString(), context);
      }
    }

    private void storeJdbcUrlForMapper(int mapperIdx, String jdbcUrl,
        MutableContext context) {

      // Now store these connection strings in such a way that each mapper knows
      // which one to use...
      String mapperJdbcUrlPropertyName =
          OracleUtilities.getMapperJdbcUrlPropertyName(mapperIdx);
      LOG.debug("Setting mapper url " + mapperJdbcUrlPropertyName + " = "
        + jdbcUrl);
      context.setString(mapperJdbcUrlPropertyName, jdbcUrl);
    }

    private boolean testDynamicallyGeneratedOracleRacInstanceConnection(
        String url, String userName, String password,
        Map<String, String> jdbcProperties,
        boolean showInstanceSysTimestamp, String instanceDescription) {

      boolean result = false;

      // Test the connection...
      try {
        Properties additionalProps = new Properties();
        if(jdbcProperties != null) {
          additionalProps.putAll(jdbcProperties);
        }
        Connection testConnection =
            OracleConnectionFactory.createOracleJdbcConnection(
                OracleJdbcConnectorConstants.ORACLE_JDBC_DRIVER_CLASS,
                url, userName, password, additionalProps);

        // Show the system time on each instance...
        if (showInstanceSysTimestamp) {
          LOG.info(String.format("\tDatabase time on %s is %s",
              instanceDescription, OracleQueries
                  .getSysTimeStamp(testConnection)));
        }

        testConnection.close();
        result = true;
      } catch (SQLException ex) {
        LOG.warn(
            String
                .format(
                    "The dynamically generated JDBC URL \"%s\" was unable to "
                    + "connect to an instance in the Oracle RAC.",
                    url), ex);
      }

      return result;
    }

    private void showUserTheOracleCommandToKillOraOop(MutableContext context) {

      String moduleName =
          OracleJdbcConnectorConstants.ORACLE_SESSION_MODULE_NAME;
      String actionName = context.getString(
          OracleJdbcConnectorConstants.ORACLE_SESSION_ACTION_NAME);

      String msg = String.format(
          "\nNote: This %s job can be killed via Oracle by executing the "
        + "following statement:\n\tbegin\n"
        + "\t\tfor row in (select sid,serial# from v$session where module='%s' "
        + "and action='%s') loop\n"
        + "\t\t\texecute immediate 'alter system kill session ''' || row.sid || "
        + "',' || row.serial# || '''';\n"
        + "\t\tend loop;\n" + "\tend;",
        OracleJdbcConnectorConstants.CONNECTOR_NAME, moduleName, actionName);
      LOG.info(msg);
    }
}
