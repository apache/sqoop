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

package org.apache.sqoop.manager.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.IncrementalMode;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ManagerFactory;
import com.cloudera.sqoop.metastore.JobData;

import org.apache.sqoop.manager.OracleManager;
import org.apache.sqoop.manager.oracle.OraOopOutputFormatUpdate.UpdateMode;
import org.apache.sqoop.manager.oracle.OraOopUtilities.
           JdbcOracleThinConnectionParsingError;

/**
 * OraOop manager - if OraOop cannot be used it should fall back to the default
 * OracleManager.
 *
 * To increase the amount of heap memory available to the mappers:
 *                                     -Dmapred.child.java.opts=-Xmx4000M
 * To prevent failed mapper tasks from being reattempted:
 *                                     -Dmapred.map.max.attempts=1
 */
public class OraOopManagerFactory extends ManagerFactory {

  @SuppressWarnings("unused")
  private static final OraOopLog ORAOOP_LOG = OraOopLogFactory
      .getLog("org.apache.sqoop.manager.oracle");
  private static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopManagerFactory.class.getName());

  static {
    Configuration
        .addDefaultResource(OraOopConstants.ORAOOP_SITE_TEMPLATE_FILENAME);
    Configuration.addDefaultResource(OraOopConstants.ORAOOP_SITE_FILENAME);
  }

  @Override
  public ConnManager accept(JobData jobData) {

    OraOopUtilities.enableDebugLoggingIfRequired(jobData.getSqoopOptions()
        .getConf());

    LOG.debug(String.format("%s can be called by Sqoop!",
        OraOopConstants.ORAOOP_PRODUCT_NAME));

    ConnManager result = null;

    if (jobData != null) {

      SqoopOptions sqoopOptions = jobData.getSqoopOptions();

      String connectString = sqoopOptions.getConnectString();
      if (connectString != null
          && connectString.toLowerCase().trim().startsWith("jdbc:oracle")) {

        if (!isOraOopEnabled(sqoopOptions)) {
          return result;
        }

        OraOopConnManager oraOopConnManager = null;

        OraOopConstants.Sqoop.Tool jobType = getSqoopJobType(jobData);
        OraOopUtilities.rememberSqoopJobType(jobType, jobData.getSqoopOptions()
            .getConf());

        List<OraOopLogMessage> messagesToDisplayAfterWelcome =
            new ArrayList<OraOopLogMessage>();

        switch (jobType) {

          case IMPORT:
            if (isNumberOfImportMappersOkay(sqoopOptions)
                && !isSqoopImportIncremental(jobData)
                && isSqoopImportJobTableBased(sqoopOptions)) {

              // At this stage, the Sqoop import job appears to be one we're
              // interested in accepting. We now need to connect to
              // the Oracle database to perform more tests...

              oraOopConnManager = new OraOopConnManager(sqoopOptions);

              try {
                Connection connection = oraOopConnManager.getConnection();

                if (isSqoopTableAnOracleTable(connection, sqoopOptions
                    .getUsername(),
                    oraOopConnManager.getOracleTableContext())) {

                  // OraOop will not accept responsibility for an Index
                  // Organized Table (IOT)...
                  if (!isSqoopTableAnIndexOrganizedTable(connection,
                      oraOopConnManager.getOracleTableContext())) {
                    result = oraOopConnManager; // <- OraOop accepts
                                                // responsibility for this Sqoop
                                                // job!
                  } else {
                    OraOopConstants.OraOopOracleDataChunkMethod method =
                        OraOopUtilities.getOraOopOracleDataChunkMethod(
                            sqoopOptions.getConf());
                    if (method == OraOopConstants.
                                      OraOopOracleDataChunkMethod.PARTITION) {
                      result = oraOopConnManager;
                    } else {
                      LOG.info(String.format("%s will not process this Sqoop"
                         + " connection, as the Oracle table %s is an"
                         + " index-organized table. If the table is"
                         + " partitioned, set "
                         + OraOopConstants.ORAOOP_ORACLE_DATA_CHUNK_METHOD
                         + " to "
                         + OraOopConstants.OraOopOracleDataChunkMethod.PARTITION
                         + ".",
                         OraOopConstants.ORAOOP_PRODUCT_NAME,
                         oraOopConnManager.getOracleTableContext().toString()));
                    }
                  }
                }
              } catch (SQLException ex) {
                throw new RuntimeException(String.format(
                    "Unable to connect to the Oracle database at %s\n"
                        + "Error:%s", sqoopOptions.getConnectString(), ex
                        .getMessage()), ex);
              }
            }
            break;

          case EXPORT:
            if (isNumberOfExportMappersOkay(sqoopOptions)) {

              // At this stage, the Sqoop export job appears to be one we're
              // interested in accepting. We now need to connect to
              // the Oracle database to perform more tests...

              oraOopConnManager = new OraOopConnManager(sqoopOptions);

              Connection connection = null;
              try {
                connection = oraOopConnManager.getConnection();
              } catch (SQLException ex) {
                throw new RuntimeException(String.format(
                    "Unable to connect to the Oracle database at %s\n"
                        + "Error:%s", sqoopOptions.getConnectString(), ex
                        .getMessage()), ex);
              }

              try {

                createAnyRequiredOracleObjects(sqoopOptions, connection,
                    oraOopConnManager, messagesToDisplayAfterWelcome);

                if (isSqoopTableAnOracleTable(connection, sqoopOptions
                    .getUsername(),
                    oraOopConnManager.getOracleTableContext())) {

                  result = oraOopConnManager; // <- OraOop accepts
                                              // responsibility for this Sqoop
                                              // job!
                }

              } catch (SQLException ex) {
                LOG.error(OraOopUtilities.getFullExceptionMessage(ex));
              }
            }

            break;
          default:
            // OraOop doesn't know how to handle other types of jobs - so won't
            // accept them.
            break;
        }

        // If OraOop has accepted this Sqoop job...
        if (result != null) {

          showUserTheOraOopWelcomeMessage();

          for (OraOopLogMessage message : messagesToDisplayAfterWelcome) {
            message.log(LOG);
          }

          // By the time we get into getSplits(), the number of mappers
          // stored in the config can be either 4 or 1 - so it seems
          // a bit unreliable. We'll use our own property name to ensure
          // getSplits() gets the correct value...
          sqoopOptions.getConf().setInt(
              OraOopConstants.ORAOOP_DESIRED_NUMBER_OF_MAPPERS,
              sqoopOptions.getNumMappers());

          // Generate the "action" name that we'll assign to our Oracle sessions
          // so that the user knows which Oracle sessions belong to OraOop...
          sqoopOptions.getConf().set(
              OraOopConstants.ORACLE_SESSION_ACTION_NAME,
              getOracleSessionActionName(jobData));

          OraOopUtilities.appendJavaSecurityEgd(sqoopOptions.getConf());

          // Get the Oracle database version...
          try {
            OracleVersion oracleVersion =
                OraOopOracleQueries.getOracleVersion(result.getConnection());
            LOG.info(String.format("Oracle Database version: %s",
                oracleVersion.getBanner()));
            sqoopOptions.getConf().setInt(
                OraOopConstants.ORAOOP_ORACLE_DATABASE_VERSION_MAJOR,
                oracleVersion.getMajor());
            sqoopOptions.getConf().setInt(
                OraOopConstants.ORAOOP_ORACLE_DATABASE_VERSION_MINOR,
                oracleVersion.getMinor());
          } catch (SQLException ex) {
            LOG.error("Unable to obtain the Oracle database version.", ex);
          }

          try {
            if (sqoopOptions.getConf().getBoolean(
                OraOopConstants.ORAOOP_IMPORT_CONSISTENT_READ, false)) {
              long scn =
                  sqoopOptions.getConf().getLong(
                      OraOopConstants.ORAOOP_IMPORT_CONSISTENT_READ_SCN, 0);
              if (scn == 0) {
                scn = OraOopOracleQueries.getCurrentScn(result.getConnection());
              }
              sqoopOptions.getConf().setLong(
                  OraOopConstants.ORAOOP_IMPORT_CONSISTENT_READ_SCN, scn);
              LOG.info("Performing a consistent read using SCN: " + scn);
            }
          } catch (SQLException ex) {
            throw new RuntimeException("Unable to determine SCN of database.",
                ex);
          }

          // Generate the JDBC URLs to be used by each mapper...
          setMapperConnectionDetails(oraOopConnManager, jobData);

          // Show the user the Oracle command that can be used to kill this
          // OraOop
          // job via Oracle...
          showUserTheOracleCommandToKillOraOop(sqoopOptions);
        }

      }
    }

    return result;
  }

  private void setMapperConnectionDetails(OraOopConnManager oraOopConnManager,
      JobData jobData) {

    // Ensure we have a connection to the database...
    Connection connection = null;
    try {
      connection = oraOopConnManager.getConnection();
    } catch (SQLException ex) {
      throw new RuntimeException(String.format(
          "Unable to connect to the Oracle database at %s\n" + "Error:%s",
          jobData.getSqoopOptions().getConnectString(), ex.getMessage()));
    }

    // Query v$active_instances to get a list of all instances in the Oracle RAC
    // (assuming this *could* be a RAC)...
    List<OracleActiveInstance> activeInstances = null;
    try {
      activeInstances =
          OraOopOracleQueries.getOracleActiveInstances(connection);
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
    if (OraOopUtilities.oracleJdbcUrlGenerationDisabled(jobData
        .getSqoopOptions().getConf())) {
      LOG.info(String
          .format(
              "%s will not use dynamically generated JDBC URLs - this feature "
            + "has been disabled.",
            OraOopConstants.ORAOOP_PRODUCT_NAME));
      return;
    }

    boolean generateRacBasedJdbcUrls = false;

    // Decide whether this is a multi-instance RAC, and whether we need to do
    // anything more...
    if (activeInstances != null) {
      generateRacBasedJdbcUrls = true;

      if (activeInstances.size() < OraOopUtilities
          .getMinNumberOfOracleRacActiveInstancesForDynamicJdbcUrlUse(jobData
              .getSqoopOptions().getConf())) {
        LOG.info(String.format(
            "There are only %d active instances in the Oracle RAC. "
          + "%s will not bother utilizing dynamically generated JDBC URLs.",
          activeInstances.size(), OraOopConstants.ORAOOP_PRODUCT_NAME));
        generateRacBasedJdbcUrls = false;
      }
    }

    // E.g. jdbc:oracle:thin:@localhost.localdomain:1521:orcl
    String jdbcConnectStr = jobData.getSqoopOptions().getConnectString();

    // Parse the JDBC URL to obtain the port number for the TNS listener...
    String jdbcHost = "";
    int jdbcPort = 0;
    String jdbcSid = "";
    String jdbcService = "";
    String jdbcTnsName = "";
    try {

      OraOopJdbcUrl oraOopJdbcUrl = new OraOopJdbcUrl(jdbcConnectStr);
      OraOopUtilities.JdbcOracleThinConnection jdbcConnection =
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
              jdbcConnectStr, OraOopConstants.ORAOOP_PRODUCT_NAME, ex));
      return;
    }

    if (generateRacBasedJdbcUrls) {

      // Retrieve the Oracle service name to use when connecting to the RAC...
      String oracleServiceName =
          OraOopUtilities.getOracleServiceName(jobData.getSqoopOptions()
              .getConf());

      // Generate JDBC URLs for each of the mappers...
      if (!oracleServiceName.isEmpty()) {
        if (!generateRacJdbcConnectionUrlsByServiceName(jdbcHost, jdbcPort,
            oracleServiceName, jobData)) {
          throw new RuntimeException(String.format(
              "Unable to connect to the Oracle database at %s "
                  + "via the service name \"%s\".", jobData.getSqoopOptions()
                  .getConnectString(), oracleServiceName));
        }
      } else {
        generateJdbcConnectionUrlsByActiveInstance(activeInstances, jdbcPort,
            jobData);
      }
    } else {
      generateJdbcConnectionUrlsByTnsnameSidOrService(jdbcHost, jdbcPort,
        jdbcSid, jdbcService, jdbcTnsName, jobData);
    }

  }

  private void generateJdbcConnectionUrlsByTnsnameSidOrService(String hostName,
    int port, String sid, String serviceName, String tnsName, JobData jobData) {

    String jdbcUrl = null;
    if (tnsName != null && !tnsName.isEmpty()) {
      jdbcUrl = OraOopUtilities.generateOracleTnsNameJdbcUrl(tnsName);
    } else if (sid != null && !sid.isEmpty()) {
      jdbcUrl = OraOopUtilities.generateOracleSidJdbcUrl(hostName, port, sid);
    } else {
      jdbcUrl =
          OraOopUtilities.generateOracleServiceNameJdbcUrl(hostName, port,
              serviceName);
    }

    // Now store these connection strings in such a way that each mapper knows
    // which one to use...
    for (int idxMapper = 0; idxMapper < jobData.getSqoopOptions()
        .getNumMappers(); idxMapper++) {
      storeJdbcUrlForMapper(idxMapper, jdbcUrl, jobData);
    }
  }

  private boolean generateRacJdbcConnectionUrlsByServiceName(String hostName,
      int port, String serviceName, JobData jobData) {

    boolean result = false;
    String jdbcUrl =
        OraOopUtilities.generateOracleServiceNameJdbcUrl(hostName, port,
            serviceName);

    if (testDynamicallyGeneratedOracleRacInstanceConnection(jdbcUrl, jobData
        .getSqoopOptions().getUsername(), jobData.getSqoopOptions()
        .getPassword(), jobData.getSqoopOptions().getConnectionParams()
        , false // <- ShowInstanceSysTimestamp
        , "" // <- instanceDescription
    )) {

      LOG.info(String.format(
          "%s will load-balance sessions across the Oracle RAC instances "
              + "by connecting each mapper to the Oracle Service \"%s\".",
          OraOopConstants.ORAOOP_PRODUCT_NAME, serviceName));

      // Now store these connection strings in such a way that each mapper knows
      // which one to use...
      for (int idxMapper = 0; idxMapper < jobData.getSqoopOptions()
          .getNumMappers(); idxMapper++) {
        storeJdbcUrlForMapper(idxMapper, jdbcUrl, jobData);
      }
      result = true;
    }
    return result;
  }

  private void
      generateJdbcConnectionUrlsByActiveInstance(
          List<OracleActiveInstance> activeInstances, int jdbcPort,
          JobData jobData) {

    // Generate JDBC URLs for each of the instances in the RAC...
    ArrayList<OraOopUtilities.JdbcOracleThinConnection>
        jdbcOracleActiveThinConnections =
            new ArrayList<OraOopUtilities.JdbcOracleThinConnection>(
                activeInstances.size());

    for (OracleActiveInstance activeInstance : activeInstances) {

      OraOopUtilities.JdbcOracleThinConnection
          jdbcActiveInstanceThinConnection =
              new OraOopUtilities.JdbcOracleThinConnection(
                  activeInstance.getHostName(),
                  jdbcPort, activeInstance.getInstanceName(), "", "");

      if (testDynamicallyGeneratedOracleRacInstanceConnection(
          jdbcActiveInstanceThinConnection.toString(), jobData
              .getSqoopOptions().getUsername(), jobData.getSqoopOptions()
              .getPassword(), jobData.getSqoopOptions().getConnectionParams(),
          true, activeInstance.getInstanceName())) {
        jdbcOracleActiveThinConnections.add(jdbcActiveInstanceThinConnection);
      }
    }

    // If there are multiple JDBC URLs that work okay for the RAC, then we'll
    // make use of them...
    if (jdbcOracleActiveThinConnections.size() < OraOopUtilities
        .getMinNumberOfOracleRacActiveInstancesForDynamicJdbcUrlUse(jobData
            .getSqoopOptions().getConf())) {
      LOG.info(String
          .format(
              "%s will not attempt to load-balance sessions across instances "
            + "of an Oracle RAC - as multiple JDBC URLs to the "
            + "Oracle RAC could not be dynamically generated.",
              OraOopConstants.ORAOOP_PRODUCT_NAME));
      return;
    } else {
      StringBuilder msg = new StringBuilder();
      msg.append(String
          .format(
              "%s will load-balance sessions across the following instances of"
            + "the Oracle RAC:\n",
              OraOopConstants.ORAOOP_PRODUCT_NAME));

      for (OraOopUtilities.JdbcOracleThinConnection thinConnection
               : jdbcOracleActiveThinConnections) {
        msg.append(String.format("\tInstance: %s \t URL: %s\n",
            thinConnection.getSid(), thinConnection.toString()));
      }
      LOG.info(msg.toString());
    }

    // Now store these connection strings in such a way that each mapper knows
    // which one to use...
    int racInstanceIdx = 0;
    OraOopUtilities.JdbcOracleThinConnection thinUrl;
    for (int idxMapper = 0; idxMapper < jobData.getSqoopOptions()
        .getNumMappers(); idxMapper++) {
      if (racInstanceIdx > jdbcOracleActiveThinConnections.size() - 1) {
        racInstanceIdx = 0;
      }
      thinUrl = jdbcOracleActiveThinConnections.get(racInstanceIdx);
      racInstanceIdx++;
      storeJdbcUrlForMapper(idxMapper, thinUrl.toString(), jobData);
    }
  }

  private boolean testDynamicallyGeneratedOracleRacInstanceConnection(
      String url, String userName, String password, Properties additionalProps,
      boolean showInstanceSysTimestamp, String instanceDescription) {

    boolean result = false;

    // Test the connection...
    try {
      Connection testConnection =
          OracleConnectionFactory.createOracleJdbcConnection(
              OraOopConstants.ORACLE_JDBC_DRIVER_CLASS, url, userName,
              password, additionalProps);

      // Show the system time on each instance...
      if (showInstanceSysTimestamp) {
        LOG.info(String.format("\tDatabase time on %s is %s",
            instanceDescription, OraOopOracleQueries
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

  private void storeJdbcUrlForMapper(int mapperIdx, String jdbcUrl,
      JobData jobData) {

    // Now store these connection strings in such a way that each mapper knows
    // which one to use...
    Configuration conf = jobData.getSqoopOptions().getConf();
    String mapperJdbcUrlPropertyName =
        OraOopUtilities.getMapperJdbcUrlPropertyName(mapperIdx, conf);
    LOG.debug("Setting mapper url " + mapperJdbcUrlPropertyName + " = "
      + jdbcUrl);
    conf.set(mapperJdbcUrlPropertyName, jdbcUrl);
  }

  private boolean isOraOopEnabled(SqoopOptions sqoopOptions) {

    String oraOopDisabled =
        sqoopOptions.getConf().get(OraOopConstants.ORAOOP_DISABLED, "false")
            .toLowerCase();
    boolean oraOopIsDisabled =
        oraOopDisabled.equalsIgnoreCase("true")
            || oraOopDisabled.equalsIgnoreCase("yes")
            || oraOopDisabled.equalsIgnoreCase("y")
            || oraOopDisabled.equalsIgnoreCase("1");

    oraOopIsDisabled = oraOopIsDisabled || !sqoopOptions.isDirect();

    if (oraOopIsDisabled) {
      LOG.info(String.format("%s is disabled.",
          OraOopConstants.ORAOOP_PRODUCT_NAME));
    }

    return !oraOopIsDisabled;
  }

  private OraOopConstants.Sqoop.Tool getSqoopJobType(JobData jobData) {

    OraOopConstants.Sqoop.Tool result = OraOopConstants.Sqoop.Tool.UNKNOWN;

    String sqoopToolName = getSqoopToolName(jobData).toUpperCase().trim();
    try {
      result = OraOopConstants.Sqoop.Tool.valueOf(sqoopToolName);
    } catch (IllegalArgumentException ex) {
      LOG.debug(String.format(
          "The Sqoop tool name \"%s\" is not supported by OraOop",
          sqoopToolName), ex);
    }
    return result;
  }

  private boolean isNumberOfImportMappersOkay(SqoopOptions sqoopOptions) {

    // Check whether there are enough mappers for OraOop to be of benefit...
    boolean result =
      (sqoopOptions.getNumMappers() >= OraOopUtilities
          .getMinNumberOfImportMappersAcceptedByOraOop(sqoopOptions.getConf()));

    if (!result) {
      LOG.info(String.format(
        "%s will not process this sqoop connection, as an insufficient number "
            + "of mappers are being used.",
        OraOopConstants.ORAOOP_PRODUCT_NAME));
    }

    return result;
  }

  private boolean isNumberOfExportMappersOkay(SqoopOptions sqoopOptions) {

    // Check whether there are enough mappers for OraOop to be of benefit...
    boolean result =
        (sqoopOptions.getNumMappers() >= OraOopUtilities
          .getMinNumberOfExportMappersAcceptedByOraOop(sqoopOptions.getConf()));

    if (!result) {
      LOG.info(String.format(
        "%s will not process this sqoop connection, as an insufficient number "
            + "of mappers are being used.",
        OraOopConstants.ORAOOP_PRODUCT_NAME));
    }

    return result;
  }

  private boolean isSqoopImportJobTableBased(SqoopOptions sqoopOptions) {

    String tableName = sqoopOptions.getTableName();
    return (tableName != null && !tableName.isEmpty());
  }

  private boolean isSqoopTableAnOracleTable(Connection connection,
      String connectionUserName, OracleTable tableContext) {

    String oracleObjectType;

    try {

      // Find the table via dba_tables...
      OracleTable oracleTable =
          OraOopOracleQueries.getTable(connection, tableContext.getSchema(),
              tableContext.getName());
      if (oracleTable != null) {
        return true;
      }

      // If we could not find the table via dba_tables, then try and determine
      // what type of database object the
      // user was referring to. Perhaps they've specified the name of a view?...
      oracleObjectType =
          OraOopOracleQueries.getOracleObjectType(connection, tableContext);

      if (oracleObjectType == null) {
        LOG.info(String.format(
            "%1$s will not process this Sqoop connection, "
          + "as the Oracle user %2$s does not own a table named %3$s.\n"
          + "\tPlease prefix the table name with the owner.\n "
          + "\tNote: You may need to double-quote the owner and/or table name."
          + "\n\tE.g. sqoop ... --username %4$s --table %2$s.%3$s\n",
          OraOopConstants.ORAOOP_PRODUCT_NAME, tableContext.getSchema(),
          tableContext.getName(), connectionUserName));
        return false;
      }

    } catch (SQLException ex) {
      LOG.warn(String.format(
        "Unable to determine the Oracle-type of the object named %s owned by "
            + "%s.\nError:\n" + "%s", tableContext.getName(), tableContext
            .getSchema(), ex.getMessage()));

      // In the absence of conflicting information, let's assume the object is
      // actually a table...
      return true;
    }

    boolean result =
        oracleObjectType
            .equalsIgnoreCase(OraOopConstants.Oracle.OBJECT_TYPE_TABLE);

    if (!result) {
      LOG.info(String.format("%s will not process this sqoop connection, "
          + "as %s is not an Oracle table, it's a %s.",
          OraOopConstants.ORAOOP_PRODUCT_NAME, tableContext.toString(),
          oracleObjectType));
    }

    return result;
  }

  private boolean isSqoopTableAnIndexOrganizedTable(Connection connection,
      OracleTable tableContext) {

    boolean result = false;
    try {
      result =
          OraOopOracleQueries.isTableAnIndexOrganizedTable(connection,
              tableContext);
      return result;
    } catch (SQLException ex) {
      LOG.warn(String.format(
         "Unable to determine whether the Oracle table %s is an index-organized"
       + " table.\nError:\n" + "%s", tableContext.toString(), ex.getMessage()));
    }
    return result;
  }

  private String getSqoopToolName(JobData jobData) {

    return jobData.getSqoopTool().getToolName();
  }

  private String getOracleSessionActionName(JobData jobData) {

    // This method has been written assuming that:
    // (1) OraOop only processes Sqoop "import" and "export" jobs; and
    // (2) a table will be used during the import/export (not a query).

    if (getSqoopJobType(jobData) != OraOopConstants.Sqoop.Tool.IMPORT
        && getSqoopJobType(jobData) != OraOopConstants.Sqoop.Tool.EXPORT) {
      throw new UnsupportedOperationException(String.format(
          "%s needs to be updated to cope " + "with Sqoop jobs of type %s.",
          OraOopUtilities.getCurrentMethodName(), getSqoopToolName(jobData)));
    }

    String timeStr =
        (new SimpleDateFormat("yyyyMMddHHmmsszzz")).format(new Date());

    String result = String.format("%s %s", getSqoopToolName(jobData), timeStr);

    // NOTE: The "action" column of v$session is only a 32 character column.
    // Therefore we need to ensure that the string returned by this
    // method does not exceed 32 characters...
    if (result.length() > 32) {
      result = result.substring(0, 32).trim();
    }

    return result;
  }

  private boolean isSqoopImportIncremental(JobData jobData) {

    boolean result =
        jobData.getSqoopOptions().getIncrementalMode() != IncrementalMode.None;

    if (result) {
      LOG.info(String.format("%1$s will not process this sqoop connection, "
          + "as incremental mode is not supported by %1$s.",
          OraOopConstants.ORAOOP_PRODUCT_NAME));
    }

    return result;
  }

  private void showUserTheOraOopWelcomeMessage() {

    String msg1 =
        String.format("Using %s", OraOopConstants.ORAOOP_PRODUCT_NAME);

    int longestMessage = msg1.length();

    msg1 = OraOopUtilities.padRight(msg1, longestMessage);

    char[] asterisks = new char[longestMessage + 8];
    Arrays.fill(asterisks, '*');

    String msg =
        String.format("\n" + "%1$s\n" + "*** %2$s ***\n" + "%1$s", new String(
            asterisks), msg1);
    LOG.info(msg);
  }

  private void showUserTheOracleCommandToKillOraOop(SqoopOptions sqoopOptions) {

    int taskAttempts =
        sqoopOptions.getConf().getInt(
            OraOopConstants.Sqoop.MAX_MAPREDUCE_ATTEMPTS, -1);

    // If killing the Oracle session if futile - because the job will be
    // reattempted, then don't
    // bother telling the user about this feature...
    if (taskAttempts != 1) {
      return;
    }

    String moduleName = OraOopConstants.ORACLE_SESSION_MODULE_NAME;
    String actionName =
        sqoopOptions.getConf().get(OraOopConstants.ORACLE_SESSION_ACTION_NAME);

    String msg = String.format(
        "\nNote: This %s job can be killed via Oracle by executing the "
      + "following statement:\n\tbegin\n"
      + "\t\tfor row in (select sid,serial# from v$session where module='%s' "
      + "and action='%s') loop\n"
      + "\t\t\texecute immediate 'alter system kill session ''' || row.sid || "
      + "',' || row.serial# || '''';\n"
      + "\t\tend loop;\n" + "\tend;",
      OraOopConstants.ORAOOP_PRODUCT_NAME, moduleName, actionName);
    LOG.info(msg);
  }

  private void createAnyRequiredOracleObjects(SqoopOptions sqoopOptions,
    Connection connection, OraOopConnManager oraOopConnManager,
    List<OraOopLogMessage> messagesToDisplayAfterWelcome) throws SQLException {

    Configuration conf = sqoopOptions.getConf();

    // The SYSDATE on the Oracle database will be used as the partition value
    // for this export job...
    Object sysDateTime = OraOopOracleQueries.getSysDate(connection);
    String sysDateStr =
      OraOopOracleQueries.oraDATEToString(sysDateTime, "yyyy-mm-dd hh24:mi:ss");
    OraOopUtilities.rememberOracleDateTime(conf,
        OraOopConstants.ORAOOP_JOB_SYSDATE, sysDateStr);

    checkForOldOraOopTemporaryOracleTables(connection, sysDateTime,
        OraOopOracleQueries.getCurrentSchema(connection),
        messagesToDisplayAfterWelcome);

    // Store the actual partition value, so the N mappers know what value to
    // insert...
    String partitionValue =
        OraOopOracleQueries.oraDATEToString(sysDateTime,
            OraOopConstants.ORAOOP_EXPORT_PARTITION_DATE_FORMAT);
    conf.set(OraOopConstants.ORAOOP_EXPORT_PARTITION_DATE_VALUE,
             partitionValue);

    // Generate the (22 character) partition name...
    String partitionName =
        OraOopUtilities
            .createExportTablePartitionNameFromOracleTimestamp(sysDateTime);

    int numMappers = sqoopOptions.getNumMappers();

    String exportTableTemplate =
        conf.get(OraOopConstants.ORAOOP_EXPORT_CREATE_TABLE_TEMPLATE, "");

    String user = sqoopOptions.getUsername();
    if (user == null) {
      user = OracleManager.getSessionUser(connection);
    }

    OracleTable templateTableContext =
        OraOopUtilities.decodeOracleTableName(user, exportTableTemplate);

    boolean noLoggingOnNewTable =
        conf.getBoolean(OraOopConstants.ORAOOP_EXPORT_CREATE_TABLE_NO_LOGGING,
            false);

    String updateKeyCol = sqoopOptions.getUpdateKeyCol();

    /* =========================== */
    /* VALIDATION OF INPUTS */
    /* =========================== */

    if (updateKeyCol == null || updateKeyCol.isEmpty()) {
      // We're performing an "insert" export, not an "update" export.

      // Check that the "oraoop.export.merge" property has not been specified,
      // as this would be
      // an invalid scenario...
      if (OraOopUtilities.getExportUpdateMode(conf) == UpdateMode.Merge) {
        throw new RuntimeException(String.format(
            "\n\nThe option \"%s\" can only be used if \"%s\" is "
                + "also used.\n", OraOopConstants.ORAOOP_EXPORT_MERGE,
            "--update-key"));
      }
    }

    if (OraOopUtilities
        .userWantsToCreatePartitionedExportTableFromTemplate(conf)
        || OraOopUtilities
            .userWantsToCreateNonPartitionedExportTableFromTemplate(conf)) {

      // OraOop will create the export table.

      if (oraOopConnManager.getOracleTableContext().getName().length()
              > OraOopConstants.Oracle.MAX_IDENTIFIER_LENGTH) {
        String msg =
            String.format(
                "The Oracle table name \"%s\" is longer than %d characters.\n"
              + "Oracle will not allow a table with this name to be created.",
            oraOopConnManager.getOracleTableContext().getName(),
            OraOopConstants.Oracle.MAX_IDENTIFIER_LENGTH);
        throw new RuntimeException(msg);
      }

      if (updateKeyCol != null && !updateKeyCol.isEmpty()) {

        // We're performing an "update" export, not an "insert" export.

        // Check whether the user is attempting an "update" (i.e. a non-merge).
        // If so, they're
        // asking to only UPDATE rows in a (about to be created) (empty) table
        // that contains no rows.
        // This will be a waste of time, as we'd be attempting to perform UPDATE
        // operations against a
        // table with no rows in it...
        UpdateMode updateMode = OraOopUtilities.getExportUpdateMode(conf);
        if (updateMode == UpdateMode.Update) {
          throw new RuntimeException(String.format(
              "\n\nCombining the option \"%s\" with the option \"%s=false\" is "
            + "nonsensical, as this would create an "
            + "empty table and then perform "
            + "a lot of work that results in a table containing no rows.\n",
              OraOopConstants.ORAOOP_EXPORT_CREATE_TABLE_TEMPLATE,
              OraOopConstants.ORAOOP_EXPORT_MERGE));
        }
      }

      // Check that the specified template table actually exists and is a
      // table...
      String templateTableObjectType =
          OraOopOracleQueries.getOracleObjectType(connection,
              templateTableContext);
      if (templateTableObjectType == null) {
        throw new RuntimeException(String.format(
            "The specified Oracle template table \"%s\" does not exist.",
            templateTableContext.toString()));
      }

      if (!templateTableObjectType
          .equalsIgnoreCase(OraOopConstants.Oracle.OBJECT_TYPE_TABLE)) {
        throw new RuntimeException(
          String.format(
              "The specified Oracle template table \"%s\" is not an "
            + "Oracle table, it's a %s.",
              templateTableContext.toString(), templateTableObjectType));
      }

      if (conf.getBoolean(OraOopConstants.ORAOOP_EXPORT_CREATE_TABLE_DROP,
          false)) {
        OraOopOracleQueries.dropTable(connection, oraOopConnManager
            .getOracleTableContext());
      }

      // Check that there is no existing database object with the same name of
      // the table to be created...
      String newTableObjectType =
          OraOopOracleQueries.getOracleObjectType(connection, oraOopConnManager
              .getOracleTableContext());
      if (newTableObjectType != null) {
        throw new RuntimeException(
          String.format(
              "%s cannot create a new Oracle table named %s as a \"%s\" "
            + "with this name already exists.",
            OraOopConstants.ORAOOP_PRODUCT_NAME, oraOopConnManager
                .getOracleTableContext().toString(), newTableObjectType));
      }
    } else {
      // The export table already exists.

      if (updateKeyCol != null && !updateKeyCol.isEmpty()) {

        // We're performing an "update" export, not an "insert" export.

        // Check that there exists an index on the export table on the
        // update-key column(s).
        // Without such an index, this export may perform like a real dog...
        String[] updateKeyColumns =
            OraOopUtilities.getExportUpdateKeyColumnNames(sqoopOptions);
        if (!OraOopOracleQueries.doesIndexOnColumnsExist(connection,
            oraOopConnManager.getOracleTableContext(), updateKeyColumns)) {
          String msg = String.format(
              "\n**************************************************************"
            + "***************************************************************"
            + "\n\tThe table %1$s does not have a valid index on "
            + "the column(s) %2$s.\n"
            + "\tAs a consequence, this export may take a long time to "
            + "complete.\n"
            + "\tIf performance is unacceptable, consider reattempting this "
            + "job after creating an index "
            + "on this table via the SQL...\n"
            + "\t\tcreate index <index_name> on %1$s(%2$s);\n"
            + "****************************************************************"
            + "*************************************************************",
                      oraOopConnManager.getOracleTableContext().toString(),
                      OraOopUtilities.stringArrayToCSV(updateKeyColumns));
          messagesToDisplayAfterWelcome.add(new OraOopLogMessage(
              OraOopConstants.Logging.Level.WARN, msg));
        }
      }
    }

    /* ================================= */
    /* CREATE A PARTITIONED TABLE */
    /* ================================= */
    if (OraOopUtilities
        .userWantsToCreatePartitionedExportTableFromTemplate(conf)) {

      // Create a new Oracle table using the specified template...

      String[] subPartitionNames =
          OraOopUtilities.generateExportTableSubPartitionNames(numMappers,
              sysDateTime, conf);
      // Create the export table from a template table...
      String tableStorageClause =
          OraOopUtilities.getExportTableStorageClause(conf);

      OraOopOracleQueries.createExportTableFromTemplateWithPartitioning(
          connection, oraOopConnManager.getOracleTableContext(),
          tableStorageClause, templateTableContext, noLoggingOnNewTable,
          partitionName, sysDateTime, sqoopOptions.getNumMappers(),
          subPartitionNames);
      return;
    }

    /* ===================================== */
    /* CREATE A NON-PARTITIONED TABLE */
    /* ===================================== */
    if (OraOopUtilities
        .userWantsToCreateNonPartitionedExportTableFromTemplate(conf)) {

      String tableStorageClause =
          OraOopUtilities.getExportTableStorageClause(conf);

      OraOopOracleQueries.createExportTableFromTemplate(connection,
          oraOopConnManager.getOracleTableContext(), tableStorageClause,
          templateTableContext, noLoggingOnNewTable);
      return;
    }

    /* ===================================================== */
    /* ADD ADDITIONAL PARTITIONS TO AN EXISTING TABLE */
    /* ===================================================== */

    // If the export table is partitioned, and the partitions were created by
    // OraOop, then we need
    // create additional partitions...

    OracleTablePartitions tablePartitions =
        OraOopOracleQueries.getPartitions(connection, oraOopConnManager
            .getOracleTableContext());
    // Find any partition name starting with "ORAOOP_"...
    OracleTablePartition oraOopPartition =
        tablePartitions.findPartitionByRegEx("^"
            + OraOopConstants.EXPORT_TABLE_PARTITION_NAME_PREFIX);

    if (tablePartitions.size() > 0 && oraOopPartition == null) {

      for (int idx = 0; idx < tablePartitions.size(); idx++) {
        messagesToDisplayAfterWelcome.add(new OraOopLogMessage(
            OraOopConstants.Logging.Level.INFO, String.format(
                "The Oracle table %s has a partition named \"%s\".",
                oraOopConnManager.getOracleTableContext().toString(),
                tablePartitions.get(idx).getName())));
      }

      messagesToDisplayAfterWelcome.add(new OraOopLogMessage(
          OraOopConstants.Logging.Level.WARN, String.format(
              "The Oracle table %s is partitioned.\n"
                  + "These partitions were not created by %s.",
              oraOopConnManager.getOracleTableContext().toString(),
              OraOopConstants.ORAOOP_PRODUCT_NAME)));
    }

    if (oraOopPartition != null) {

      // Indicate in the configuration what's happening...
      conf.setBoolean(OraOopConstants.EXPORT_TABLE_HAS_ORAOOP_PARTITIONS, true);

      messagesToDisplayAfterWelcome
          .add(new OraOopLogMessage(
              OraOopConstants.Logging.Level.INFO,
              String
                  .format(
                      "The Oracle table %s is partitioned.\n"
                          + "These partitions were created by %s, so "
                          + "additional partitions will now be created.\n"
                          + "The name of the new partition will be \"%s\".",
                      oraOopConnManager.getOracleTableContext().toString(),
                      OraOopConstants.ORAOOP_PRODUCT_NAME, partitionName)));

      String[] subPartitionNames =
          OraOopUtilities.generateExportTableSubPartitionNames(numMappers,
              sysDateTime, conf);

      // Add another partition (and N subpartitions) to this existing,
      // partitioned export table...
      OraOopOracleQueries.createMoreExportTablePartitions(connection,
          oraOopConnManager.getOracleTableContext(), partitionName,
          sysDateTime, subPartitionNames);

      return;
    }
  }

  private void checkForOldOraOopTemporaryOracleTables(Connection connection,
      Object sysDateTime, String schema,
      List<OraOopLogMessage> messagesToDisplayAfterWelcome) {

    try {

      StringBuilder message = new StringBuilder();
      message
        .append(String.format(
          "The following tables appear to be old temporary tables created by "
        + "%s that have not been deleted.\n"
        + "They are probably left over from jobs that encountered an error and "
        + "could not clean up after themselves.\n"
        + "You might want to drop these Oracle tables in order to reclaim "
        + "Oracle storage space:\n", OraOopConstants.ORAOOP_PRODUCT_NAME));
      boolean showMessage = false;

      String generatedTableName =
          OraOopUtilities.generateExportTableMapperTableName(0, sysDateTime,
              schema).getName();
      generatedTableName = generatedTableName.replaceAll("[0-9]", "%");
      generatedTableName =
          OraOopUtilities.replaceAll(generatedTableName, "%%", "%");
      Date sysDate = OraOopOracleQueries.oraDATEToDate(sysDateTime);

      List<OracleTable> tables =
          OraOopOracleQueries.getTablesWithTableNameLike(connection, schema,
              generatedTableName);

      for (OracleTable oracleTable : tables) {
        OraOopUtilities.DecodedExportMapperTableName tableName =
            OraOopUtilities.decodeExportTableMapperTableName(oracleTable);
        if (tableName != null) {
          Date tableDate =
              OraOopOracleQueries.oraDATEToDate(tableName.getTableDateTime());
          double daysApart =
              (sysDate.getTime() - tableDate.getTime()) / (1000 * 60 * 60 * 24);
          if (daysApart > 1.0) {
            showMessage = true;
            message.append(String.format("\t%s\n", oracleTable.toString()));
          }
        }
      }

      if (showMessage) {
        messagesToDisplayAfterWelcome.add(new OraOopLogMessage(
            OraOopConstants.Logging.Level.INFO, message.toString()));
      }
    } catch (Exception ex) {
      messagesToDisplayAfterWelcome.add(new OraOopLogMessage(
          OraOopConstants.Logging.Level.WARN, String.format(
              "%s was unable to check for the existance of old "
                  + "temporary Oracle tables.\n" + "Error:\n%s",
              OraOopConstants.ORAOOP_PRODUCT_NAME, ex.toString())));
    }
  }
}
