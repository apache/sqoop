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

package org.apache.sqoop.connector.jdbc.oracle.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnectorConstants;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ConnectionConfig;
import org.apache.sqoop.connector.jdbc.oracle.configuration.ToJobConfig;

/**
 * Utilities used by OraOop.
 */
public final class OracleUtilities {

  private static final Logger LOG = Logger.getLogger(OracleUtilities.class);

  private OracleUtilities() {
  }

  private static String currentSessionUser = null;

  /**
   * Type of export - straight update or merge (update-insert).
   */
  public enum UpdateMode {
    Update, Merge
  }

  /**
   * Type of insert to use - direct or partition exchange load.
   */
  public enum InsertMode {
    DirectInsert, ExchangePartition
  }

  public enum ExportTableUpdateTechnique {
    ReInsertUpdatedRows, ReInsertUpdatedRowsAndNewRows, UpdateSql, MergeSql
  }

  /**
   * How should data be split up - by ROWID range, or by partition.
   */
  public enum OracleDataChunkMethod {
    ROWID, PARTITION
  }

  /**
  * How splits should be allocated to the mappers.
  */
  public enum OracleBlockToSplitAllocationMethod {
    ROUNDROBIN, SEQUENTIAL, RANDOM
  }

  /**
   * Location to place the WHERE clause.
   */
  public enum OracleTableImportWhereClauseLocation {
    SUBSPLIT, SPLIT
  }

  /**
   * Whether to use the append values hint for exports.
   */
  public enum AppendValuesHintUsage {
    AUTO, ON, OFF
  }

//  /**
//   * Used for testing purposes - can get OraOop to call a class to run a report
//   * on various performance metrics.
//   */
//  public static class OraOopStatsReports {
//    private String csvReport;
//    private String performanceReport;
//
//    public String getPerformanceReport() {
//      return performanceReport;
//    }
//
//    public void setPerformanceReport(String newPerformanceReport) {
//      this.performanceReport = newPerformanceReport;
//    }
//
//    public String getCsvReport() {
//      return csvReport;
//    }
//
//    public void setCsvReport(String newCsvReport) {
//      this.csvReport = newCsvReport;
//    }
//  }
//
//
//
//  public static List<String> copyStringList(List<String> list) {
//
//    List<String> result = new ArrayList<String>(list.size());
//    result.addAll(list);
//    return result;
//  }

  public static OracleTable decodeOracleTableName(
      String oracleConnectionUserName, String tableStr) {

    String tableOwner;
    String tableName;

    // These are the possibilities for double-quote location...
    // table
    // "table"
    // schema.table
    // schema."table"
    // "schema".table
    // "schema"."table"
    String[] tableStrings = tableStr.split("\"");

    if (oracleConnectionUserName == null) {
      oracleConnectionUserName = currentSessionUser;
    }

    switch (tableStrings.length) {

      case 1: // <- table or schema.table

        tableStrings = tableStr.split("\\.");

        switch (tableStrings.length) {

          case 1: // <- No period
            tableOwner = oracleConnectionUserName.toUpperCase();
            tableName = tableStrings[0].toUpperCase();
            break;
          case 2: // <- 1 period
            tableOwner = tableStrings[0].toUpperCase();
            tableName = tableStrings[1].toUpperCase();
            break;
          default:
            LOG.debug(String.format(
                "Unable to decode the table name (displayed in "
                    + "double quotes): \"%s\"", tableStr));
            throw new RuntimeException(String.format(
                "Unable to decode the table name: %s", tableStr));
        }
        break;

      case 2: // <- "table" or schema."table"

        if (tableStrings[0] == null || tableStrings[0].isEmpty()) {
          tableOwner = oracleConnectionUserName.toUpperCase();
        } else {
          tableOwner = tableStrings[0].toUpperCase();
          // Remove the "." from the end of the schema name...
          if (tableOwner.endsWith(".")) {
            tableOwner = tableOwner.substring(0, tableOwner.length() - 1);
          }
        }

        tableName = tableStrings[1];
        break;

      case 3: // <- "schema".table

        tableOwner = tableStrings[1];
        tableName = tableStrings[2].toUpperCase();
        // Remove the "." from the start of the table name...
        if (tableName.startsWith(".")) {
          tableName = tableName.substring(1, tableName.length());
        }

        break;

      case 4: // <- "schema"."table"
        tableOwner = tableStrings[1];
        tableName = tableStrings[3];
        break;

      default:
        LOG.debug(String.format(
            "Unable to decode the table name (displayed in double "
                + "quotes): \"%s\"", tableStr));
        throw new RuntimeException(String.format(
            "Unable to decode the table name: %s", tableStr));

    }
    OracleTable result = new OracleTable(tableOwner, tableName);
    return result;
  }


  public static boolean oracleJdbcUrlGenerationDisabled(ConnectionConfig config) {
    return BooleanUtils.isTrue(config.jdbcUrlVerbatim);
  }

//  public static boolean userWantsOracleSessionStatisticsReports(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.getBoolean(OraOopConstants.ORAOOP_REPORT_SESSION_STATISTICS,
//        false);
//  }
//
//  public static boolean enableDebugLoggingIfRequired(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    boolean result = false;
//
//    try {
//
//      Level desiredOraOopLoggingLevel =
//          Level.toLevel(conf.get(OraOopConstants.ORAOOP_LOGGING_LEVEL),
//              Level.INFO);
//
//      Level sqoopLogLevel =
//          Logger.getLogger(Sqoop.class.getName()).getParent().getLevel();
//
//      if (desiredOraOopLoggingLevel == Level.DEBUG
//          || desiredOraOopLoggingLevel == Level.ALL
//          || sqoopLogLevel == Level.DEBUG || sqoopLogLevel == Level.ALL) {
//
//        Category oraOopLogger =
//            Logger.getLogger(OraOopManagerFactory.class.getName()).getParent();
//        oraOopLogger.setLevel(Level.DEBUG);
//        LOG.debug("Enabled OraOop debug logging.");
//        result = true;
//
//        conf.set(OraOopConstants.ORAOOP_LOGGING_LEVEL, Level.DEBUG.toString());
//      }
//    } catch (Exception ex) {
//      LOG.error(String.format(
//          "Unable to determine whether debug logging should be enabled.\n%s",
//          getFullExceptionMessage(ex)));
//    }
//
//    return result;
//  }
//
  public static String generateDataChunkId(int fileId, int fileBatch) {
    StringBuilder sb = new StringBuilder();
    return sb.append(fileId).append("_").append(fileBatch).toString();
  }

  public static String getCurrentMethodName() {

    StackTraceElement[] stackTraceElements = (new Throwable()).getStackTrace();
    return String.format("%s()", stackTraceElements[1].getMethodName());
  }

  public static String[] getDuplicatedStringArrayValues(String[] list,
      boolean ignoreCase) {

    if (list == null) {
      throw new IllegalArgumentException("The list argument cannot be null");
    }

    ArrayList<String> duplicates = new ArrayList<String>();

    for (int idx1 = 0; idx1 < list.length - 1; idx1++) {
      for (int idx2 = idx1 + 1; idx2 < list.length; idx2++) {
        if (list[idx1].equals(list[idx2])) {
          // If c is a duplicate of both a & b, don't add c to the list twice...
          if (!duplicates.contains(list[idx2])) {
            duplicates.add(list[idx2]);
          }

        } else if (ignoreCase && list[idx1].equalsIgnoreCase((list[idx2]))) {
          // If c is a duplicate of both a & b, don't add c to the list twice...
          if (stringListIndexOf(duplicates, list[idx2], ignoreCase) == -1) {
            duplicates.add(list[idx2]);
          }
        }
      }
    }

    return duplicates.toArray(new String[duplicates.size()]);
  }

  public static String getFullExceptionMessage(Exception ex) {

    ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(arrayStream);
    ex.printStackTrace(printStream);
    return arrayStream.toString();
  }

//  public static int getMinNumberOfImportMappersAcceptedByOraOop(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.getInt(OraOopConstants.ORAOOP_MIN_IMPORT_MAPPERS,
//        OraOopConstants.MIN_NUM_IMPORT_MAPPERS_ACCEPTED_BY_ORAOOP);
//  }
//
//  public static int getMinAppendValuesBatchSize(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.getInt(OraOopConstants.ORAOOP_MIN_APPEND_VALUES_BATCH_SIZE,
//        OraOopConstants.ORAOOP_MIN_APPEND_VALUES_BATCH_SIZE_DEFAULT);
//  }
//
//  public static int getMinNumberOfExportMappersAcceptedByOraOop(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.getInt(OraOopConstants.ORAOOP_MIN_EXPORT_MAPPERS,
//        OraOopConstants.MIN_NUM_EXPORT_MAPPERS_ACCEPTED_BY_ORAOOP);
//  }
//
//  public static int getMinNumberOfOracleRacActiveInstancesForDynamicJdbcUrlUse(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.getInt(OraOopConstants.ORAOOP_MIN_RAC_ACTIVE_INSTANCES,
//        OraOopConstants.MIN_NUM_RAC_ACTIVE_INSTANCES_FOR_DYNAMIC_JDBC_URLS);
//  }
//
//  public static int getNumberOfDataChunksPerOracleDataFile(
//      int desiredNumberOfMappers, org.apache.hadoop.conf.Configuration conf) {
//
//    final String MAPPER_MULTIPLIER = "oraoop.datachunk.mapper.multiplier";
//    final String RESULT_INCREMENT = "oraoop.datachunk.result.increment";
//
//    int numberToMultiplyMappersBy = conf.getInt(MAPPER_MULTIPLIER, 2);
//    int numberToIncrementResultBy = conf.getInt(RESULT_INCREMENT, 1);
//
//    // The number of chunks generated will *not* be a multiple of the number of
//    // splits,
//    // to ensure that each split doesn't always get data from the start of each
//    // data-file...
//    int numberOfDataChunksPerOracleDataFile =
//        (desiredNumberOfMappers * numberToMultiplyMappersBy)
//            + numberToIncrementResultBy;
//
//    LOG.debug(String.format("%s:\n" + "\t%s=%d\n" + "\t%s=%d\n"
//        + "\tdesiredNumberOfMappers=%d\n" + "\tresult=%d",
//        getCurrentMethodName(), MAPPER_MULTIPLIER, numberToMultiplyMappersBy,
//        RESULT_INCREMENT, numberToIncrementResultBy, desiredNumberOfMappers,
//        numberOfDataChunksPerOracleDataFile));
//
//    return numberOfDataChunksPerOracleDataFile;
//  }

  public static OracleDataChunkMethod
      getOraOopOracleDataChunkMethod(FromJobConfig jobConfig) {
    OracleDataChunkMethod result = jobConfig.dataChunkMethod;
    if(result == null) {
      result = OracleDataChunkMethod.ROWID;
    }
    return result;
  }

  public static
      OracleBlockToSplitAllocationMethod getOracleBlockToSplitAllocationMethod(
          FromJobConfig jobConfig) {
    OracleBlockToSplitAllocationMethod result =
        jobConfig.dataChunkAllocationMethod;
    if(result == null) {
      result = OracleBlockToSplitAllocationMethod.ROUNDROBIN;
    }
    return result;
  }

  public static OracleTableImportWhereClauseLocation
      getTableImportWhereClauseLocation(
        FromJobConfig jobConfig) {
    OracleTableImportWhereClauseLocation result = jobConfig.whereClauseLocation;
    if(result == null) {
      result = OracleTableImportWhereClauseLocation.SUBSPLIT;
    }
    return result;
  }

//  public static String getOutputDirectory(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    String workingDir = conf.get("mapred.working.dir");
//    String outputDir = conf.get("mapred.output.dir");
//
//    return workingDir + "/" + outputDir;
//  }
//
//  public static String padLeft(String s, int n) {
//    return StringUtils.leftPad(s, n);
//  }
//
//  public static String padRight(String s, int n) {
//    return StringUtils.rightPad(s, n);
//  }
//
//  public static boolean stackContainsClass(String className) {
//
//    StackTraceElement[] stackTraceElements = (new Throwable()).getStackTrace();
//    for (StackTraceElement stackTraceElement : stackTraceElements) {
//      if (stackTraceElement.getClassName().equalsIgnoreCase(className)) {
//        return true;
//      }
//    }
//
//    return false;
//  }
//
//  public static Object startSessionSnapshot(Connection connection) {
//
//    Object result = null;
//    try {
//
//      Class<?> oraOopOraStatsClass =
//          Class.forName("quest.com.oraOop.oracleStats.OraOopOraStats");
//      Method startSnapshotMethod =
//          oraOopOraStatsClass.getMethod("startSnapshot", Connection.class);
//      if (connection != null) {
//        result = startSnapshotMethod.invoke(null, connection);
//      }
//    } catch (ClassNotFoundException ex) {
//      throw new RuntimeException(ex);
//    } catch (NoSuchMethodException ex) {
//      throw new RuntimeException(ex);
//    } catch (InvocationTargetException ex) {
//      throw new RuntimeException(ex);
//    } catch (IllegalAccessException ex) {
//      throw new RuntimeException(ex);
//    }
//
//    return result;
//  }
//
//  public static OraOopStatsReports stopSessionSnapshot(Object oraOopOraStats) {
//
//    OraOopStatsReports result = new OraOopStatsReports();
//
//    if (oraOopOraStats == null) {
//      return result;
//    }
//
//    try {
//
//      Class<?> oraOopOraStatsClass =
//          Class.forName("quest.com.oraOop.oracleStats.OraOopOraStats");
//      Method finalizeSnapshotMethod =
//          oraOopOraStatsClass.getMethod("finalizeSnapshot", (Class<?>[]) null);
//      finalizeSnapshotMethod.invoke(oraOopOraStats, (Object[]) null);
//
//      Method performanceReportCsvMethod =
//          oraOopOraStatsClass.getMethod("getStatisticsCSV", (Class<?>[]) null);
//      result.setCsvReport((String) performanceReportCsvMethod.invoke(
//          oraOopOraStats, (Object[]) null));
//
//      Method performanceReportMethod =
//          oraOopOraStatsClass.getMethod("performanceReport", (Class<?>[]) null);
//      result.setPerformanceReport((String) performanceReportMethod.invoke(
//          oraOopOraStats, (Object[]) null));
//    } catch (ClassNotFoundException ex) {
//      throw new RuntimeException(ex);
//    } catch (NoSuchMethodException ex) {
//      throw new RuntimeException(ex);
//    } catch (InvocationTargetException ex) {
//      throw new RuntimeException(ex);
//    } catch (IllegalAccessException ex) {
//      throw new RuntimeException(ex);
//    }
//
//    return result;
//  }
//
  public static boolean stringArrayContains(String[] list, String value,
      boolean ignoreCase) {

    return stringArrayIndexOf(list, value, ignoreCase) > -1;
  }

  public static int stringArrayIndexOf(String[] list, String value,
      boolean ignoreCase) {

    for (int idx = 0; idx < list.length; idx++) {
      if (list[idx].equals(value)) {
        return idx;
      }
      if (ignoreCase && list[idx].equalsIgnoreCase(value)) {
        return idx;
      }
    }
    return -1;
  }

  public static String stringArrayToCSV(String[] list) {

    return stringArrayToCSV(list, "");
  }

  public static String
      stringArrayToCSV(String[] list, String encloseValuesWith) {

    StringBuilder result = new StringBuilder((list.length * 2) - 1);
    for (int idx = 0; idx < list.length; idx++) {
      if (idx > 0) {
        result.append(",");
      }
      result
          .append(String.format("%1$s%2$s%1$s", encloseValuesWith, list[idx]));
    }
    return result.toString();
  }

  public static int stringListIndexOf(List<String> list, String value,
      boolean ignoreCase) {

    for (int idx = 0; idx < list.size(); idx++) {
      if (list.get(idx).equals(value)) {
        return idx;
      }
      if (ignoreCase && list.get(idx).equalsIgnoreCase(value)) {
        return idx;
      }
    }
    return -1;
  }

//  public static void writeOutputFile(org.apache.hadoop.conf.Configuration conf,
//      String fileName, String fileText) {
//
//    Path uniqueFileName = null;
//    try {
//      FileSystem fileSystem = FileSystem.get(conf);
//
//      // NOTE: This code is not thread-safe.
//      // i.e. A race-condition could still cause this code to 'fail'.
//
//      int suffix = 0;
//      String fileNameTemplate = fileName + "%s";
//      while (true) {
//        uniqueFileName =
//            new Path(getOutputDirectory(conf), String.format(fileNameTemplate,
//                suffix == 0 ? "" : String.format(" (%d)", suffix)));
//        if (!fileSystem.exists(uniqueFileName)) {
//          break;
//        }
//        suffix++;
//      }
//
//      FSDataOutputStream outputStream =
//          fileSystem.create(uniqueFileName, false);
//      if (fileText != null) {
//        outputStream.writeBytes(fileText);
//      }
//      outputStream.flush();
//      outputStream.close();
//    } catch (IOException ex) {
//      LOG.error(String.format("Error attempting to write the file %s\n" + "%s",
//          (uniqueFileName == null ? "null" : uniqueFileName.toUri()),
//          getFullExceptionMessage(ex)));
//    }
//  }
//
  /**
   * Class to wrap details about Oracle connection string.
   */
  public static class JdbcOracleThinConnection {
    private String host;
    private int port;
    private String sid;
    private String service;
    private String tnsName;

    public JdbcOracleThinConnection(String host, int port, String sid,
      String service, String tnsName) {
      this.host = host;
      this.port = port;
      this.sid = sid;
      this.service = service;
      this.tnsName = tnsName;
    }

    @Override
    public String toString() {
       // Use tnsName if it is available
      if (this.tnsName != null && !this.tnsName.isEmpty()) {
        return String.format("jdbc:oracle:thin:@%s", tnsName);
      }

      // Use the SID if it's available...
      if (this.sid != null && !this.sid.isEmpty()) {
        return String.format("jdbc:oracle:thin:@%s:%d:%s", this.host,
            this.port, this.sid);
      }

      // Otherwise, use the SERVICE. Note that the service is prefixed by "/",
      // not by ":"...
      if (this.service != null && !this.service.isEmpty()) {
        return String.format("jdbc:oracle:thin:@%s:%d/%s", this.host,
            this.port, this.service);
      }

      throw new RuntimeException(
          "Unable to generate a JDBC URL, as no TNS name, SID or SERVICE "
            + "has been provided.");

    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public String getSid() {
      return sid;
    }

    public String getService() {
      return service;
    }

    public String getTnsName() {
      return tnsName;
    }
  }

  /**
   * Thrown if the Oracle connection string cannot be parsed.
   */
  public static class JdbcOracleThinConnectionParsingError extends Exception {

    private static final long serialVersionUID = 1559860600099354233L;

    public JdbcOracleThinConnectionParsingError(String message) {

      super(message);
    }

    public JdbcOracleThinConnectionParsingError(String message,
                                                Throwable cause) {

      super(message, cause);
    }

    public JdbcOracleThinConnectionParsingError(Throwable cause) {

      super(cause);
    }
  }

//  public static String getOracleServiceName(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    return conf.get(OraOopConstants.ORAOOP_ORACLE_RAC_SERVICE_NAME, "");
//  }
//
  public static String generateOracleSidJdbcUrl(String hostName, int port,
      String sid) {

    return String.format("jdbc:oracle:thin:@(DESCRIPTION=" + "(ADDRESS_LIST="
        + "(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))" + ")"
        + "(CONNECT_DATA=(SERVER=DEDICATED)(SID=%s))" + ")", hostName, port,
        sid);
  }

  public static String generateOracleServiceNameJdbcUrl(String hostName,
      int port, String serviceName) {

    return String.format("jdbc:oracle:thin:@(DESCRIPTION=" + "(ADDRESS_LIST="
        + "(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))" + ")"
        + "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s))" + ")", hostName,
        port, serviceName);
  }

  public static String generateOracleTnsNameJdbcUrl(String tnsName) {
    return String.format("jdbc:oracle:thin:@%s", tnsName);
  }

  public static String getMapperJdbcUrlPropertyName(int mapperId) {
    return String.format("oraoop.mapper.jdbc.url.%d", mapperId);
  }
//
//  public static final String SQOOP_JOB_TYPE = "oraoop.sqoop.job.type";
//
//  public static void rememberSqoopJobType(OraOopConstants.Sqoop.Tool jobType,
//      org.apache.hadoop.conf.Configuration conf) {
//
//    conf.set(SQOOP_JOB_TYPE, jobType.name());
//  }
//
//  public static OraOopConstants.Sqoop.Tool recallSqoopJobType(
//      org.apache.hadoop.conf.Configuration conf) {
//
//    String jobType = conf.get(SQOOP_JOB_TYPE);
//    if (jobType == null || jobType.isEmpty()) {
//      throw new RuntimeException(
//          "RecallSqoopJobType() cannot be called unless RememberSqoopJobType() "
//            + "has been used.");
//    }
//
//    OraOopConstants.Sqoop.Tool result =
//        OraOopConstants.Sqoop.Tool.valueOf(jobType);
//    return result;
//  }

  public static boolean omitLobAndLongColumnsDuringImport(
      FromJobConfig jobConfig) {
    return BooleanUtils.isTrue(jobConfig.omitLobColumns);
  }

//  public static boolean oracleSessionHasBeenKilled(Exception exception) {
//
//    Throwable ex = exception;
//
//    while (ex != null) {
//      if (ex instanceof SQLException
//          && ((SQLException) ex).getErrorCode() == 28) { // ORA-00028: your
//                                                         // session has been
//                                                         // killed
//        return true;
//      }
//
//      ex = ex.getCause();
//    }
//
//    return false;
//  }
//
  private static String
      formatTimestampForOracleObjectName(Object oracleDateTime) {

    // NOTE: Update decodeTimestampFromOracleObjectName() if you modify this
    // method.

    String jobTimeStr =
        OracleQueries.oraDATEToString(oracleDateTime,
            OracleJdbcConnectorConstants.
            ORACLE_OBJECT_NAME_DATE_TO_STRING_FORMAT_STRING);

    return jobTimeStr;

    // E.g. 20101028_151000 (15 characters)
  }

  private static Object decodeTimestampFromOracleObjectName(
      String oracleObjectNameTimestampFragment) {

    String dateString = oracleObjectNameTimestampFragment;
    String dateFormatString = OracleJdbcConnectorConstants.
        ORACLE_OBJECT_NAME_DATE_TO_STRING_FORMAT_STRING;

    // return oracle.sql.DATE.fromText(oracleObjectNameTimestampFragment
    // ,OraOopConstants.ORACLE_OBJECT_NAME_DATE_TO_STRING_FORMAT_STRING
    // ,null);

    /*
     * Unfortunately, we don't seem to be able to reliably decode strings into
     * DATE objects using Oracle. For example, the following string will cause
     * Oracle to throw an "Invalid Oracle date" exception, due to the time
     * portion starting with a zero...
     * oracle.sql.DATE.fromText("20101123 091554", "yyyymmdd hh24miss", null);
     *
     * Therefore, we need to manually deconstruct the date string and insert
     * some colons into the time so that Oracle can decode the string. (This is
     * therefore an Oracle bug we're working around.)
     */

    try {
      String year = oracleObjectNameTimestampFragment.substring(0, 4);
      String month = oracleObjectNameTimestampFragment.substring(4, 6);
      String day = oracleObjectNameTimestampFragment.substring(6, 8);
      String hour = oracleObjectNameTimestampFragment.substring(9, 11);
      String minute = oracleObjectNameTimestampFragment.substring(11, 13);
      String second = oracleObjectNameTimestampFragment.substring(13, 15);
      dateString =
          String.format("%s/%s/%s %s:%s:%s", year, month, day, hour, minute,
              second);
      dateFormatString = "yyyy/mm/dd hh24:mi:ss";

      return OracleQueries.oraDATEFromString(
          dateString, dateFormatString);
    } catch (Exception ex) {
      LOG.debug(String.format(
          "%s could not convert the string \"%s\" into a DATE via the format "
              + "string \"%s\".\n" + "The error encountered was:\n%s",
          getCurrentMethodName(), dateString, dateFormatString,
          getFullExceptionMessage(ex)));

      return null;
    }
  }

  public static String createExportTablePartitionNameFromOracleTimestamp(
      Object oracleDateTime) {

    // Partition name can be up to 30 characters long and must start with a
    // letter...
    return OracleJdbcConnectorConstants.EXPORT_TABLE_PARTITION_NAME_PREFIX
        + formatTimestampForOracleObjectName(oracleDateTime);

    // E.g. ORAOOP_20101028_151000 (22 characters)
  }

  public static String createExportTableNamePrefixFromOracleTimestamp(
      Object oracleDateTime) {

    // NOTE: Alter decodeExportTableNamePrefix() if you modify this method.

    // Table name can be 30 characters long and must start with a letter...
    return OracleJdbcConnectorConstants.EXPORT_MAPPER_TABLE_NAME_PREFIX
        + formatTimestampForOracleObjectName(oracleDateTime);
    // G1.ORAOOP_20101028_152500 (22 characters) (This is just the prefix,
    // append "_3" for mapper 4)
  }

  public static Object decodeExportTableNamePrefix(String tableNamePrefix) {

    if (tableNamePrefix == null || tableNamePrefix.isEmpty()) {
      return null;
    }

    if (!tableNamePrefix.startsWith(OracleJdbcConnectorConstants.
        EXPORT_MAPPER_TABLE_NAME_PREFIX)) {
      return null;
    }

    String formattedTimestamp =
        tableNamePrefix.substring(OracleJdbcConnectorConstants.
            EXPORT_MAPPER_TABLE_NAME_PREFIX.length(),
            tableNamePrefix.length());

    return decodeTimestampFromOracleObjectName(formattedTimestamp);
  }

  private static boolean userWantsToCreateExportTableFromTemplate(
      ToJobConfig jobConfig) {
    String exportTableTemplate = jobConfig.templateTable;
    if (StringUtils.isEmpty(exportTableTemplate)) {
      return false;
    } else {
      return true;
    }
  }

  public static boolean enableOracleParallelProcessingDuringExport(
      ToJobConfig jobConfig) {
    return BooleanUtils.isTrue(jobConfig.parallel);
  }

  public static boolean userWantsToCreatePartitionedExportTableFromTemplate(
      ToJobConfig jobConfig) {
    return userWantsToCreateExportTableFromTemplate(jobConfig)
        && BooleanUtils.isTrue(jobConfig.partitioned);
  }

  public static boolean userWantsToCreateNonPartitionedExportTableFromTemplate(
      ToJobConfig jobConfig) {
    return userWantsToCreateExportTableFromTemplate(jobConfig)
        && !BooleanUtils.isTrue(jobConfig.partitioned);
  }

  public static String generateExportTableSubPartitionName(int mapperId,
      Object sysDateTime) {

    String partitionName =
        createExportTablePartitionNameFromOracleTimestamp(sysDateTime);

    String subPartitionName = String.format("%s_MAP_%d" // <- Should allow for
                                                        // 1,000 mappers before
                                                        // exceeding 30
                                                        // characters
        , partitionName // <- Partition name is 22 characters
        , mapperId);

    // Check the length of the name...
    if (subPartitionName.length()
            > OracleJdbcConnectorConstants.Oracle.MAX_IDENTIFIER_LENGTH) {
      throw new RuntimeException(
          String
              .format(
                  "The generated Oracle subpartition name \"%s\" is longer "
                + "than %d characters.",
                  subPartitionName,
                  OracleJdbcConnectorConstants.Oracle.MAX_IDENTIFIER_LENGTH));
    }

    return subPartitionName;
  }

  public static String[] generateExportTableSubPartitionNames(int numMappers,
      Object sysDateTime) {

    String[] result = new String[numMappers];
    for (int idx = 0; idx < numMappers; idx++) {
      result[idx] = generateExportTableSubPartitionName(idx, sysDateTime);
    }

    return result;
  }

  public static OracleTable generateExportTableMapperTableName(int mapperId,
      Object sysDateTime, String schema) {
    //mapperId: should allow 10,000,000 mappers before it exceeds 30 characters.
    return generateExportTableMapperTableName(Integer.toString(mapperId)
        , sysDateTime, schema);
  }

  public static OracleTable generateExportTableMapperTableName(
      String mapperSuffix, Object sysDateTime, String schema) {

    // NOTE: Update decodeExportTableMapperTableName() if you alter this method.

    // Generate a (22 character) prefix to use for the N tables that need to be
    // created for the N mappers to insert into...
    String mapperTableNamePrefix =
        createExportTableNamePrefixFromOracleTimestamp(sysDateTime);

    // Generate the name...
    String tableName = String.format("%s_%s", mapperTableNamePrefix // <- 22
                                                                    // chars
        , mapperSuffix);

    // Check the length of the name...
    if (tableName.length() > OracleJdbcConnectorConstants.
        Oracle.MAX_IDENTIFIER_LENGTH) {
      throw new RuntimeException(
          String
              .format(
                  "The generated Oracle table name \"%s\" is longer than "
                + "%d characters.",
                  tableName, OracleJdbcConnectorConstants.
                  Oracle.MAX_IDENTIFIER_LENGTH));
    }

    return new OracleTable(schema, tableName);
  }

  /**
   * Class to wrap the table name to be used for the mapper.
   */
  public static class DecodedExportMapperTableName {
    private String mapperId; // <- This is not an int, because it might be "CHG"
                            // in the case of a "changes-table".
    private Object tableDateTime;

    public String getMapperId() {
      return mapperId;
    }

    public void setMapperId(String newMapperId) {
      this.mapperId = newMapperId;
    }

    public Object getTableDateTime() {
      return tableDateTime;
    }

    public void setTableDateTime(Object newTableDateTime) {
      this.tableDateTime = newTableDateTime;
    }
  }

  public static DecodedExportMapperTableName decodeExportTableMapperTableName(
      OracleTable oracleTable) {

    DecodedExportMapperTableName result = null;
    try {
      int lastUnderScoreIndex = oracleTable.getName().lastIndexOf("_");
      if (lastUnderScoreIndex == -1) {
        return result;
      }

      String dateFragment =
          oracleTable.getName().substring(0, lastUnderScoreIndex);
      String mapperIdFragment =
          oracleTable.getName().substring(lastUnderScoreIndex + 1,
              oracleTable.getName().length());

      Object sysDateTime = decodeExportTableNamePrefix(dateFragment);
      if (sysDateTime != null) {
        result = new DecodedExportMapperTableName();
        result.setTableDateTime(sysDateTime);
        result.setMapperId(mapperIdFragment);
      }
    } catch (Exception ex) {
      LOG.debug(
        String.format(
         "Error when attempting to decode the export mapper-table name \"%s\".",
                  oracleTable.toString()), ex);
    }
    return result;
  }

//  public static void rememberOracleDateTime(
//      org.apache.hadoop.conf.Configuration conf, String propertyName,
//      String dateTime) {
//    conf.set(propertyName, dateTime);
//  }
//
  public static Object recallOracleDateTime(ImmutableContext context) {

    String dateTimeStr = context.getString(
        OracleJdbcConnectorConstants.SQOOP_ORACLE_JOB_SYSDATE);
    if (dateTimeStr == null || dateTimeStr.isEmpty()) {
      throw new RuntimeException(String.format(
          "Unable to recall the value of the property \"%s\".",
          OracleJdbcConnectorConstants.SQOOP_ORACLE_JOB_SYSDATE));
    }

    return OracleQueries.oraDATEFromString(dateTimeStr,
        "yyyy-mm-dd hh24:mi:ss");
  }

  public static UpdateMode getExportUpdateMode(ToJobConfig jobConfig) {
    UpdateMode updateMode = UpdateMode.Update;

    if (BooleanUtils.isTrue(jobConfig.updateMerge)) {
      updateMode = UpdateMode.Merge;
    }

    return updateMode;
  }

  public static InsertMode getExportInsertMode(ToJobConfig jobConfig,
      ImmutableContext context) {

    InsertMode result = InsertMode.DirectInsert;

    if (OracleUtilities
        .userWantsToCreatePartitionedExportTableFromTemplate(jobConfig)
        || context.getBoolean(OracleJdbcConnectorConstants.
            EXPORT_TABLE_HAS_SQOOP_PARTITIONS, false)) {
      result = InsertMode.ExchangePartition;
    }

    return result;
  }

//  public static String getJavaClassPath() {
//
//    return System.getProperty("java.class.path");
//  }

  public static String replaceAll(String inputString, String textToReplace,
      String replaceWith) {

    String result = inputString.replaceAll(textToReplace, replaceWith);
    if (!result.equals(inputString)) {
      result = replaceAll(result, textToReplace, replaceWith);
    }

    return result;
  }

  public static String getTemporaryTableStorageClause(ToJobConfig jobConfig) {

    String result = jobConfig.temporaryStorageClause;
    if (result == null) {
      result = "";
    }
    return result;
  }

  public static String getExportTableStorageClause(ToJobConfig jobConfig) {

    String result = jobConfig.storageClause;
    if (result == null) {
      result = "";
    }
    return result;
  }

  public static String[] getExportUpdateKeyColumnNames(ToJobConfig jobConfig) {

    if (jobConfig.updateKey == null || jobConfig.updateKey.isEmpty()) {
      // This must be an "insert-export" if no --update-key has been specified!
      return new String[0];
    }

    return jobConfig.updateKey.toArray(new String[jobConfig.updateKey.size()]);
  }
//
  /**
   * Splits a string separated by commas - the elements can be optionally
   * enclosed in quotes - this allows the elements to have commas in them.
   *
   * @param value
   *          The String to be split
   * @return A list of values
   */
  public static List<String> splitStringList(String value) {
    List<String> result = new ArrayList<String>();
    if (value != null && !value.isEmpty()) {
      Pattern pattern = Pattern.compile("([^\",]*|\"[^\"]*\")(,|$)");
      Matcher matcher = pattern.matcher(value);
      while (matcher.find()) {
        if (matcher.group(1) != null && !matcher.group(1).isEmpty()) {
          result.add(matcher.group(1));
        }
      }
    }
    return result;
  }

  /**
   * Splits a string list separated by commas. If the element is not surrounded
   * by quotes it will be return in upper case. If the element is enclosed in
   * quotes it will be returned in the same case and special characters will be
   * retained.
   *
   * @param value
   *          The String to be split
   * @return A list of values
   */
  public static List<String> splitOracleStringList(String value) {
    List<String> result = new ArrayList<String>();
    List<String> splitValue = splitStringList(value);
    Pattern pattern = Pattern.compile("(\")([^\"]*)(\")");
    for (String element : splitValue) {
      Matcher matcher = pattern.matcher(element);
      if (matcher.find()) {
        result.add(matcher.group(2));
      } else {
        result.add(element.toUpperCase());
      }
    }
    return result;
  }

  public static AppendValuesHintUsage getOracleAppendValuesHintUsage(
      ToJobConfig jobConfig) {
    AppendValuesHintUsage result = jobConfig.appendValuesHint;
    if (result == null) {
      result = AppendValuesHintUsage.AUTO;
    }
    return result;
  }

  public static String getImportHint(FromJobConfig jobConfig) {
    String result = null;
    result = jobConfig.queryHint;
    if (result == null || result.isEmpty()) {
      result = "/*+ NO_INDEX(t) */ ";
    } else if(result.trim().isEmpty()) {
      result = "";
    } else {
      result = String.format(
          OracleJdbcConnectorConstants.Oracle.HINT_SYNTAX, result);
    }
    return result;
  }

//  public static void appendJavaSecurityEgd(Configuration conf) {
//    String mapredJavaOpts = conf.get("mapred.child.java.opts");
//    if (mapredJavaOpts == null
//        || !mapredJavaOpts.contains("-Djava.security.egd")) {
//      StringBuilder newMapredJavaOpts =
//          new StringBuilder("-Djava.security.egd=file:///dev/urandom");
//      if (mapredJavaOpts != null && !mapredJavaOpts.isEmpty()) {
//        newMapredJavaOpts.append(" ").append(mapredJavaOpts);
//      }
//      String newMapredJavaOptsString = newMapredJavaOpts.toString();
//      conf.set("mapred.child.java.opts", newMapredJavaOptsString);
//      LOG.debug("Updated mapred.child.java.opts from \"" + mapredJavaOpts
//          + "\" to \"" + newMapredJavaOptsString + "\"");
//    }
//  }

  public static void checkJavaSecurityEgd() {
    String javaSecurityEgd = System.getProperty("java.security.egd");
    if (!"file:///dev/urandom".equals(javaSecurityEgd)) {
      LOG.warn("System property java.security.egd is not set to "
             + "file:///dev/urandom - Oracle connections may time out.");
    }
  }

  public static void setCurrentSessionUser(String user) {
    currentSessionUser = user;
  }

  public static ExportTableUpdateTechnique getExportTableUpdateTechnique(
      ImmutableContext context, UpdateMode updateMode) {

    ExportTableUpdateTechnique result;

    if (context.getBoolean(OracleJdbcConnectorConstants.
        EXPORT_TABLE_HAS_SQOOP_PARTITIONS, false)) {
      switch (updateMode) {

        case Update:
          result = ExportTableUpdateTechnique.ReInsertUpdatedRows;
          break;

        case Merge:
          result = ExportTableUpdateTechnique.ReInsertUpdatedRowsAndNewRows;
          break;

        default:
          throw new RuntimeException(String.format(
              "Update %s to cater for the updateMode \"%s\".",
              OracleUtilities.getCurrentMethodName(), updateMode
                  .toString()));
      }
    } else {
      switch (updateMode) {

        case Update:
          result = ExportTableUpdateTechnique.UpdateSql;
          break;

        case Merge:
          result = ExportTableUpdateTechnique.MergeSql;
          break;

        default:
          throw new RuntimeException(String.format(
              "Update %s to cater for the updateMode \"%s\".",
              OracleUtilities.getCurrentMethodName(), updateMode
                  .toString()));
      }
    }

    return result;
  }

  private static boolean isEscaped(String name) {
    return name.startsWith("\"") && name.endsWith("\"");
  }

  private String escapeOracleColumnName(String columnName) {
    if (isEscaped(columnName)) {
      return columnName;
    } else {
      return "\"" + columnName + "\"";
    }
  }

  private static String unescapeOracleColumnName(String columnName) {
    if (isEscaped(columnName)) {
      return columnName.substring(1, columnName.length() - 1);
    } else {
      return columnName;
    }
  }

  public static List<String> getSelectedColumnNamesInOracleTable(
      OracleTable table, List<String> colNamesInTable,
      List<String> selectedColumnsInput) {
    if (selectedColumnsInput != null && selectedColumnsInput.size() > 0) {
      String[] selectedColumns = new String[selectedColumnsInput.size()];

      for (int idx = 0; idx < selectedColumnsInput.size(); idx++) {
        String column = selectedColumnsInput.get(idx);
        // If the user did not escape this column name, then we should
        // uppercase it...
        if (!isEscaped(column)) {
          selectedColumns[idx] = column.toUpperCase();
        } else {
          // If the user escaped this column name, then we should
          // retain its case...
          selectedColumns[idx] = unescapeOracleColumnName(column);
        }
      }

      // Ensure there are no duplicated column names...
      String[] duplicates =
          OracleUtilities
              .getDuplicatedStringArrayValues(selectedColumns, false);
      if (duplicates.length > 0) {
        StringBuilder msg = new StringBuilder();
        msg.append("The following column names have been duplicated in the ");
        msg.append("\"--columns\" clause:\n");

        for (String duplicate : duplicates) {
          msg.append("\t" + duplicate + "\n");
        }

        throw new RuntimeException(msg.toString());
      }

      List<String> result = new ArrayList<String>(selectedColumnsInput.size());
      // Ensure the user selected column names that actually exist...
      for (String selectedColumn : selectedColumns) {
        if (!colNamesInTable.contains(selectedColumn)) {
          throw new RuntimeException(String.format(
              "The column named \"%s\" does not exist within the table"
                  + "%s (or is of an unsupported data-type).", selectedColumn,
              table.toString()));
        }
        result.add(selectedColumn);
      }
      return result;
    } else {
      return colNamesInTable;
    }
  }
}
