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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.manager.OracleManager;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.GenericJdbcManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;
import com.cloudera.sqoop.mapreduce.JdbcUpdateExportJob;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * OraOop manager for high performance Oracle import / export.
 * NOTES:
 *   Escaping Column Names:
 *   ----------------------
 *   There are 3 main queries that occur during a Sqoop import.
 *   (1) Selecting columns to obtain their data-type via getColTypesQuery();
 *   (2) selecting column names via getColNamesQuery(); and
 *   (3) getting the data during the import via
 *       OraOopDBRecordReader.getSelectQuery();
 *   In each of these queries, we'd ideally escape the column names so that
 *   Oracle columns that require this work okay.
 *   Unfortunately we can't do this, because if the user specifies column
 *   names via the "--columns" clause, these names will be used (verbatim)
 *   during OraOopDBRecordReader.getSelectQuery(). This means that we could
 *   only escape the column names during OraOopDBRecordReader.getSelectQuery()
 *   if the user entered them in the correct case.
 *   Therefore, escapeColName() in this class does not actually do anything so
 *   that OraOopDBRecordReader.getSelectQuery() generates a valid SQL statement
 *   when the user utilises the "--columns" clause.
 *   However, getColTypesQuery() and getColNamesQuery() do escape column names
 *   via the method escapeOracleColumnName(). We also get getColumnTypes() to
 *   unescape the column names so that Sqoop has the most accurate column
 *   name strings.
 */
public class OraOopConnManager extends GenericJdbcManager {

  public static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopConnManager.class.getName());
  private List<String> columnNamesInOracleTable = null;
  private Map<String, Integer> columnTypesInOracleTable = null;
  private final String timestampJavaType;

  public OraOopConnManager(final SqoopOptions sqoopOptions) {
    super(OraOopConstants.ORACLE_JDBC_DRIVER_CLASS, sqoopOptions);
    if (this.options.getConf().getBoolean(
        OraOopConstants.ORAOOP_MAP_TIMESTAMP_AS_STRING,
        OraOopConstants.ORAOOP_MAP_TIMESTAMP_AS_STRING_DEFAULT)) {
      timestampJavaType = "String";
    } else {
      timestampJavaType = super.toJavaType(Types.TIMESTAMP);
    }
  }

  @Override
  protected Connection makeConnection() throws SQLException {

    String connectStr = this.options.getConnectString();
    String username = this.options.getUsername();
    String password = this.options.getPassword();
    Properties additionalProps = this.options.getConnectionParams();

    Connection connection =
        OracleConnectionFactory.createOracleJdbcConnection(this
            .getDriverClass(), connectStr, username, password, additionalProps);
    if (username == null) {
      username = OracleManager.getSessionUser(connection);
    }
    OraOopUtilities.setCurrentSessionUser(username);
    return connection;
  }

  @Override
  public void close() throws SQLException {

    super.close();
  }

  private List<String> getColumnNamesInOracleTable(String tableName) {

    if (this.columnNamesInOracleTable == null) {

      OracleTable tableContext = null;

      try {
        tableContext = getOracleTableContext();

        Configuration conf = this.options.getConf();

        this.columnNamesInOracleTable =
            OraOopOracleQueries.getTableColumnNames(getConnection(),
                tableContext, OraOopUtilities
                    .omitLobAndLongColumnsDuringImport(conf), OraOopUtilities
                    .recallSqoopJobType(conf), true, // <-
                                                     // onlyOraOopSupportedTypes
                true // <- omitOraOopPseudoColumns
                );
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }

    // Return a copy of our list, so the original will not be inadvertently
    // altered...
    return OraOopUtilities.copyStringList(this.columnNamesInOracleTable);
  }

  protected List<String> getSelectedColumnNamesInOracleTable(String tableName) {

    List<String> colNamesInTable = getColumnNamesInOracleTable(tableName);

    String[] selectedColumns = this.options.getColumns();
    if (selectedColumns != null && selectedColumns.length > 0) {

      for (int idx = 0; idx < selectedColumns.length; idx++) {

        String selectedColumn = selectedColumns[idx];
        // If the user did not escape this column name, then we should
        // uppercase it...
        if (!isEscaped(selectedColumn)) {
          selectedColumns[idx] = selectedColumn.toUpperCase();
        } else {
          // If the user escaped this column name, then we should
          // retain its case...
          selectedColumns[idx] = unescapeOracleColumnName(selectedColumn);
        }
      }

      // Ensure there are no duplicated column names...
      String[] duplicates =
          OraOopUtilities
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

      // Ensure the user selected column names that actually exist...
      for (String selectedColumn : selectedColumns) {
        if (!colNamesInTable.contains(selectedColumn)) {
          OracleTable tableContext = getOracleTableContext();
          throw new RuntimeException(String.format(
              "The column named \"%s\" does not exist within the table"
                  + "%s (or is of an unsupported data-type).", selectedColumn,
              tableContext.toString()));
        }
      }

      // Remove any columns (that exist in the table) that were not
      // selected by the user...
      for (int idx = colNamesInTable.size() - 1; idx >= 0; idx--) {
        String colName = colNamesInTable.get(idx);
        if (!OraOopUtilities.stringArrayContains(selectedColumns, colName,
            false)) {
          colNamesInTable.remove(idx);
        }
      }
    }

    // To assist development/testing of Oracle data-types, you can use this
    // to limit the number of columns from the table...
    int columnNameLimit =
        this.options.getConf().getInt("oraoop.column.limit", 0);
    if (columnNameLimit > 0) {
      columnNameLimit = Math.min(columnNameLimit, colNamesInTable.size());
      colNamesInTable = colNamesInTable.subList(0, columnNameLimit);
    }

    return colNamesInTable;
  }

  @Override
  protected String getColTypesQuery(String tableName) {

    List<String> colNames = getSelectedColumnNamesInOracleTable(tableName);

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (int idx = 0; idx < colNames.size(); idx++) {
      if (idx > 0) {
        sb.append(",");
      }
      sb.append(escapeOracleColumnName(colNames.get(idx))); // <- See notes at
                                                            // top about escaped
                                                            // column names
    }
    sb.append(String.format(" FROM %s WHERE 0=1", tableName));

    return sb.toString();
  }

  @Override
  protected String getColNamesQuery(String tableName) {

    // NOTE: This code is similar to getColTypesQuery() - except the
    // escaping of column names and table name differs.

    List<String> colNames = getSelectedColumnNamesInOracleTable(tableName);

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (int idx = 0; idx < colNames.size(); idx++) {
      if (idx > 0) {
        sb.append(",");
      }
      sb.append(escapeColName(colNames.get(idx))); // <- See notes at top about
                                                   // escaped column names
    }
    sb.append(String.format(" FROM %s WHERE 1=0", escapeTableName(tableName)));

    return sb.toString();
  }

  @Override
  protected String getSplitColumn(SqoopOptions opts, String tableName) {

    // If we're importing an Oracle table and will be generating
    // "splits" based on its Oracle data-files, we don't actually require
    // a primary key to exist, or for the user to identify the split-column.
    // As a consequence, return "NotRequired" to prevent sqoop code
    // such as SqlManager.importTable() from throwing an exception.
    //
    // NB: The tableName parameter will be null if no table is involved,
    // such as when importing data via an (arbitrary) SQL query.
    if (tableName != null) {
      return OraOopConstants.TABLE_SPLIT_COLUMN_NOT_REQUIRED;
    } else {
      return super.getSplitColumn(opts, tableName);
    }
  }

  @Override
  public void importTable(ImportJobContext context) throws IOException,
      ImportException {

    logImportTableDetails(context);

    context.setConnManager(this);

    // Specify the Oracle-specific DBInputFormat for import.
    context.setInputFormat(OraOopDataDrivenDBInputFormat.class);

    super.importTable(context);
  }

  @Override
  public void exportTable(ExportJobContext context) throws IOException,
      ExportException {

    logExportTableDetails(context);

    if (this.columnTypesInOracleTable == null) {
      throw new ExportException("The column-types for the table are not"
          + "known.");
    }
    if (this.columnTypesInOracleTable.containsValue(OraOopOracleQueries
        .getOracleType("BINARY_DOUBLE"))) {
      context.getOptions().getConf().setBoolean(
          OraOopConstants.TABLE_CONTAINS_BINARY_DOUBLE_COLUMN, true);
    }
    if (this.columnTypesInOracleTable.containsValue(OraOopOracleQueries
        .getOracleType("BINARY_FLOAT"))) {
      context.getOptions().getConf().setBoolean(
          OraOopConstants.TABLE_CONTAINS_BINARY_FLOAT_COLUMN, true);
    }

    context.setConnManager(this);

    @SuppressWarnings("rawtypes")
    Class<? extends OutputFormat> oraOopOutputFormatClass;
    try {
      oraOopOutputFormatClass = OraOopOutputFormatInsert.class;
    } catch (NoClassDefFoundError ex) {
      explainWhyExportClassCannotBeLoaded(ex, "OraOopOutputFormatInsert");
      throw ex;
    }
    JdbcExportJob exportJob =
        new JdbcExportJob(context, null, null, oraOopOutputFormatClass);
    exportJob.runExport();
  }

  @Override
  public void updateTable(ExportJobContext context) throws IOException,
      ExportException {

    logExportTableDetails(context);

    context.setConnManager(this);

    @SuppressWarnings("rawtypes")
    Class<? extends OutputFormat> oraOopOutputFormatClass;
    try {
      oraOopOutputFormatClass = OraOopOutputFormatUpdate.class;
    } catch (NoClassDefFoundError ex) {
      explainWhyExportClassCannotBeLoaded(ex, "OraOopOutputFormatUpdate");
      throw ex;
    }

    JdbcUpdateExportJob exportJob =
        new JdbcUpdateExportJob(context, null, null, oraOopOutputFormatClass);
    exportJob.runExport();
  }

  @Override
  protected void finalize() throws Throwable {

    close();
    super.finalize();
  }

  @Override
  public String toHiveType(int sqlType) {

    String hiveType = super.toHiveType(sqlType);

    if (hiveType == null) {

      // http://wiki.apache.org/hadoop/Hive/Tutorial#Primitive_Types

      if (sqlType == OraOopOracleQueries.getOracleType("BFILE")
          || sqlType == OraOopOracleQueries.getOracleType("INTERVALYM")
          || sqlType == OraOopOracleQueries.getOracleType("INTERVALDS")
          || sqlType == OraOopOracleQueries.getOracleType("NCLOB")
          || sqlType == OraOopOracleQueries.getOracleType("NCHAR")
          || sqlType == OraOopOracleQueries.getOracleType("NVARCHAR")
          || sqlType == OraOopOracleQueries.getOracleType("OTHER")
          || sqlType == OraOopOracleQueries.getOracleType("ROWID")
          || sqlType == OraOopOracleQueries.getOracleType("TIMESTAMPTZ")
          || sqlType == OraOopOracleQueries.getOracleType("TIMESTAMPLTZ")
          || sqlType == OraOopOracleQueries.getOracleType("STRUCT")) {
        hiveType = "STRING";
      }

      if (sqlType == OraOopOracleQueries.getOracleType("BINARY_FLOAT")) {
        hiveType = "FLOAT";
      }

      if (sqlType == OraOopOracleQueries.getOracleType("BINARY_DOUBLE")) {
        hiveType = "DOUBLE";
      }
    }

    if (hiveType == null) {
      LOG.warn(String.format("%s should be updated to cater for data-type: %d",
          OraOopUtilities.getCurrentMethodName(), sqlType));
    }

    return hiveType;
  }

  @Override
  public String toJavaType(int sqlType) {

    String javaType = super.toJavaType(sqlType);

    if (sqlType == OraOopOracleQueries.getOracleType("TIMESTAMP")) {
      // Get the Oracle JDBC driver to convert this value to a string
      // instead of the generic JDBC driver.
      // If the generic JDBC driver is used, it will take into account the
      // timezone of the client machine's locale. The problem with this is that
      // timestamp data should not be associated with a timezone. In practice,
      // this
      // leads to problems, for example, the time '2010-10-03 02:01:00' being
      // changed to '2010-10-03 03:01:00' if the client machine's locale is
      // Melbourne.
      // (This is in response to daylight saving starting in Melbourne on
      // this date at 2am.)
      javaType = timestampJavaType;
    }

    if (sqlType == OraOopOracleQueries.getOracleType("TIMESTAMPTZ")) {
      // Returning "String" produces: "2010-08-08 09:00:00.0 +10:00"
      // Returning "java.sql.Timestamp" produces: "2010-08-08 09:00:00.0"

      // If we use "java.sql.Timestamp", the field's value will not
      // contain the timezone when converted to a string and written to the HDFS
      // CSV file.
      // I.e. Get the Oracle JDBC driver to convert this value to a string
      // instead of the generic JDBC driver...
      javaType = timestampJavaType;
    }

    if (sqlType == OraOopOracleQueries.getOracleType("TIMESTAMPLTZ")) {
      // Returning "String" produces:
      // "2010-08-08 09:00:00.0 Australia/Melbourne"
      // Returning "java.sql.Timestamp" produces: "2010-08-08 09:00:00.0"
      javaType = timestampJavaType;
    }

    /*
     * http://www.oracle.com/technology/sample_code/tech/java/sqlj_jdbc/files
     * /oracle10g/ieee/Readme.html
     *
     * BINARY_DOUBLE is a 64-bit, double-precision floating-point number
     * datatype. (IEEE 754) Each BINARY_DOUBLE value requires 9 bytes, including
     * a length byte. A 64-bit double format number X is divided as sign s 1-bit
     * exponent e 11-bits fraction f 52-bits
     *
     * BINARY_FLOAT is a 32-bit, single-precision floating-point number
     * datatype. (IEEE 754) Each BINARY_FLOAT value requires 5 bytes, including
     * a length byte. A 32-bit single format number X is divided as sign s 1-bit
     * exponent e 8-bits fraction f 23-bits
     */
    if (sqlType == OraOopOracleQueries.getOracleType("BINARY_FLOAT")) {
      // http://people.uncw.edu/tompkinsj/133/numbers/Reals.htm
      javaType = "Float";
    }

    if (sqlType == OraOopOracleQueries.getOracleType("BINARY_DOUBLE")) {
      // http://people.uncw.edu/tompkinsj/133/numbers/Reals.htm
      javaType = "Double";
    }

    if (sqlType == OraOopOracleQueries.getOracleType("STRUCT")) {
      // E.g. URITYPE
      javaType = "String";
    }

    if (javaType == null) {

      // For constant values, refer to:
      // http://oracleadvisor.com/documentation/oracle/database/11.2/
      //   appdev.112/e13995/constant-values.html#oracle_jdbc

      if (sqlType == OraOopOracleQueries.getOracleType("BFILE")
          || sqlType == OraOopOracleQueries.getOracleType("NCLOB")
          || sqlType == OraOopOracleQueries.getOracleType("NCHAR")
          || sqlType == OraOopOracleQueries.getOracleType("NVARCHAR")
          || sqlType == OraOopOracleQueries.getOracleType("ROWID")
          || sqlType == OraOopOracleQueries.getOracleType("INTERVALYM")
          || sqlType == OraOopOracleQueries.getOracleType("INTERVALDS")
          || sqlType == OraOopOracleQueries.getOracleType("OTHER")) {
        javaType = "String";
      }

    }

    if (javaType == null) {
      LOG.warn(String.format("%s should be updated to cater for data-type: %d",
          OraOopUtilities.getCurrentMethodName(), sqlType));
    }

    return javaType;
  }

  @Override
  public String timestampToQueryString(Timestamp ts) {

    return "TO_TIMESTAMP('" + ts + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
  }

  public OracleTable getOracleTableContext() {

    return OraOopUtilities.decodeOracleTableName(this.options.getUsername(),
        this.options.getTableName(), this.options.getConf());
  }

  @Override
  public Map<String, Integer> getColumnTypes(String tableName) {

    if (this.columnTypesInOracleTable == null) {

      Map<String, Integer> columnTypes = super.getColumnTypes(tableName);
      this.columnTypesInOracleTable = new HashMap<String, Integer>();

      List<String> colNames = getColumnNamesInOracleTable(tableName);

      for (int idx = 0; idx < colNames.size(); idx++) {

        String columnNameInTable = colNames.get(idx);
        if (columnTypes.containsKey(columnNameInTable)) {

          // Unescape the column names being returned...
          int colType = columnTypes.get(columnNameInTable);
          String key = unescapeOracleColumnName(columnNameInTable); // <- See
                                                                    // notes at
                                                                    // top about
                                                                    // escaped
                                                                    // column
                                                                    // names
          this.columnTypesInOracleTable.put(key, colType);
        }
      }
    }

    return this.columnTypesInOracleTable;
  }

  private boolean isEscaped(String name) {

    return name.startsWith("\"") && name.endsWith("\"");
  }

  private String escapeOracleColumnName(String columnName) {
    // See notes at top about escaped column names
    if (isEscaped(columnName)) {
      return columnName;
    } else {
      return "\"" + columnName + "\"";
    }
  }

  @Override
  public String escapeColName(String colName) {

    return super.escapeColName(colName); // <- See notes at top about escaped
                                         // column names
  }

  private String unescapeOracleColumnName(String columnName) {

    if (isEscaped(columnName)) {
      return columnName.substring(1, columnName.length() - 1);
    } else {
      return columnName;
    }
  }

  private void logImportTableDetails(ImportJobContext context) {

    Path outputDirectory = context.getDestination();
    if (outputDirectory != null) {
      LOG.debug("The output directory for the sqoop table import is : "
          + outputDirectory.getName());
    }

    // Indicate whether we can load the class named: OraOopOraStats
    showUserWhetherOraOopOraStatsIsAvailable(context.getOptions().getConf());
  }

  private void logExportTableDetails(ExportJobContext context) {

    // Indicate whether we can load the class named: OraOopOraStats
    showUserWhetherOraOopOraStatsIsAvailable(context.getOptions().getConf());

    // Indicate what the update/merge columns are...
    String[] updateKeyColumns =
        OraOopUtilities.getExportUpdateKeyColumnNames(context.getOptions());
    if (updateKeyColumns.length > 0) {
      LOG.info(String.format(
          "The column%s used to match rows in the HDFS file with rows in "
              + "the Oracle table %s: %s", updateKeyColumns.length > 1 ? "s"
              : "", updateKeyColumns.length > 1 ? "are" : "is", OraOopUtilities
              .stringArrayToCSV(updateKeyColumns)));
    }
  }

  private void showUserWhetherOraOopOraStatsIsAvailable(Configuration conf) {

    if (OraOopUtilities.userWantsOracleSessionStatisticsReports(conf)) {

      LOG.info(String.format("%s=true",
          OraOopConstants.ORAOOP_REPORT_SESSION_STATISTICS));

      // This will log a warning if it's unable to load the OraOopOraStats
      // class...
      OraOopUtilities.startSessionSnapshot(null);
    }
  }

  @Override
  protected String getCurTimestampQuery() {

    return "SELECT SYSTIMESTAMP FROM DUAL";
  }

  @Override
  protected void checkTableImportOptions(ImportJobContext context)
      throws IOException, ImportException {

    // Update the unit-test code if you modify this method.
    super.checkTableImportOptions(context);
  }

  private void explainWhyExportClassCannotBeLoaded(NoClassDefFoundError ex,
      String exportClassName) {

    String msg =
        String.format("Unable to load class %s.\n"
            + "This is most likely caused by the Cloudera Shim Jar "
            + "not being included in the Java Classpath.\n" + "Either:\n"
            + "\tUse \"-libjars\" on the Sqoop command-line to "
            + "include the Cloudera shim jar in the Java Classpath; or"
            + "\n\tCopy the Cloudera shim jar into the Sqoop/lib "
            + "directory so that it is automatically included in the "
            + "Java Classpath; or\n"
            + "\tObtain an updated version of Sqoop that addresses "
            + "the Sqoop Jira \"SQOOP-127\".\n" + "\n"
            + "The Java Classpath is:\n%s", exportClassName, OraOopUtilities
            .getJavaClassPath());
    LOG.fatal(msg, ex);
  }
  /**
   * Determine if HCat integration from direct mode of the connector is
   * allowed.  By default direct mode is not compatible with HCat
   * @return Whether direct mode is allowed.
   */
  @Override
  public boolean isDirectModeHCatSupported() {
    return true;
  }

  /**
   * Determine if HBase operations from direct mode of the connector is
   * allowed.  By default direct mode is not compatible with HBase
   * @return Whether direct mode is allowed.
   */
  public boolean isDirectModeHBaseSupported() {
    return true;
  }

  /**
   * Determine if Accumulo operations from direct mode of the connector is
   * allowed.  By default direct mode is not compatible with HBase
   * @return Whether direct mode is allowed.
   */
  public boolean isDirectModeAccumuloSupported() {
    return true;
  }
}
