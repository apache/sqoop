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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;

/**
 * Contains the queries to get data dictionary information from Oracle database.
 */
public final class OraOopOracleQueries {

  private static final OraOopLog LOG = OraOopLogFactory
      .getLog(OraOopOracleQueries.class);

  private static Class<?> oracleConnectionClass;
  private static Class<?> oracleStatementClass;
  private static Class<?> oracleResultSetClass;
  private static Class<?> oracleTypesClass;
  private static Class<?> oracleDateClass;
  private static Method methSetLongAtName;
  private static Method methSetBigDecimalAtName;
  private static Method methSetStringAtName;
  private static Method methSetTimestampAtName;
  private static Method methSetBinaryDoubleAtName;
  private static Method methSetObjectAtName;
  private static Method methSetBinaryFloatAtName;
  private static Method methSetIntAtName;

  private static final Map<String, Integer> ORACLE_TYPES =
      new HashMap<String, Integer>();

  static {
    try {
      oracleStatementClass =
          Class.forName("oracle.jdbc.OraclePreparedStatement");
      methSetLongAtName =
          oracleStatementClass.getMethod("setLongAtName", String.class,
              long.class);
      methSetBigDecimalAtName =
          oracleStatementClass.getMethod("setBigDecimalAtName", String.class,
              BigDecimal.class);
      methSetStringAtName =
          oracleStatementClass.getMethod("setStringAtName", String.class,
              String.class);
      methSetTimestampAtName =
          oracleStatementClass.getMethod("setTimestampAtName", String.class,
              Timestamp.class);
      methSetBinaryDoubleAtName =
          oracleStatementClass.getMethod("setBinaryDoubleAtName", String.class,
              double.class);
      methSetObjectAtName =
          oracleStatementClass.getMethod("setObjectAtName", String.class,
              Object.class);
      methSetBinaryFloatAtName =
          oracleStatementClass.getMethod("setBinaryFloatAtName", String.class,
              float.class);
      methSetIntAtName =
          oracleStatementClass.getMethod("setIntAtName", String.class,
              int.class);

      oracleResultSetClass = Class.forName("oracle.jdbc.OracleResultSet");
      oracleDateClass = Class.forName("oracle.sql.DATE");
      oracleConnectionClass = Class.forName("oracle.jdbc.OracleConnection");
      oracleTypesClass = Class.forName("oracle.jdbc.OracleTypes");
    } catch (Exception e) {
      throw new RuntimeException(
          "Problem getting Oracle JDBC methods via reflection.", e);
    }
  }

  private OraOopOracleQueries() {
  }

  protected static void setJdbcFetchSize(Connection connection,
      org.apache.hadoop.conf.Configuration conf) {

    int fetchSize =
        conf.getInt(OraOopConstants.ORACLE_ROW_FETCH_SIZE,
            OraOopConstants.ORACLE_ROW_FETCH_SIZE_DEFAULT);
    try {
      Method methSetPrefetch =
          oracleConnectionClass.getMethod("setDefaultRowPrefetch", int.class);
      methSetPrefetch.invoke(connection, fetchSize);

      String msg =
          "The Oracle connection has had its default row fetch size set to : "
              + fetchSize;
      if (fetchSize == OraOopConstants.ORACLE_ROW_FETCH_SIZE_DEFAULT) {
        LOG.debug(msg);
      } else {
        LOG.info(msg);
      }
    } catch (Exception ex) {
      LOG.warn(
          String
              .format(
                  "Unable to configure the DefaultRowPrefetch of the "
                + "Oracle connection in %s.",
                  OraOopUtilities.getCurrentMethodName()), ex);
    }

  }

  public static void setConnectionTimeZone(Connection connection,
      Configuration conf) {
    String timeZoneString = conf.get("oracle.sessionTimeZone", "GMT");
    setConnectionTimeZone(connection, timeZoneString);
  }

  public static void setConnectionTimeZone(Connection connection,
      String timeZone) {
    TimeZone timeZoneObj = TimeZone.getTimeZone(timeZone);
    try {
      Method methSession =
          oracleConnectionClass.getMethod("setSessionTimeZone", String.class);
      Method methDefault =
          oracleConnectionClass.getMethod("setDefaultTimeZone", TimeZone.class);
      methSession.invoke(connection, timeZoneObj.getID());
      methDefault.invoke(connection, timeZoneObj);
      TimeZone.setDefault(timeZoneObj);
      LOG.info("Session Time Zone set to " + timeZoneObj.getID());
    } catch (Exception e) {
      LOG.error("Error setting time zone: " + e.getMessage());
    }
  }

  public static OracleTablePartitions getPartitions(Connection connection,
      OracleTable table) throws SQLException {

    OracleTablePartitions result = new OracleTablePartitions();

    PreparedStatement statement =
      connection
        .prepareStatement("with"
        + " partitions as"
        + "   (select table_owner, table_name, partition_name"
        +  "   from dba_tab_partitions"
        + "   where"
        + "   table_owner = ? and"
        + "   table_name = ?),"
        + " subpartitions as"
        + "  (select table_owner, table_name, partition_name, subpartition_name"
        + "  from dba_tab_subpartitions"
        + "  where"
        + "  table_owner = ? and"
        + "  table_name = ?)"
        + " select"
        + "   partitions.partition_name,"
        + "   subpartitions.subpartition_name"
        + " from partitions left outer join subpartitions on"
        + " (partitions.table_owner = subpartitions.table_owner"
        + " and partitions.table_name = subpartitions.table_name"
        + " and partitions.partition_name = subpartitions.partition_name)"
        + " order by partition_name, subpartition_name");

    statement.setString(1, table.getSchema());
    statement.setString(2, table.getName());
    statement.setString(3, table.getSchema());
    statement.setString(4, table.getName());

    ResultSet resultSet = statement.executeQuery();

    OracleTablePartition partition = null;
    while (resultSet.next()) {
      String partitionName = resultSet.getString("partition_name");
      String subPartitionName = resultSet.getString("subpartition_name");

      if (subPartitionName != null &&  !("".equals(subPartitionName))) {
        partition = new OracleTablePartition(subPartitionName, true);
        result.add(partition);
      } else {
        if (partition == null || partition.isSubPartition()
            || partition.getName() != partitionName) {
          partition = new OracleTablePartition(partitionName, false);
          result.add(partition);
        }
      }
    }

    resultSet.close();
    statement.close();

    return result;
  }

  public static int getOracleDataObjectNumber(Connection connection,
      OracleTable table) throws SQLException {

    PreparedStatement statement =
        connection.prepareStatement("SELECT data_object_id "
            + " FROM dba_objects" + " WHERE owner = ?" + " and object_name = ?"
            + " and object_type = ?");
    statement.setString(1, table.getSchema());
    statement.setString(2, table.getName());
    statement.setString(3, "TABLE");

    ResultSet resultSet = statement.executeQuery();

    resultSet.next();
    int result = resultSet.getInt("data_object_id");

    resultSet.close();
    statement.close();

    return result;
  }

  private static String getPartitionBindVars(List<String> partitionList) {
    String result = "";
    for (int i = 1; i <= partitionList.size(); i++) {
      result += (i > 1) ? "," : "";
      result += ":part" + i;
    }
    return result;
  }

  private static void bindPartitionBindVars(PreparedStatement statement,
      List<String> partitionList) throws SQLException {
    int i = 0;
    for (String partition : partitionList) {
      i++;
      OraOopOracleQueries.setStringAtName(statement, "part" + i, partition);
    }
  }

  public static List<OraOopOracleDataChunkPartition>
      getOracleDataChunksPartition(Connection connection, OracleTable table,
          List<String> partitionList) throws SQLException {
    List<OraOopOracleDataChunkPartition> result =
        new ArrayList<OraOopOracleDataChunkPartition>();
    String sql =
        "SELECT "
          + "  pl.partition_name, "
          + "  pl.is_subpartition, "
          + "  s.blocks "
          + "FROM "
          + "  (SELECT tp.table_owner, "
          + "    tp.table_name, "
          + "    NVL(tsp.subpartition_name,tp.partition_name) partition_name, "
          + "    nvl2(tsp.subpartition_name,1,0) is_subpartition "
          + "  FROM dba_tab_partitions tp, "
          + "    dba_tab_subpartitions tsp "
          + "  WHERE tp.table_owner     = :table_owner"
          + "  AND tp.table_name        = :table_name"
          + "  AND tsp.table_owner(+)   =tp.table_owner "
          + "  AND tsp.table_name(+)    =tp.table_name "
          + "  AND tsp.partition_name(+)=tp.partition_name ";

    if (partitionList != null && partitionList.size() > 0) {
      sql +=
          " AND tp.partition_name IN (" + getPartitionBindVars(partitionList)
              + ") ";
    }

    sql += "  ) pl, dba_tables t, dba_segments s "
        + "WHERE t.owner=pl.table_owner "
        + "AND t.table_name=pl.table_name "
        + "AND ( "
        + "     (t.iot_type='IOT' AND (s.owner,s.segment_name)= "
        + "                          (SELECT c.index_owner,c.index_name "
        + "                             FROM dba_constraints c "
        + "                            WHERE c.owner=pl.table_owner "
        + "                              AND c.table_name=pl.table_name "
        + "                              AND c.constraint_type='P')) "
        + "     OR (t.iot_type IS NULL "
        + "         AND s.owner=t.owner "
        + "         AND s.segment_name=t.table_name) "
        + "    ) "
        + "AND s.partition_name=pl.partition_name";

    PreparedStatement statement = connection.prepareStatement(sql);
    OraOopOracleQueries.setStringAtName(statement, "table_owner", table
        .getSchema());
    OraOopOracleQueries.setStringAtName(statement, "table_name", table
        .getName());

    if (partitionList != null && partitionList.size() > 0) {
      bindPartitionBindVars(statement, partitionList);
    }

    trace(String.format("%s SQL Query =\n%s", OraOopUtilities
        .getCurrentMethodName(), sql.replace(":table_owner", table.getSchema())
        .replace(":table_name", table.getName())));

    ResultSet resultSet = statement.executeQuery();

    while (resultSet.next()) {
      OraOopOracleDataChunkPartition dataChunk =
          new OraOopOracleDataChunkPartition(resultSet
              .getString("partition_name"), resultSet
              .getBoolean("is_subpartition"), resultSet.getLong("blocks"));
      result.add(dataChunk);
    }
    resultSet.close();
    statement.close();
    return result;
  }

  public static List<OraOopOracleDataChunkExtent> getOracleDataChunksExtent(
      Configuration conf, Connection connection, OracleTable table,
      List<String> partitionList, int numberOfChunksPerOracleDataFile)
      throws SQLException {

    List<OraOopOracleDataChunkExtent> result =
        new ArrayList<OraOopOracleDataChunkExtent>();

    String sql =
        "SELECT data_object_id, "
          + "file_id, "
          + "relative_fno, "
          + "file_batch, "
          + "MIN (start_block_id) start_block_id, "
          + "MAX (end_block_id) end_block_id, "
          + "SUM (blocks) blocks "
          + "FROM (SELECT o.data_object_id, "
          + "e.file_id, "
          + "e.relative_fno, "
          + "e.block_id start_block_id, "
          + "e.block_id + e.blocks - 1 end_block_id, "
          + "e.blocks, "
          + "CEIL ( "
          + "   SUM ( "
          + "      e.blocks) "
          + "   OVER (PARTITION BY o.data_object_id, e.file_id "
          + "         ORDER BY e.block_id ASC) "
          + "   / (SUM (e.blocks) "
          + "         OVER (PARTITION BY o.data_object_id, e.file_id) "
          + "      / :numchunks)) "
          + "   file_batch "
          + "FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp "
          + "WHERE     o.owner = :owner "
          + "AND o.object_name = :object_name "
          + "AND e.owner = :owner "
          + "AND e.segment_name = :object_name "
          + "AND o.owner = e.owner "
          + "AND o.object_name = e.segment_name "
          + "AND (o.subobject_name = e.partition_name "
          + "     OR (o.subobject_name IS NULL AND e.partition_name IS NULL)) "
          + "AND o.owner = tsp.table_owner(+) "
          + "AND o.object_name = tsp.table_name(+) "
          + "AND o.subobject_name = tsp.subpartition_name(+) ";

    if (partitionList != null && partitionList.size() > 0) {
      sql +=
          " AND case when o.object_type='TABLE SUBPARTITION' then "
          + "tsp.partition_name else o.subobject_name end IN ("
              + getPartitionBindVars(partitionList) + ") ";
    }

    sql +=
        ") " + "GROUP BY data_object_id, " + "         file_id, "
            + "         relative_fno, " + "         file_batch "
            + "ORDER BY data_object_id, " + "         file_id, "
            + "         relative_fno, " + "         file_batch";

    sql = conf.get(OraOopConstants.ORAOOP_ORACLE_DATA_CHUNKS_QUERY, sql);

    PreparedStatement statement = connection.prepareStatement(sql);
    OraOopOracleQueries.setIntAtName(statement, "numchunks",
        numberOfChunksPerOracleDataFile);
    OraOopOracleQueries.setStringAtName(statement, "owner", table.getSchema());
    OraOopOracleQueries.setStringAtName(statement, "object_name", table
        .getName());

    if (partitionList != null && partitionList.size() > 0) {
      bindPartitionBindVars(statement, partitionList);
    }

    trace(String.format("%s SQL Query =\n%s", OraOopUtilities
        .getCurrentMethodName(), sql.replace(":numchunks",
        Integer.toString(numberOfChunksPerOracleDataFile)).replace(":owner",
        table.getSchema()).replace(":object_name", table.getName())));

    ResultSet resultSet = statement.executeQuery();

    while (resultSet.next()) {
      int fileId = resultSet.getInt("relative_fno");
      int fileBatch = resultSet.getInt("file_batch");
      String dataChunkId =
          OraOopUtilities.generateDataChunkId(fileId, fileBatch);
      OraOopOracleDataChunkExtent dataChunk =
          new OraOopOracleDataChunkExtent(dataChunkId, resultSet
              .getInt("data_object_id"), resultSet.getInt("relative_fno"),
              resultSet.getLong("start_block_id"), resultSet
                  .getLong("end_block_id"));
      result.add(dataChunk);
    }

    resultSet.close();
    statement.close();

    return result;
  }

  private static void trace(String message) {

    LOG.debug(message);
  }

  public static String getOracleObjectType(Connection connection,
      OracleTable table) throws SQLException {

    PreparedStatement statement =
        connection.prepareStatement("SELECT object_type " + " FROM dba_objects"
            + " WHERE owner = ?" + " and object_name = ?");
    statement.setString(1, table.getSchema());
    statement.setString(2, table.getName());

    ResultSet resultSet = statement.executeQuery();

    String result = null;
    if (resultSet.next()) {
      result = resultSet.getString("object_type");
    }

    resultSet.close();
    statement.close();

    return result;
  }

  public static OracleVersion getOracleVersion(Connection connection)
      throws SQLException {

    String sql =
        "SELECT \n"
      + "  v.banner, \n"
      + "  rtrim(v.version)      full_version, \n"
      + "  rtrim(v.version_bit) version_bit, \n"
      + "  SUBSTR(v.version, 1, INSTR(v.version, '.', 1, 1)-1) major, \n"
      + "  SUBSTR(v.version, INSTR(v.version, '.', 1, 1) + 1, "
      + "  INSTR(v.version, '.', 1, 2) - INSTR(v.version, '.', 1, 1) - 1) "
      + "    minor, \n"
      + "  SUBSTR(v.version, INSTR(v.version, '.', 1, 2) + 1, "
      + "  INSTR(v.version, '.', 1, 3) - INSTR(v.version, '.', 1, 2) - 1) "
      + "    version, \n"
      + "  SUBSTR(v.version, INSTR(v.version, '.', 1, 3) + 1, "
      + "  INSTR(v.version, '.', 1, 4) - INSTR(v.version, '.', 1, 3) - 1) "
      + "    patch, \n"
      + "  DECODE(instr(v.banner, '64bit'), 0, 'False', 'True') isDb64bit, \n"
      + "  DECODE(instr(b.banner, 'HPUX'), 0, 'False', 'True') isHPUX \n"
      + "FROM (SELECT rownum row_num, \n"
      + "   banner,\n"
      + "   SUBSTR(SUBSTR(banner,INSTR(banner,'Release ')+8), 1) version_bit,\n"
      + "   SUBSTR(SUBSTR(banner,INSTR(banner,'Release ')+8), 1,\n"
      + "    INSTR(SUBSTR(banner,INSTR(banner,'Release ')+8),' ')) version\n"
      + "FROM v$version\n" + "  WHERE banner LIKE 'Oracle%'\n"
      + "     OR banner LIKE 'Personal Oracle%') v,\n" + "v$version b\n"
      + "  WHERE v.row_num = 1\n" + "  and b.banner like 'TNS%'\n";

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    resultSet.next();
    OracleVersion result =
        new OracleVersion(resultSet.getInt("major"), resultSet.getInt("minor"),
            resultSet.getInt("version"), resultSet.getInt("patch"), resultSet
                .getString("banner"));

    resultSet.close();
    statement.close();

    return result;
  }

  public static List<OracleTable> getTables(Connection connection)
      throws SQLException {

    return getTables(connection, null, null, TableNameQueryType.Equals);
  }

  private enum GetTablesOptions {
    Owner, Table
  }

  private enum TableNameQueryType {
    Equals, Like
  }

  public static List<OracleTable>
      getTables(Connection connection, String owner) throws SQLException {

    return getTables(connection, owner, null, TableNameQueryType.Equals);
  }

  public static OracleTable getTable(Connection connection, String owner,
      String tableName) throws SQLException {

    List<OracleTable> tables =
        getTables(connection, owner, tableName, TableNameQueryType.Equals);
    if (tables.size() > 0) {
      return tables.get(0);
    }

    return null;
  }

  public static List<OracleTable> getTablesWithTableNameLike(
      Connection connection, String owner, String tableNameLike)
      throws SQLException {

    return getTables(connection, owner, tableNameLike, TableNameQueryType.Like);
  }

  private static List<OracleTable> getTables(Connection connection,
      String owner, String tableName, TableNameQueryType tableNameQueryType)
      throws SQLException {

    EnumSet<GetTablesOptions> options = EnumSet.noneOf(GetTablesOptions.class);

    if (owner != null && !owner.isEmpty()) {
      options.add(GetTablesOptions.Owner);
    }

    if (tableName != null && !tableName.isEmpty()) {
      options.add(GetTablesOptions.Table);
    }

    String sql =
        "SELECT owner, table_name " + " FROM dba_tables" + " %s %s %s %s "
            + " ORDER BY owner, table_name";

    String tableComparitor = null;
    switch (tableNameQueryType) {
      case Equals:
        tableComparitor = "=";
        break;
      case Like:
        tableComparitor = "LIKE";
        break;
      default:
        throw new RuntimeException("Operator not implemented.");
    }

    sql =
        String.format(sql, options.isEmpty() ? "" : "WHERE", options
            .contains(GetTablesOptions.Owner) ? "owner = ?" : "", options
            .containsAll(EnumSet.of(GetTablesOptions.Owner,
                GetTablesOptions.Table)) ? "AND" : "", options
            .contains(GetTablesOptions.Table) ? String.format(
            "table_name %s ?", tableComparitor) : "");

    PreparedStatement statement = connection.prepareStatement(sql);

    if (options.containsAll(EnumSet.of(GetTablesOptions.Owner,
        GetTablesOptions.Table))) {
      statement.setString(1, owner);
      statement.setString(2, tableName);
    } else {
      if (options.contains(GetTablesOptions.Owner)) {
        statement.setString(1, owner);
      } else if (options.contains(GetTablesOptions.Table)) {
        statement.setString(1, tableName);
      }
    }

    ResultSet resultSet = statement.executeQuery();

    ArrayList<OracleTable> result = new ArrayList<OracleTable>();
    while (resultSet.next()) {
      result.add(new OracleTable(resultSet.getString("owner"), resultSet
          .getString("table_name")));
    }

    resultSet.close();
    statement.close();

    return result;
  }

  public static List<String> getTableColumnNames(Connection connection,
      OracleTable table) throws SQLException {

    OracleTableColumns oracleTableColumns = getTableColumns(connection, table);
    List<String> result = new ArrayList<String>(oracleTableColumns.size());

    for (int idx = 0; idx < oracleTableColumns.size(); idx++) {
      result.add(oracleTableColumns.get(idx).getName());
    }

    return result;
  }

  public static List<String> getTableColumnNames(Connection connection,
      OracleTable table, boolean omitLobAndLongColumnsDuringImport,
      OraOopConstants.Sqoop.Tool sqoopTool, boolean onlyOraOopSupportedTypes,
      boolean omitOraOopPseudoColumns) throws SQLException {

    OracleTableColumns oracleTableColumns =
        getTableColumns(connection, table, omitLobAndLongColumnsDuringImport,
            sqoopTool, onlyOraOopSupportedTypes, omitOraOopPseudoColumns);

    List<String> result = new ArrayList<String>(oracleTableColumns.size());

    for (int idx = 0; idx < oracleTableColumns.size(); idx++) {
      result.add(oracleTableColumns.get(idx).getName());
    }

    return result;
  }

  private static OracleTableColumns getTableColumns(Connection connection,
      OracleTable table, boolean omitLobColumns, String dataTypesClause,
      HashSet<String> columnNamesToOmit) throws SQLException {

    String sql =
        "SELECT column_name, data_type " + " FROM dba_tab_columns"
            + " WHERE owner = ?" + " and table_name = ?" + " %s"
            + " ORDER BY column_id";

    sql =
        String.format(sql, dataTypesClause == null ? "" : " and "
            + dataTypesClause);

    LOG.debug(String.format("%s : sql = \n%s", OraOopUtilities
        .getCurrentMethodName(), sql));

    OracleTableColumns result = new OracleTableColumns();
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setString(1, getTableSchema(connection, table));
    statement.setString(2, table.getName());

    ResultSet resultSet = statement.executeQuery();

    while (resultSet.next()) {

      String columnName = resultSet.getString("column_name");

      if (columnNamesToOmit != null) {
        if (columnNamesToOmit.contains(columnName)) {
          continue;
        }
      }

      result.add(new OracleTableColumn(columnName, resultSet
          .getString("data_type")));
    }

    resultSet.close();
    statement.close();

    // Now get the actual JDBC data-types for these columns...
    StringBuilder columnList = new StringBuilder();
    for (int idx = 0; idx < result.size(); idx++) {
      if (idx > 0) {
        columnList.append(",");
      }
      columnList.append(result.get(idx).getName());
    }
    sql =
        String.format("SELECT %s FROM %s WHERE 0=1", columnList.toString(),
            table.toString());
    Statement statementDesc = connection.createStatement();
    ResultSet resultSetDesc = statementDesc.executeQuery(sql);
    ResultSetMetaData metaData = resultSetDesc.getMetaData();
    for (int idx = 0; idx < metaData.getColumnCount(); idx++) {
      result.get(idx).setOracleType(metaData.getColumnType(idx + 1)); // <- JDBC
                                                                    // is
                                                                    // 1-based
    }
    resultSetDesc.close();
    statementDesc.close();

    return result;
  }

  public static OracleTableColumns getTableColumns(Connection connection,
      OracleTable table) throws SQLException {

    return getTableColumns(connection, table, false, null // <- dataTypesClause
        , null); // <-columnNamesToOmit
  }

  public static OracleTableColumns getTableColumns(Connection connection,
      OracleTable table, boolean omitLobAndLongColumnsDuringImport,
      OraOopConstants.Sqoop.Tool sqoopTool, boolean onlyOraOopSupportedTypes,
      boolean omitOraOopPseudoColumns) throws SQLException {

    String dataTypesClause = "";
    HashSet<String> columnNamesToOmit = null;

    if (onlyOraOopSupportedTypes) {

      switch (sqoopTool) {

        case UNKNOWN:
          throw new InvalidParameterException(
              "The sqoopTool parameter must not be \"UNKNOWN\".");

        case IMPORT:
          dataTypesClause =
              OraOopConstants.SUPPORTED_IMPORT_ORACLE_DATA_TYPES_CLAUSE;

          if (omitLobAndLongColumnsDuringImport) {
            LOG.info("LOB and LONG columns are being omitted from the Import.");
            dataTypesClause =
                " DATA_TYPE not in ('BLOB', 'CLOB', 'NCLOB', 'LONG') and "
                    + dataTypesClause;
          }
          break;

        case EXPORT:
          dataTypesClause =
              OraOopConstants.SUPPORTED_EXPORT_ORACLE_DATA_TYPES_CLAUSE;
          break;

        default:
          throw new InvalidParameterException("Sqoop Tool not implemented.");

      }
    }

    if (omitOraOopPseudoColumns) {

      switch (sqoopTool) {

        case EXPORT:
          if (columnNamesToOmit == null) {
            columnNamesToOmit = new HashSet<String>();
          }
          columnNamesToOmit.add(OraOopConstants.COLUMN_NAME_EXPORT_PARTITION);
          columnNamesToOmit
              .add(OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION);
          columnNamesToOmit.add(OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW);
          break;
        default:
          // Only applicable for export.
          break;
      }
    }

    return getTableColumns(connection, table,
        omitLobAndLongColumnsDuringImport, dataTypesClause, columnNamesToOmit);
  }

  public static List<OracleActiveInstance> getOracleActiveInstances(
      Connection connection) throws SQLException {

    // Returns null if there are no rows in v$active_instances - which indicates
    // this Oracle database is not a RAC.
    ArrayList<OracleActiveInstance> result = null;

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select inst_name from v$active_instances ");

    while (resultSet.next()) {
      String instName = resultSet.getString("inst_name");
      String[] nameFragments = instName.split(":");

      if (nameFragments.length != 2) {
        throw new SQLException(
            "Parsing Error: The inst_name column of v$active_instances does "
            + "not contain two values separated by a colon.");
      }

      String hostName = nameFragments[0].trim();
      String instanceName = nameFragments[1].trim();

      if (hostName.isEmpty()) {
        throw new SQLException(
            "Parsing Error: The inst_name column of v$active_instances does "
            + "not include a host name.");
      }

      if (instanceName.isEmpty()) {
        throw new SQLException(
            "Parsing Error: The inst_name column of v$active_instances does "
            + "not include an instance name.");
      }

      OracleActiveInstance instance = new OracleActiveInstance();
      instance.setHostName(hostName);
      instance.setInstanceName(instanceName);

      if (result == null) {
        result = new ArrayList<OracleActiveInstance>();
      }

      result.add(instance);
    }

    resultSet.close();
    statement.close();
    return result;
  }

  public static String getCurrentOracleInstanceName(Connection connection)
      throws SQLException {

    String result = "";

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select instance_name from v$instance");

    if (resultSet.next()) {
      result = resultSet.getString("instance_name");
    }

    resultSet.close();
    statement.close();
    return result;
  }

  public static Object getSysDate(Connection connection) throws SQLException {
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select sysdate from dual");

    resultSet.next();
    try {
      Method method = oracleResultSetClass.getMethod("getDATE", int.class);
      return method.invoke(resultSet, 1);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not get sysdate", e);
      }
    } finally {
      resultSet.close();
      statement.close();
    }
  }

  public static String oraDATEToString(Object date, String format) {
    try {
      Method dateMethod =
          oracleDateClass.getMethod("toText", String.class, String.class);
      return (String) dateMethod.invoke(date, format, null);
    } catch (Exception e) {
      throw new RuntimeException(String.format(
          "Unable to convert the oracle.sql.DATE value \"%s\" to text.", date
              .toString()), e);
    }
  }

  public static Object oraDATEFromString(String date, String format) {
    try {
      Method dateMethod =
          oracleDateClass.getMethod("fromText", String.class, String.class,
              String.class);
      return dateMethod.invoke(null, date, format, null);
    } catch (Exception e) {
      throw new RuntimeException(String
          .format(
              "Unable to convert the String value \"%s\" to oracle.sql.DATE.",
              date), e);
    }
  }

  public static Date oraDATEToDate(Object date) {
    try {
      Method dateMethod = oracleDateClass.getMethod("dateValue");
      return (Date) dateMethod.invoke(date);
    } catch (Exception e) {
      throw new RuntimeException("Could not get sysdate", e);
    }
  }

  public static String getSysTimeStamp(Connection connection)
      throws SQLException {

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select systimestamp from dual");

    resultSet.next();

    try {
      Method method = oracleResultSetClass.getMethod("getTIMESTAMP", int.class);
      Object timestamp = method.invoke(resultSet, 1);
      return timestamp.toString();
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not get sysdate", e);
      }
    } finally {
      resultSet.close();
      statement.close();
    }
  }

  public static boolean isTableAnIndexOrganizedTable(Connection connection,
      OracleTable table) throws SQLException {

    /*
     * http://ss64.com/orad/DBA_TABLES.html IOT_TYPE: If index-only table,then
     * IOT_TYPE is IOT or IOT_OVERFLOW or IOT_MAPPING else NULL
     */

    boolean result = false;

    PreparedStatement statement =
        connection.prepareStatement("select iot_type " + "from dba_tables "
            + "where owner = ? " + "and table_name = ?");
    statement.setString(1, table.getSchema());
    statement.setString(2, table.getName());
    ResultSet resultSet = statement.executeQuery();

    if (resultSet.next()) {
      String iotType = resultSet.getString("iot_type");
      result = iotType != null && !iotType.isEmpty();
    }

    resultSet.close();
    statement.close();

    return result;
  }

  public static void dropTable(Connection connection, OracleTable table)
      throws SQLException {

    String sql = String.format("DROP TABLE %s", table.toString());

    Statement statement = connection.createStatement();
    try {
      statement.execute(sql);
    } catch (SQLException ex) {
      if (ex.getErrorCode() != 942) { // ORA-00942: table or view does not exist
        throw ex;
      }
    }
    statement.close();
  }

  public static void
      exchangeSubpartition(Connection connection, OracleTable table,
          String subPartitionName, OracleTable subPartitionTable)
          throws SQLException {

    Statement statement = connection.createStatement();
    String sql =
        String.format("ALTER TABLE %s EXCHANGE SUBPARTITION %s WITH TABLE %s",
            table.toString(), subPartitionName, subPartitionTable.toString());
    statement.execute(sql);
    statement.close();
  }

  public static void createExportTableFromTemplate(Connection connection,
      OracleTable newTable, String tableStorageClause,
      OracleTable templateTable, boolean noLogging) throws SQLException {

    String sql =
        String.format("CREATE TABLE %s \n" + "%s %s \n" + "AS \n"
            + "(SELECT * FROM %s WHERE 0=1)", newTable.toString(),
            noLogging ? "NOLOGGING" : "", tableStorageClause, templateTable
                .toString());

    Statement statement = connection.createStatement();
    statement.execute(sql);
    statement.close();
  }

  private static Object oraDATEAddJulianDays(Object date, int julianDay,
      int julianSec) {
    try {
      Constructor<?> dateCon = oracleDateClass.getConstructor(byte[].class);
      Method dateBytes = oracleDateClass.getMethod("toBytes");
      Object result = dateCon.newInstance(dateBytes.invoke(date));
      Method dateAdd =
          oracleDateClass.getMethod("addJulianDays", int.class, int.class);
      result = dateAdd.invoke(result, julianDay, julianSec);
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Could not add days to date.", e);
    }
  }

  public static void createExportTableFromTemplateWithPartitioning(
      Connection connection, OracleTable newTable, String tableStorageClause,
      OracleTable templateTable, boolean noLogging, String partitionName,
      Object jobDateTime, int numberOfMappers, String[] subPartitionNames)
      throws SQLException {

    String dateFormat = "yyyy-mm-dd hh24:mi:ss";

    Object partitionBound =
        OraOopOracleQueries.oraDATEAddJulianDays(jobDateTime, 0, 1);

    String partitionBoundStr =
        OraOopOracleQueries.oraDATEToString(partitionBound, dateFormat);

    StringBuilder subPartitions = new StringBuilder();
    for (int idx = 0; idx < numberOfMappers; idx++) {
      if (idx > 0) {
        subPartitions.append(",");
      }

      subPartitions.append(String.format(" SUBPARTITION %s VALUES (%d)",
          subPartitionNames[idx], idx));
    }

    String sql =
        String.format(
            "CREATE TABLE %s \n" + "%s %s \n" + "PARTITION BY RANGE (%s) \n"
                + "SUBPARTITION BY LIST (%s) \n" + "(PARTITION %s \n"
                + "VALUES LESS THAN (to_date('%s', '%s')) \n" + "( %s ) \n"
                + ") \n" + "AS \n"
                + "(SELECT t.*, sysdate %s, 0 %s, 0 %s FROM %s t \n"
                + "WHERE 0=1)", newTable.toString(), noLogging ? "NOLOGGING"
                : "", tableStorageClause,
            OraOopConstants.COLUMN_NAME_EXPORT_PARTITION,
            OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION, partitionName,
            partitionBoundStr, dateFormat, subPartitions.toString(),
            OraOopConstants.COLUMN_NAME_EXPORT_PARTITION,
            OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION,
            OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW, templateTable
                .toString());

    LOG.debug(String.format("SQL generated by %s:\n%s", OraOopUtilities
        .getCurrentMethodName(), sql));

    try {

      // Create the main export table...
      PreparedStatement preparedStatement = connection.prepareStatement(sql);
      preparedStatement.execute(sql);
      preparedStatement.close();
    } catch (SQLException ex) {
      LOG.error(String
          .format(
              "The error \"%s\" was encountered when executing the following "
              + "SQL statement:\n%s",
              ex.getMessage(), sql));
      throw ex;
    }
  }

  public static void createExportTableForMapper(Connection connection,
      OracleTable table, String tableStorageClause, OracleTable templateTable,
      boolean addOraOopPartitionColumns) throws SQLException {

    String sql = "";
    try {

      // Create the N tables to be used by the mappers...
      Statement statement = connection.createStatement();
      if (addOraOopPartitionColumns) {
        sql =
            String.format("CREATE TABLE %s \n" + "NOLOGGING %s \n" + "AS \n"
                + "(SELECT t.*, SYSDATE %s, 0 %s, 0 %s FROM %s t WHERE 0=1)",
                table.toString(), tableStorageClause,
                OraOopConstants.COLUMN_NAME_EXPORT_PARTITION,
                OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION,
                OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW, templateTable
                    .toString());
      } else {
        sql =
            String.format("CREATE TABLE %s \n" + "NOLOGGING %s \n" + "AS \n"
                + "(SELECT * FROM %s WHERE 0=1)", table.toString(),
                tableStorageClause, templateTable.toString());
      }

      LOG.info(String.format("SQL generated by %s:\n%s", OraOopUtilities
          .getCurrentMethodName(), sql));

      statement.execute(sql);
      statement.close();
    } catch (SQLException ex) {
      LOG.error(String
          .format(
              "The error \"%s\" was encountered when executing the following "
              + "SQL statement:\n%s",
              ex.getMessage(), sql));
      throw ex;
    }
  }

  // public static void createExportTablesForMappers(Connection connection
  // ,String[] mapperTableNames
  // ,OracleTable templateTable
  // ,boolean addOraOopPartitionColumns)
  // throws SQLException {
  //
  // for(String tableName : mapperTableNames)
  // createExportTableForMapper(connection, tableName, templateTable,
  // addOraOopPartitionColumns);
  // }

  public static void createMoreExportTablePartitions(Connection connection,
      OracleTable table, String partitionName, Object jobDateTime,
      String[] subPartitionNames) throws SQLException {

    String dateFormat = "yyyy-mm-dd hh24:mi:ss";

    Object partitionBound =
        OraOopOracleQueries.oraDATEAddJulianDays(jobDateTime, 0, 1);
    String partitionBoundStr =
        OraOopOracleQueries.oraDATEToString(partitionBound, dateFormat);

    StringBuilder subPartitions = new StringBuilder();
    for (int idx = 0; idx < subPartitionNames.length; idx++) {
      if (idx > 0) {
        subPartitions.append(",");
      }

      subPartitions.append(String.format(" SUBPARTITION %s VALUES (%d)",
          subPartitionNames[idx], idx));
    }

    String sql =
        String.format("ALTER TABLE %s " + "ADD PARTITION %s "
            + "VALUES LESS THAN (to_date('%s', '%s'))" + "( %s ) ", table
            .toString(), partitionName, partitionBoundStr, dateFormat,
            subPartitions.toString());

    LOG.debug(String.format("SQL generated by %s:\n%s", OraOopUtilities
        .getCurrentMethodName(), sql));

    try {
      PreparedStatement preparedStatement = connection.prepareStatement(sql);
      preparedStatement.execute(sql);
      preparedStatement.close();

    } catch (SQLException ex) {
      LOG.error(String
          .format(
              "The error \"%s\" was encountered when executing the following "
              + "SQL statement:\n%s",
              ex.getMessage(), sql));
      throw ex;
    }
  }

  public static void mergeTable(Connection connection, OracleTable targetTable,
      OracleTable sourceTable, String[] mergeColumnNames,
      OracleTableColumns oracleTableColumns, Object oraOopSysDate,
      int oraOopMapperId, boolean parallelizationEnabled) throws SQLException {

    StringBuilder updateClause = new StringBuilder();
    StringBuilder insertClause = new StringBuilder();
    StringBuilder valuesClause = new StringBuilder();
    for (int idx = 0; idx < oracleTableColumns.size(); idx++) {
      OracleTableColumn oracleTableColumn = oracleTableColumns.get(idx);
      String columnName = oracleTableColumn.getName();

      if (insertClause.length() > 0) {
        insertClause.append(",");
      }
      insertClause.append(String.format("target.%s", columnName));

      if (valuesClause.length() > 0) {
        valuesClause.append(",");
      }
      valuesClause.append(String.format("source.%s", columnName));

      if (!OraOopUtilities.stringArrayContains(mergeColumnNames, columnName,
          true)) {

        // If we're performing a merge, then the table is not partitioned. (If
        // the table
        // was partitioned, we'd be deleting and then inserting rows.)
        if (!columnName
            .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_PARTITION)
            && !columnName
            .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION)
            && !columnName
            .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW)) {

          if (updateClause.length() > 0) {
            updateClause.append(",");
          }
          updateClause.append(String.format("target.%1$s = source.%1$s",
              columnName));

        }
      }
    }

    String sourceClause = valuesClause.toString();

    String sql =
        String.format("MERGE %7$s INTO %1$s target \n"
            + "USING (SELECT %8$s * FROM %2$s) source \n" + "  ON (%3$s) \n"
            + "WHEN MATCHED THEN \n" + "  UPDATE SET %4$s \n"
            + "WHEN NOT MATCHED THEN \n" + "  INSERT (%5$s) \n"
            + "  VALUES (%6$s)", targetTable.toString(),
            sourceTable.toString(),
            generateUpdateKeyColumnsWhereClauseFragment(mergeColumnNames,
                "target", "source"), updateClause.toString(), insertClause
                .toString(), sourceClause,
            parallelizationEnabled ? "/*+ append parallel(target) */" : "",
            parallelizationEnabled ? "/*+parallel*/" : "");

    LOG.info(String.format("Merge SQL statement:\n" + sql));

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    resultSet.close();
    statement.close();
  }

  public static void updateTable(Connection connection,
      OracleTable targetTable, OracleTable sourceTable,
      String[] mergeColumnNames, OracleTableColumns oracleTableColumns,
      Object oraOopSysDate, int oraOopMapperId, boolean parallelizationEnabled)
      throws SQLException {

    StringBuilder targetColumnsClause = new StringBuilder();
    StringBuilder sourceColumnsClause = new StringBuilder();
    for (int idx = 0; idx < oracleTableColumns.size(); idx++) {
      OracleTableColumn oracleTableColumn = oracleTableColumns.get(idx);
      String columnName = oracleTableColumn.getName();

      if (targetColumnsClause.length() > 0) {
        targetColumnsClause.append(",");
      }
      targetColumnsClause.append(String.format("a.%s", columnName));

      if (sourceColumnsClause.length() > 0) {
        sourceColumnsClause.append(",");
      }
      sourceColumnsClause.append(String.format("b.%s", columnName));
    }

    String sourceClause = sourceColumnsClause.toString();

    sourceClause =
        sourceClause.replaceAll(OraOopConstants.COLUMN_NAME_EXPORT_PARTITION,
            String.format("to_date('%s', 'yyyy/mm/dd hh24:mi:ss')",
                OraOopOracleQueries.oraDATEToString(oraOopSysDate,
                    "yyyy/mm/dd hh24:mi:ss")));

    sourceClause =
        sourceClause.replaceAll(
            OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION, Integer
                .toString(oraOopMapperId));

    String sql =
        String.format("UPDATE %5$s %1$s a \n" + "SET \n" + "(%2$s) \n"
            + "= (SELECT \n" + "%3$s \n" + "FROM %4$s b \n" + "WHERE %6$s) \n"
            + "WHERE EXISTS (SELECT null FROM %4$s c " + "WHERE %7$s)",
            targetTable.toString(), targetColumnsClause.toString(),
            sourceClause, sourceTable.toString(),
            parallelizationEnabled ? "/*+ parallel */" : "",
            generateUpdateKeyColumnsWhereClauseFragment(mergeColumnNames, "b",
                "a"), generateUpdateKeyColumnsWhereClauseFragment(
                mergeColumnNames, "c", "a"));

    LOG.info(String.format("Update SQL statement:\n" + sql));

    Statement statement = connection.createStatement();
    int rowsAffected = statement.executeUpdate(sql);

    LOG.info(String.format(
        "The number of rows affected by the update SQL was: %d", rowsAffected));

    statement.close();
  }

  /**
   * Whether new rows should be included in changes table or not.
   */
  public enum CreateExportChangesTableOptions {
    OnlyRowsThatDiffer, RowsThatDifferPlusNewRows
  }

  public static int createExportChangesTable(Connection connection,
      OracleTable tableToCreate, String tableToCreateStorageClause,
      OracleTable tableContainingUpdates, OracleTable tableToBeUpdated,
      String[] joinColumnNames, CreateExportChangesTableOptions options,
      boolean parallelizationEnabled) throws SQLException {

    List<String> columnNames =
        getTableColumnNames(connection, tableToBeUpdated
            , false // <- omitLobAndLongColumnsDuringImport
            , OraOopConstants.Sqoop.Tool.EXPORT
            , true // <- onlyOraOopSupportedTypes
            , false // <- omitOraOopPseudoColumns
        );

    StringBuilder columnClause = new StringBuilder(2 * columnNames.size());
    for (int idx = 0; idx < columnNames.size(); idx++) {
      if (idx > 0) {
        columnClause.append(",");
      }
      columnClause.append("a." + columnNames.get(idx));
    }

    StringBuilder rowEqualityClause = new StringBuilder();
    for (int idx = 0; idx < columnNames.size(); idx++) {
      String columnName = columnNames.get(idx);

      // We need to omit the OraOop pseudo columns from the SQL statement that
      // compares the data in
      // the two tables we're interested in. Otherwise, EVERY row will be
      // considered to be changed,
      // since the values in the pseudo columns will differ. (i.e.
      // ORAOOP_EXPORT_SYSDATE will differ.)
      if (columnName
          .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_PARTITION)
          || columnName
             .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_SUBPARTITION)
          || columnName
             .equalsIgnoreCase(OraOopConstants.COLUMN_NAME_EXPORT_MAPPER_ROW)) {
        continue;
      }

      if (idx > 0) {
        rowEqualityClause.append("OR");
      }

      rowEqualityClause.append(String.format("(a.%1$s <> b.%1$s "
          + "OR (a.%1$s IS NULL AND b.%1$s IS NOT NULL) "
          + "OR (a.%1$s IS NOT NULL AND b.%1$s IS NULL))", columnName));
    }

    String sqlJoin = null;
    switch (options) {

      case OnlyRowsThatDiffer:
        sqlJoin = "";
        break;

      case RowsThatDifferPlusNewRows:
        sqlJoin = "(+)"; // <- An outer-join will cause the "new" rows to be
                         // included
        break;

      default:
        throw new RuntimeException(String.format(
            "Update %s to cater for the option \"%s\".", OraOopUtilities
                .getCurrentMethodName(), options.toString()));
    }

    String sql =
        String.format("CREATE TABLE %1$s \n" + "NOLOGGING %8$s \n" + "%7$s \n"
            + "AS \n " + "SELECT \n" + "%5$s \n" + "FROM %2$s a, %3$s b \n"
            + "WHERE (%4$s) \n" + "AND ( \n" + "%6$s \n" + ")", tableToCreate
            .toString(), tableContainingUpdates.toString(), tableToBeUpdated
            .toString(), generateUpdateKeyColumnsWhereClauseFragment(
            joinColumnNames, "a", "b", sqlJoin), columnClause.toString(),
            rowEqualityClause.toString(), parallelizationEnabled ? "PARALLEL"
                : "", tableToCreateStorageClause);

    LOG.info(String.format("The SQL to create the changes-table is:\n%s", sql));

    Statement statement = connection.createStatement();

    long start = System.nanoTime();
    statement.executeUpdate(sql);
    double timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
    LOG.info(String.format("Time spent creating change-table: %f sec.",
        timeInSec));

    String indexName = tableToCreate.toString().replaceAll("CHG", "IDX");
    start = System.nanoTime();
    statement.execute(String.format("CREATE INDEX %s ON %s (%s)", indexName,
        tableToCreate.toString(), OraOopUtilities
            .stringArrayToCSV(joinColumnNames)));
    timeInSec = (System.nanoTime() - start) / Math.pow(10, 9);
    LOG.info(String.format("Time spent creating change-table index: %f sec.",
        timeInSec));

    int changeTableRowCount = 0;

    ResultSet resultSet =
        statement.executeQuery(String.format("select count(*) from %s",
            tableToCreate.toString()));
    resultSet.next();
    changeTableRowCount = resultSet.getInt(1);
    LOG.info(String.format("The number of rows in the change-table is: %d",
        changeTableRowCount));

    statement.close();
    return changeTableRowCount;
  }

  public static void deleteRowsFromTable(Connection connection,
      OracleTable tableToDeleteRowsFrom,
      OracleTable tableContainingRowsToDelete, String[] joinColumnNames,
      boolean parallelizationEnabled) throws SQLException {

    String sql =
        String.format("DELETE %4$s FROM %1$s a \n" + "WHERE EXISTS ( \n"
            + "SELECT null FROM %3$s b WHERE \n" + "%2$s)",
            tableToDeleteRowsFrom.toString(),
            generateUpdateKeyColumnsWhereClauseFragment(joinColumnNames, "a",
                "b"), tableContainingRowsToDelete.toString(),
            parallelizationEnabled ? "/*+ parallel */" : "");

    LOG.info(String.format("The SQL to delete rows from a table:\n%s", sql));

    Statement statement = connection.createStatement();
    int rowsAffected = statement.executeUpdate(sql);

    LOG.info(String.format(
        "The number of rows affected by the delete SQL was: %d", rowsAffected));

    statement.close();
  }

  public static void insertRowsIntoExportTable(Connection connection,
      OracleTable tableToInsertRowsInto,
      OracleTable tableContainingRowsToInsert, Object oraOopSysDate,
      int oraOopMapperId, boolean parallelizationEnabled) throws SQLException {

    List<String> columnNames =
        getTableColumnNames(connection, tableToInsertRowsInto);

    StringBuilder columnClause =
        new StringBuilder(2 + (2 * columnNames.size()));
    for (int idx = 0; idx < columnNames.size(); idx++) {
      if (idx > 0) {
        columnClause.append(",");
      }
      columnClause.append(columnNames.get(idx));
    }

    String columnsClause = columnClause.toString();

    String sql =
        String.format("insert %4$s \n" + "into %1$s \n" + "select \n"
            + "%2$s \n" + "from %3$s", tableToInsertRowsInto.toString(),
            columnsClause, tableContainingRowsToInsert.toString(),
            parallelizationEnabled ? "/*+ append parallel */" : "");

    LOG.info(String.format(
        "The SQL to insert rows from one table into another:\n%s", sql));

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    resultSet.close();
    statement.close();
  }

  public static boolean doesIndexOnColumnsExist(Connection connection,
      OracleTable oracleTable, String[] columnNames) throws SQLException {

    // Attempts to find an index on the table that *starts* with the N column
    // names passed.
    // These columns can be in any order.

    String columnNamesInClause =
        OraOopUtilities.stringArrayToCSV(columnNames, "'");

    String sql =
        String.format("SELECT b.index_name, \n"
            + "  sum(case when b.column_name in (%1$s) then 1 end) num_cols \n"
            + "FROM dba_indexes a, dba_ind_columns b \n" + "WHERE \n"
            + "a.owner = b.index_owner \n"
            + "AND a.index_name = b.index_name \n" + "AND b.table_owner = ? \n"
            + "AND b.table_name = ? \n" + "AND a.status = 'VALID' \n"
            + "AND b.column_position <= ? \n" + "GROUP BY b.index_name \n"
            + "HAVING sum(case when b.column_name in (%1$s) then 1 end) = ?",
            columnNamesInClause);

    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setString(1, oracleTable.getSchema());
    statement.setString(2, oracleTable.getName());
    statement.setInt(3, columnNames.length);
    statement.setInt(4, columnNames.length);

    LOG.debug(String.format("SQL to find an index on the columns %s:\n%s",
        columnNamesInClause, sql));

    ResultSet resultSet = statement.executeQuery();

    boolean result = false;
    if (resultSet.next()) {
      LOG.debug(String
          .format(
              "The table %s has an index named %s starting with the column(s) "
              + "%s (in any order).",
              oracleTable.toString(), resultSet.getString("index_name"),
              columnNamesInClause));
      result = true;
    }

    resultSet.close();
    statement.close();

    return result;
  }

  private static String generateUpdateKeyColumnsWhereClauseFragment(
      String[] joinColumnNames, String prefix1, String prefix2) {

    return generateUpdateKeyColumnsWhereClauseFragment(joinColumnNames,
        prefix1, prefix2, "");
  }

  private static String generateUpdateKeyColumnsWhereClauseFragment(
      String[] joinColumnNames, String prefix1, String prefix2,
      String sqlJoinOperator) {

    StringBuilder result = new StringBuilder();
    for (int idx = 0; idx < joinColumnNames.length; idx++) {
      String joinColumnName = joinColumnNames[idx];
      if (idx > 0) {
        result.append(" AND ");
      }
      result.append(String.format("%1$s.%3$s = %2$s.%3$s %4$s", prefix1,
          prefix2, joinColumnName, sqlJoinOperator));
    }
    return result.toString();
  }

  public static String getCurrentSchema(Connection connection)
      throws SQLException {
    String sql = "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL";

    PreparedStatement statement = connection.prepareStatement(sql);

    ResultSet resultSet = statement.executeQuery();

    resultSet.next();
    String result = resultSet.getString(1);

    resultSet.close();
    statement.close();

    LOG.info("Current schema is: " + result);

    return result;
  }

  public static String getTableSchema(Connection connection, OracleTable table)
      throws SQLException {
    if (table.getSchema() == null || table.getSchema().isEmpty()) {
      return getCurrentSchema(connection);
    } else {
      return table.getSchema();
    }
  }

  public static long getCurrentScn(Connection connection) throws SQLException {
    String sql = "SELECT current_scn FROM v$database";
    PreparedStatement statement = connection.prepareStatement(sql);
    ResultSet resultSet = statement.executeQuery();

    resultSet.next();
    long result = resultSet.getLong(1);
    resultSet.close();
    statement.close();

    return result;
  }

  public static void setLongAtName(PreparedStatement statement,
      String bindName, long bindValue) throws SQLException {
    try {
      methSetLongAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setBigDecimalAtName(PreparedStatement statement,
      String bindName, BigDecimal bindValue) throws SQLException {
    try {
      methSetBigDecimalAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setStringAtName(PreparedStatement statement,
      String bindName, String bindValue) throws SQLException {
    try {
      methSetStringAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setTimestampAtName(PreparedStatement statement,
      String bindName, Timestamp bindValue) throws SQLException {
    try {
      methSetTimestampAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setBinaryDoubleAtName(PreparedStatement statement,
      String bindName, double bindValue) throws SQLException {
    try {
      methSetBinaryDoubleAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setObjectAtName(PreparedStatement statement,
      String bindName, Object bindValue) throws SQLException {
    try {
      methSetObjectAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setBinaryFloatAtName(PreparedStatement statement,
      String bindName, float bindValue) throws SQLException {
    try {
      methSetBinaryFloatAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static void setIntAtName(PreparedStatement statement, String bindName,
      int bindValue) throws SQLException {
    try {
      methSetIntAtName.invoke(statement, bindName, bindValue);
    } catch (Exception e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      } else {
        throw new RuntimeException("Could not set bind variable", e);
      }
    }
  }

  public static int getOracleType(String name) {
    Integer result = ORACLE_TYPES.get(name);
    if (result == null) {
      synchronized (ORACLE_TYPES) {
        try {
          result = oracleTypesClass.getField(name).getInt(null);
          ORACLE_TYPES.put(name, result);
        } catch (Exception e) {
          throw new RuntimeException("Invalid oracle type specified", e);
        }
      }
    }
    return result;
  }
}
