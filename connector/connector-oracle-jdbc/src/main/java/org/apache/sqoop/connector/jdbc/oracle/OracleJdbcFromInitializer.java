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

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.jdbc.oracle.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleQueries;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.job.etl.InitializerContext;

public class OracleJdbcFromInitializer extends
    OracleJdbcCommonInitializer<FromJobConfiguration> {

  private static final Logger LOG =
      Logger.getLogger(OracleJdbcFromInitializer.class);

    @Override
    public void connect(InitializerContext context,
        LinkConfiguration linkConfiguration,
        FromJobConfiguration jobConfiguration) throws SQLException {
      super.connect(context, linkConfiguration, jobConfiguration);
      table = OracleUtilities.decodeOracleTableName(
          linkConfiguration.connectionConfig.username,
          jobConfiguration.fromJobConfig.tableName);
    }

    @Override
    public void initialize(InitializerContext context,
        LinkConfiguration linkConfiguration,
        FromJobConfiguration jobConfiguration) {
      super.initialize(context, linkConfiguration, jobConfiguration);
      LOG.debug("Running Oracle JDBC connector FROM initializer");

      try {
        if(OracleQueries.isTableAnIndexOrganizedTable(connection, table)) {
          if(OracleUtilities.getOraOopOracleDataChunkMethod(
              jobConfiguration.fromJobConfig) !=
                  OracleUtilities.OracleDataChunkMethod.PARTITION) {
          throw new RuntimeException(String.format("Cannot process this Sqoop"
              + " connection, as the Oracle table %s is an"
              + " index-organized table. If the table is"
              + " partitioned, set the data chunk method to "
              + OracleUtilities.OracleDataChunkMethod.PARTITION
              + ".",
              table.toString()));
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException(String.format(
            "Unable to determine whether the Oracle table %s is an"
            + "index-organized table.", table.toString()), e);
      }

      if(BooleanUtils.isTrue(jobConfiguration.fromJobConfig.consistentRead)) {
        Long scn = jobConfiguration.fromJobConfig.consistentReadScn;
        if(scn==null || scn.equals(Long.valueOf(0L))) {
          try {
            scn = OracleQueries.getCurrentScn(connection);
          } catch(SQLException e) {
            throw new RuntimeException("Unable to determine SCN of database.",
                e);
          }
        }
        context.getContext().setLong(
            OracleJdbcConnectorConstants.ORACLE_IMPORT_CONSISTENT_READ_SCN,
            scn);
        LOG.info("Performing a consistent read using SCN: " + scn);
      }
    }

    @Override
    protected List<String> getColumnNames(FromJobConfiguration jobConfiguration)
        throws SQLException {
      List<String> colNames = OracleQueries.getFromTableColumnNames(connection,
          table, OracleUtilities.omitLobAndLongColumnsDuringImport(
              jobConfiguration.fromJobConfig),
          true // <- onlyOraOopSupportedTypes
          );

      return OracleUtilities.getSelectedColumnNamesInOracleTable(table,
          colNames, jobConfiguration.fromJobConfig.columns);
    }

}
