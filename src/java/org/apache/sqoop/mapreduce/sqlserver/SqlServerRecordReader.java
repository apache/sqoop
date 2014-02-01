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
package org.apache.sqoop.mapreduce.sqlserver;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.SQLServerManager;
import org.apache.sqoop.mapreduce.DBWritable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Microsoft SQL Server specific Record Reader.
 */
public class SqlServerRecordReader<T extends DBWritable>
  extends DataDrivenDBRecordReader<T> {

  private static final Log LOG =
    LogFactory.getLog(SqlServerRecordReader.class);

  // CHECKSTYLE:OFF
  public SqlServerRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields,
      String table) throws SQLException {

    super(split, inputClass, conf, conn, dbConfig, cond, fields, table,
      "MICROSOFT SQL SERVER");
  }
  // CHECKSTYLE:ON


  /**
   * {@inheritDoc}
   */
  @Override
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();

    DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();

    DBConfiguration dbConf = getDBConf();
    String [] fieldNames = getFieldNames();
    String tableName = getTableName();
    String conditions = getConditions();

    // Build the WHERE clauses associated with the data split first.
    // We need them in both branches of this function.
    StringBuilder conditionClauses = new StringBuilder();
    conditionClauses.append("( ").append(dataSplit.getLowerClause());
    conditionClauses.append(" ) AND ( ").append(dataSplit.getUpperClause());
    conditionClauses.append(" )");

    if (dbConf.getInputQuery() == null) {
      // We need to generate the entire query.
      query.append("SELECT ");

      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      query.append(" FROM ").append(tableName);

      String tableHints =
        dbConf.getConf().get(SQLServerManager.TABLE_HINTS_PROP);
      if (tableHints != null) {
        LOG.info("Using table hints: " + tableHints);
        query.append(" WITH (").append(tableHints).append(")");
      }

      query.append(" WHERE ");
      if (conditions != null && conditions.length() > 0) {
        // Put the user's conditions first.
        query.append("( ").append(conditions).append(" ) AND ");
      }

      // Now append the conditions associated with our split.
      query.append(conditionClauses.toString());

    } else {
      // User provided the query. We replace the special token with
      // our WHERE clause.
      String inputQuery = dbConf.getInputQuery();
      if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
        LOG.error("Could not find the clause substitution token "
            + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: ["
            + inputQuery + "]. Parallel splits may not work correctly.");
      }

      query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
          conditionClauses.toString()));
    }

    LOG.info("Using query: " + query.toString());
    return query.toString();
  }
}
