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
package org.apache.sqoop.repository.derby.upgrade;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.DerbyRepoError;
import org.apache.sqoop.repository.common.CommonRepoUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQB_ID;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.COLUMN_SQB_NAME;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.SCHEMA_SQOOP;
import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.TABLE_SQ_JOB_NAME;

/**
 * Rename all jobs that have the same name to a unique name.
 * Just provide a suffix that auto-increments.
 */
public class UniqueJobRename {
  public static final String QUERY_SELECT_JOBS_WITH_NON_UNIQUE_NAMES =
      "SELECT j1." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID)
          + ", j1." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " AS j1 INNER JOIN ( SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " GROUP BY (" + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + ")"
          + " HAVING COUNT(*) > 1 ) AS j2 ON j1." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME)
          + " = j2." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME);

  public static final String QUERY_UPDATE_JOB_NAME_BY_ID =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + " = ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  private Connection conn;

  public UniqueJobRename(Connection conn) {
    this.conn = conn;
  }

  public void execute() {
    Map<Long, String> idToNewNameMap = new TreeMap<Long, String>();

    Statement fetchJobStmt = null;
    PreparedStatement updateJobStmt = null;
    ResultSet fetchJobResultSet = null;

    // Fetch all non-unique job IDs and Names.
    // Transform names.
    try {
      fetchJobStmt = conn.createStatement();
      fetchJobResultSet = fetchJobStmt.executeQuery(QUERY_SELECT_JOBS_WITH_NON_UNIQUE_NAMES);
      while (fetchJobResultSet.next()) {
        idToNewNameMap.put(fetchJobResultSet.getLong(1), getNewName(fetchJobResultSet.getString(2)));
      }
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      CommonRepoUtils.closeResultSets(fetchJobResultSet);
      CommonRepoUtils.closeStatements(fetchJobStmt);
    }


    try {
      updateJobStmt = conn.prepareStatement(QUERY_UPDATE_JOB_NAME_BY_ID);
      for (Long jobId : idToNewNameMap.keySet()) {
        updateJobStmt.setString(1, idToNewNameMap.get(jobId));
        updateJobStmt.setLong(2, jobId);
        updateJobStmt.addBatch();
      }

      int[] counts = updateJobStmt.executeBatch();
      for (int count : counts) {
        if (count != 1) {
          throw new SqoopException(DerbyRepoError.DERBYREPO_0000,
              "Update count wrong when changing names for non-unique jobs. Update coutns are: "
                  + StringUtils.join(Arrays.asList(counts), ","));
        }
      }
    } catch (SQLException e) {
      throw new SqoopException(DerbyRepoError.DERBYREPO_0000, e);
    } finally {
      CommonRepoUtils.closeResultSets(fetchJobResultSet);
      CommonRepoUtils.closeStatements(fetchJobStmt);
    }
  }

  /**
   * Make new name from old name.
   * New name takes on form: old name + "_" + uuid.
   * New name will substring old name if old name is longer than 47 characters.
   * @param oldName
   * @return newName
   */
  private String getNewName(String oldName) {
    String suffix = "_" + UUID.randomUUID().toString();
    // Make sure new name is max 64 characters.
    int maxLength = 64 - suffix.length();
    if (oldName.length() > maxLength) {
      return oldName.substring(0, maxLength) + suffix;
    } else {
      return oldName + suffix;
    }
  }
}
