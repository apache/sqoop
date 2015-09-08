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
package org.apache.sqoop.repository.postgresql;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.PostgresqlRepoError;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.common.CommonRepoConstants;
import org.apache.sqoop.repository.common.CommonRepositoryHandler;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public class PostgresqlRepositoryHandler extends CommonRepositoryHandler {
  private static final Logger LOG =
      Logger.getLogger(PostgresqlRepositoryHandler.class);

  private JdbcRepositoryContext repoContext;

  /**
   * {@inheritDoc}
   */
  @Override
  public String name() {
    return "PostgreSQL";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void initialize(JdbcRepositoryContext ctx) {
    repoContext = ctx;
    LOG.info("PostgresqlRepositoryHandler initialized.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void shutdown() {}

  /**
   * Detect version of underlying database structures
   *
   * @param conn JDBC Connection
   * @return
   */
  public int detectRepositoryVersion(Connection conn) {
    // Select and return the version
    try {
      DatabaseMetaData md = conn.getMetaData();
      try (ResultSet metadataResultSet = md.getTables(null,
          CommonRepositorySchemaConstants.SCHEMA_SQOOP,
          CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME, null)){

        if (metadataResultSet.next()) {
          try (PreparedStatement stmt = conn.prepareStatement(PostgresqlSchemaQuery.STMT_SELECT_SYSTEM)){
            stmt.setString(1, CommonRepoConstants.SYSKEY_VERSION);
            try (ResultSet rs = stmt.executeQuery()){

              if (!rs.next()) {
                return 0;
              }

              return rs.getInt(1);
            }
          }
        }
      }
    } catch (SQLException e) {
      LOG.info("Can't fetch repository structure version.", e);
      return 0;
    }

    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createOrUpgradeRepository(Connection conn) {
    int version = detectRepositoryVersion(conn);
    LOG.info("Detected repository version: " + version);

    if (version == PostgresqlRepoConstants.LATEST_POSTGRESQL_REPOSITORY_VERSION) {
      return;
    }

    if (version < 1) {
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_SCHEMA_SQOOP, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIGURABLE, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIG, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_INPUT, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_INPUT_RELATION, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_LINK, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_JOB, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_LINK_INPUT, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_JOB_INPUT, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_SUBMISSION, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER_GROUP, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_SYSTEM, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_DIRECTION, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS, conn);

      // Insert FROM and TO directions.
      insertDirections(conn);
    }
    if (version < 2) {
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT_TYPE, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT_PROPERTY, conn);
      runQuery(PostgresqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT, conn);

      runQuery(PostgresqlSchemaUpgradeQuery.QUERY_UPGRADE_TABLE_SQ_LINK_UPDATE_COLUMN_SQ_LINK_NAME, conn);
      runQuery(PostgresqlSchemaUpgradeQuery.QUERY_UPGRADE_TABLE_SQ_LINK_ALTER_COLUMN_SQ_LINK_NAME_NOT_NULL, conn);
      runQuery(PostgresqlSchemaUpgradeQuery.QUERY_UPGRADE_TABLE_SQ_JOB_UPDATE_COLUMN_SQB_NAME, conn);
      runQuery(PostgresqlSchemaUpgradeQuery.QUERY_UPGRADE_TABLE_SQ_JOB_ALTER_COLUMN_SQB_NAME_NOT_NULL, conn);
      runQuery(PostgresqlSchemaUpgradeQuery.QUERY_UPGRADE_TABLE_SQ_CONFIGURABLE_ALTER_COLUMN_SQB_NAME_NOT_NULL, conn);
    }

    try (PreparedStatement stmtDel = conn.prepareStatement(PostgresqlSchemaQuery.STMT_DELETE_SYSTEM);
         PreparedStatement stmtInsert = conn.prepareStatement(PostgresqlSchemaQuery.STMT_INSERT_SYSTEM);) {
      stmtDel.setString(1, CommonRepoConstants.SYSKEY_VERSION);
      stmtDel.executeUpdate();

      stmtInsert.setString(1, CommonRepoConstants.SYSKEY_VERSION);
      stmtInsert.setString(2, Integer.toString(PostgresqlRepoConstants.LATEST_POSTGRESQL_REPOSITORY_VERSION));
      stmtInsert.executeUpdate();
    } catch (SQLException e) {
      LOG.error("Can't persist the repository version", e);
    }
  }

  /**
   * Insert directions: FROM and TO.
   * @param conn
   * @return Map<Direction, Long> direction ID => Direction
   */
  protected Map<Direction, Long> insertDirections(Connection conn) {
    // Add directions
    Map<Direction, Long> directionMap = new TreeMap<Direction, Long>();
    PreparedStatement insertDirectionStmt = null;
    try {
      // Insert directions and get IDs.
      for (Direction direction : Direction.values()) {
        insertDirectionStmt = conn.prepareStatement(PostgresqlSchemaQuery.STMT_INSERT_DIRECTION, Statement.RETURN_GENERATED_KEYS);
        insertDirectionStmt.setString(1, direction.toString());
        if (insertDirectionStmt.executeUpdate() != 1) {
          throw new SqoopException(PostgresqlRepoError.POSTGRESQLREPO_0003, "Could not add directions FROM and TO.");
        }

        ResultSet directionId = insertDirectionStmt.getGeneratedKeys();
        if (directionId.next()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded direction: " + directionId.getLong(1));
          }

          directionMap.put(direction, directionId.getLong(1));
        } else {
          throw new SqoopException(PostgresqlRepoError.POSTGRESQLREPO_0004, "Could not get ID of direction " + direction);
        }
      }
    } catch (SQLException e) {
      throw new SqoopException(PostgresqlRepoError.POSTGRESQLREPO_0000, e);
    } finally {
      closeStatements(insertDirectionStmt);
    }

    return directionMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isRepositorySuitableForUse(Connection conn) {
    int version = detectRepositoryVersion(conn);

    if(version != PostgresqlRepoConstants.LATEST_POSTGRESQL_REPOSITORY_VERSION) {
      return false;
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String validationQuery() {
    return "values(1)"; // Yes, this is valid PostgreSQL SQL
  }
}
