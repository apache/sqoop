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
package org.apache.sqoop.repository.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.MySqlRepoError;
import org.apache.sqoop.repository.JdbcRepositoryContext;
import org.apache.sqoop.repository.common.CommonRepoConstants;
import org.apache.sqoop.repository.common.CommonRepositoryHandler;
import org.apache.sqoop.repository.common.CommonRepositorySchemaConstants;

/**
 * JDBC based repository handler for MySQL database.
 *
 * Repository implementation for MySQL database.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public class MySqlRepositoryHandler extends CommonRepositoryHandler {

  private static final Logger LOG =
      Logger.getLogger(MySqlRepositoryHandler.class);

  private JdbcRepositoryContext repoContext;

  public MySqlRepositoryHandler() {
    crudQueries = new MysqlRepositoryInsertUpdateDeleteSelectQuery();
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public String name() {
    return "MySql";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void initialize(JdbcRepositoryContext ctx) {
    repoContext = ctx;
    LOG.info("MySqlRepositoryHandler initialized.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void shutdown() {
  }

  /**
   * Detect version of underlying database structures
   *
   * @param conn
   *          JDBC Connection
   * @return
   */
  public int detectRepositoryVersion(Connection conn) {
    ResultSet metadataResultSet = null;

    // Select and return the version
    try {
      DatabaseMetaData md = conn.getMetaData();
      metadataResultSet = md.getTables(null,
          CommonRepositorySchemaConstants.SCHEMA_SQOOP,
          CommonRepositorySchemaConstants.TABLE_SQ_SYSTEM_NAME, null);

      if (metadataResultSet.next()) {
        try (PreparedStatement stmt = conn.prepareStatement(MySqlSchemaQuery.STMT_SELECT_SYSTEM)){
          stmt.setString(1, CommonRepoConstants.SYSKEY_VERSION);
          try (ResultSet rs = stmt.executeQuery()){

            if (!rs.next()) {
              return 0;
            }

            return rs.getInt(1);
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

    if (version == MySqlRepoConstants.LATEST_MYSQL_REPOSITORY_VERSION) {
      return;
    }

    if (version < 1) {
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_DATABASE_SQOOP, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIGURABLE, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIG, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_INPUT, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_INPUT_RELATION, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_LINK, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_JOB, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_LINK_INPUT, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_JOB_INPUT, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_SUBMISSION, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER_GROUP, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_COUNTER_SUBMISSION, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_SYSTEM, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_DIRECTION, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONNECTOR_DIRECTIONS, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONFIG_DIRECTIONS, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT_TYPE, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT_PROPERTY, conn);
      runQuery(MySqlSchemaCreateQuery.QUERY_CREATE_TABLE_SQ_CONTEXT, conn);

      // Insert FROM and TO directions.
      insertDirections(conn);
    }

    ResultSet rs = null;
    try (PreparedStatement stmt = conn
            .prepareStatement(MySqlSchemaQuery.STMT_INSERT_ON_DUPLICATE_KEY_SYSTEM);) {
      stmt.setString(1, CommonRepoConstants.SYSKEY_VERSION);
      stmt.setString(2,
          Integer.toString(MySqlRepoConstants.LATEST_MYSQL_REPOSITORY_VERSION));
      stmt.setString(3,
          Integer.toString(MySqlRepoConstants.LATEST_MYSQL_REPOSITORY_VERSION));
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error("Can't persist the repository version", e);
    }
  }

  /**
   * Insert directions: FROM and TO.
   *
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
        insertDirectionStmt = conn.prepareStatement(
            MySqlSchemaQuery.STMT_INSERT_DIRECTION,
            Statement.RETURN_GENERATED_KEYS);
        insertDirectionStmt.setString(1, direction.toString());
        if (insertDirectionStmt.executeUpdate() != 1) {
          throw new SqoopException(MySqlRepoError.MYSQLREPO_0001,
              "Could not add directions FROM and TO.");
        }

        ResultSet directionId = insertDirectionStmt.getGeneratedKeys();
        if (directionId.next()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("Loaded direction: " + directionId.getLong(1));
          }

          directionMap.put(direction, directionId.getLong(1));
        } else {
          throw new SqoopException(MySqlRepoError.MYSQLREPO_0002,
              "Could not get ID of direction " + direction);
        }
      }
    } catch (SQLException e) {
      throw new SqoopException(MySqlRepoError.MYSQLREPO_0000, e);
    } finally {
      closeStatements(insertDirectionStmt);
    }

    return directionMap;
  }

  @Override
  public boolean isRepositorySuitableForUse(Connection conn) {
    int version = detectRepositoryVersion(conn);

    if (version != MySqlRepoConstants.LATEST_MYSQL_REPOSITORY_VERSION) {
      return false;
    }

    return true;
  }
}
