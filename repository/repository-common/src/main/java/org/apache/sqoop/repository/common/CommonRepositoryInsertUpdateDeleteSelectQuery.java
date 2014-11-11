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
package org.apache.sqoop.repository.common;

import static org.apache.sqoop.repository.common.CommonRepositorySchemaConstants.*;

public class CommonRepositoryInsertUpdateDeleteSelectQuery {
  /*******DIRECTION TABLE **************/
  public static final String STMT_SELECT_SQD_ID_BY_SQD_NAME =
      "SELECT " + COLUMN_SQD_ID + " FROM " + TABLE_SQ_DIRECTION
          + " WHERE " + COLUMN_SQD_NAME + "=?";

  public static final String STMT_SELECT_SQD_NAME_BY_SQD_ID =
      "SELECT " + COLUMN_SQD_NAME + " FROM " + TABLE_SQ_DIRECTION
          + " WHERE " + COLUMN_SQD_ID + "=?";

  /*********CONFIGURABLE TABLE ***************/
  //DML: Get configurable by given name
  public static final String STMT_SELECT_FROM_CONFIGURABLE =
      "SELECT "
          + COLUMN_SQC_ID + ", "
          + COLUMN_SQC_NAME + ", "
          + COLUMN_SQC_CLASS + ", "
          + COLUMN_SQC_VERSION
          + " FROM " + TABLE_SQ_CONFIGURABLE
          + " WHERE " + COLUMN_SQC_NAME + " = ?";

  //DML: Get all configurables for a given type
  public static final String STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE =
      "SELECT "
          + COLUMN_SQC_ID + ", "
          + COLUMN_SQC_NAME + ", "
          + COLUMN_SQC_CLASS + ", "
          + COLUMN_SQC_VERSION
          + " FROM " + TABLE_SQ_CONFIGURABLE
          + " WHERE " + COLUMN_SQC_TYPE + " = ?";

  //DML: Insert into configurable
  public static final String STMT_INSERT_INTO_CONFIGURABLE =
      "INSERT INTO " + TABLE_SQ_CONFIGURABLE + " ("
          + COLUMN_SQC_NAME + ", "
          + COLUMN_SQC_CLASS + ", "
          + COLUMN_SQC_VERSION + ", "
          + COLUMN_SQC_TYPE
          + ") VALUES (?, ?, ?, ?)";

  //Delete all configs for a given configurable
  public static final String STMT_DELETE_CONFIGS_FOR_CONFIGURABLE =
      "DELETE FROM " + TABLE_SQ_CONFIG
          + " WHERE " + COLUMN_SQ_CFG_CONFIGURABLE + " = ?";

  //Delete all inputs for a given configurable
  public static final String STMT_DELETE_INPUTS_FOR_CONFIGURABLE =
      "DELETE FROM " + TABLE_SQ_INPUT
          + " WHERE "
          + COLUMN_SQI_CONFIG
          + " IN (SELECT "
          + COLUMN_SQ_CFG_ID
          + " FROM " + TABLE_SQ_CONFIG
          + " WHERE "
          + COLUMN_SQ_CFG_CONFIGURABLE + " = ?)";

  //Update the configurable
  public static final String STMT_UPDATE_CONFIGURABLE =
      "UPDATE " + TABLE_SQ_CONFIGURABLE
          + " SET " + COLUMN_SQC_NAME + " = ?, "
          + COLUMN_SQC_CLASS + " = ?, "
          + COLUMN_SQC_VERSION + " = ?, "
          + COLUMN_SQC_TYPE + " = ? "
          + " WHERE " + COLUMN_SQC_ID + " = ?";

  /**********CONFIG TABLE **************/
  //DML: Get all configs for a given configurable
  public static final String STMT_SELECT_CONFIG_FOR_CONFIGURABLE =
      "SELECT "
          + COLUMN_SQ_CFG_ID + ", "
          + COLUMN_SQ_CFG_CONFIGURABLE + ", "
          + COLUMN_SQ_CFG_NAME + ", "
          + COLUMN_SQ_CFG_TYPE + ", "
          + COLUMN_SQ_CFG_INDEX
          + " FROM " + TABLE_SQ_CONFIG
          + " WHERE " + COLUMN_SQ_CFG_CONFIGURABLE + " = ? "
          + " ORDER BY " + COLUMN_SQ_CFG_INDEX;


  //DML: Insert into config
  public static final String STMT_INSERT_INTO_CONFIG =
      "INSERT INTO " + TABLE_SQ_CONFIG + " ("
          + COLUMN_SQ_CFG_CONFIGURABLE + ", "
          + COLUMN_SQ_CFG_NAME + ", "
          + COLUMN_SQ_CFG_TYPE + ", "
          + COLUMN_SQ_CFG_INDEX
          + ") VALUES ( ?, ?, ?, ?)";

  /********** INPUT TABLE **************/
  // DML: Get inputs for a given config
  public static final String STMT_SELECT_INPUT =
      "SELECT "
          + COLUMN_SQI_ID + ", "
          + COLUMN_SQI_NAME + ", "
          + COLUMN_SQI_CONFIG + ", "
          + COLUMN_SQI_INDEX + ", "
          + COLUMN_SQI_TYPE + ", "
          + COLUMN_SQI_STRMASK + ", "
          + COLUMN_SQI_STRLENGTH + ", "
          + COLUMN_SQI_ENUMVALS + ", "
          + "cast(null as varchar(100))"
          + " FROM " + TABLE_SQ_INPUT
          + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
          + " ORDER BY " + COLUMN_SQI_INDEX;

  // DML: Insert into config input
  public static final String STMT_INSERT_INTO_INPUT =
      "INSERT INTO " + TABLE_SQ_INPUT + " ("
          + COLUMN_SQI_NAME + ", "
          + COLUMN_SQI_CONFIG + ", "
          + COLUMN_SQI_INDEX + ", "
          + COLUMN_SQI_TYPE + ", "
          + COLUMN_SQI_STRMASK + ", "
          + COLUMN_SQI_STRLENGTH + ", "
          + COLUMN_SQI_ENUMVALS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  /**********LINK INPUT TABLE **************/
  //DML: Get inputs and values for a given link
  public static final String STMT_FETCH_LINK_INPUT =
      "SELECT "
          + COLUMN_SQI_ID + ", "
          + COLUMN_SQI_NAME + ", "
          + COLUMN_SQI_CONFIG + ", "
          + COLUMN_SQI_INDEX + ", "
          + COLUMN_SQI_TYPE + ", "
          + COLUMN_SQI_STRMASK + ", "
          + COLUMN_SQI_STRLENGTH + ","
          + COLUMN_SQI_ENUMVALS + ", "
          + COLUMN_SQ_LNKI_VALUE
          + " FROM " + TABLE_SQ_INPUT
          + " LEFT OUTER JOIN " + TABLE_SQ_LINK_INPUT
          + " ON " + COLUMN_SQ_LNKI_INPUT + " = " + COLUMN_SQI_ID
          + " AND " + COLUMN_SQ_LNKI_LINK + " = ?"
          + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
          + " AND (" + COLUMN_SQ_LNKI_LINK + " = ?" + " OR " + COLUMN_SQ_LNKI_LINK + " IS NULL)"
          + " ORDER BY " + COLUMN_SQI_INDEX;

  /**********JOB INPUT TABLE **************/
  //DML: Fetch inputs and values for a given job
  public static final String STMT_FETCH_JOB_INPUT =
      "SELECT "
          + COLUMN_SQI_ID + ", "
          + COLUMN_SQI_NAME + ", "
          + COLUMN_SQI_CONFIG + ", "
          + COLUMN_SQI_INDEX + ", "
          + COLUMN_SQI_TYPE + ", "
          + COLUMN_SQI_STRMASK + ", "
          + COLUMN_SQI_STRLENGTH + ", "
          + COLUMN_SQI_ENUMVALS + ", "
          + COLUMN_SQBI_VALUE
          + " FROM " + TABLE_SQ_INPUT
          + " LEFT OUTER JOIN " + TABLE_SQ_JOB_INPUT
          + " ON " + COLUMN_SQBI_INPUT + " = " + COLUMN_SQI_ID
          + " AND  " + COLUMN_SQBI_JOB + " = ?"
          + " WHERE " + COLUMN_SQI_CONFIG + " = ?"
          + " AND (" + COLUMN_SQBI_JOB + " = ? OR " + COLUMN_SQBI_JOB + " IS NULL)"
          + " ORDER BY " + COLUMN_SQI_INDEX;

  /**********LINK TABLE **************/
  // DML: Insert new link
  public static final String STMT_INSERT_LINK =
      "INSERT INTO " + TABLE_SQ_LINK + " ("
          + COLUMN_SQ_LNK_NAME + ", "
          + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + COLUMN_SQ_LNK_ENABLED + ", "
          + COLUMN_SQ_LNK_CREATION_USER + ", "
          + COLUMN_SQ_LNK_CREATION_DATE + ", "
          + COLUMN_SQ_LNK_UPDATE_USER + ", "
          + COLUMN_SQ_LNK_UPDATE_DATE
          + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new link inputs
  public static final String STMT_INSERT_LINK_INPUT =
      "INSERT INTO " + TABLE_SQ_LINK_INPUT + " ("
          + COLUMN_SQ_LNKI_LINK + ", "
          + COLUMN_SQ_LNKI_INPUT + ", "
          + COLUMN_SQ_LNKI_VALUE
          + ") VALUES (?, ?, ?)";

  // DML: Update link
  public static final String STMT_UPDATE_LINK =
      "UPDATE " + TABLE_SQ_LINK + " SET "
          + COLUMN_SQ_LNK_NAME + " = ?, "
          + COLUMN_SQ_LNK_UPDATE_USER + " = ?, "
          + COLUMN_SQ_LNK_UPDATE_DATE + " = ? "
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Enable or disable link
  public static final String STMT_ENABLE_LINK =
      "UPDATE " + TABLE_SQ_LINK + " SET "
          + COLUMN_SQ_LNK_ENABLED + " = ? "
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Delete rows from link input table
  public static final String STMT_DELETE_LINK_INPUT =
      "DELETE FROM " + TABLE_SQ_LINK_INPUT
          + " WHERE " + COLUMN_SQ_LNKI_LINK + " = ?";

  // DML: Delete row from link table
  public static final String STMT_DELETE_LINK =
      "DELETE FROM " + TABLE_SQ_LINK
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Select one specific link
  public static final String STMT_SELECT_LINK_SINGLE =
      "SELECT "
          + COLUMN_SQ_LNK_ID + ", "
          + COLUMN_SQ_LNK_NAME + ", "
          + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + COLUMN_SQ_LNK_ENABLED + ", "
          + COLUMN_SQ_LNK_CREATION_USER + ", "
          + COLUMN_SQ_LNK_CREATION_DATE + ", "
          + COLUMN_SQ_LNK_UPDATE_USER + ", "
          + COLUMN_SQ_LNK_UPDATE_DATE
          + " FROM " + TABLE_SQ_LINK
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  // DML: Select one specific link by name
  public static final String STMT_SELECT_LINK_SINGLE_BY_NAME =
      "SELECT "
          + COLUMN_SQ_LNK_ID + ", "
          + COLUMN_SQ_LNK_NAME + ", "
          + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + COLUMN_SQ_LNK_ENABLED + ", "
          + COLUMN_SQ_LNK_CREATION_USER + ", "
          + COLUMN_SQ_LNK_CREATION_DATE + ", "
          + COLUMN_SQ_LNK_UPDATE_USER + ", "
          + COLUMN_SQ_LNK_UPDATE_DATE
          + " FROM " + TABLE_SQ_LINK
          + " WHERE " + COLUMN_SQ_LNK_NAME + " = ?";

  // DML: Select all links
  public static final String STMT_SELECT_LINK_ALL =
      "SELECT "
          + COLUMN_SQ_LNK_ID + ", "
          + COLUMN_SQ_LNK_NAME + ", "
          + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + COLUMN_SQ_LNK_ENABLED + ", "
          + COLUMN_SQ_LNK_CREATION_USER + ", "
          + COLUMN_SQ_LNK_CREATION_DATE + ", "
          + COLUMN_SQ_LNK_UPDATE_USER + ", "
          + COLUMN_SQ_LNK_UPDATE_DATE
          + " FROM " + TABLE_SQ_LINK;

  // DML: Select all links for a specific connector.
  public static final String STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE =
      "SELECT "
          + COLUMN_SQ_LNK_ID + ", "
          + COLUMN_SQ_LNK_NAME + ", "
          + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + COLUMN_SQ_LNK_ENABLED + ", "
          + COLUMN_SQ_LNK_CREATION_USER + ", "
          + COLUMN_SQ_LNK_CREATION_DATE + ", "
          + COLUMN_SQ_LNK_UPDATE_USER + ", "
          + COLUMN_SQ_LNK_UPDATE_DATE
          + " FROM " + TABLE_SQ_LINK
          + " WHERE " + COLUMN_SQ_LNK_CONFIGURABLE + " = ?";

  // DML: Check if given link exists
  public static final String STMT_SELECT_LINK_CHECK_BY_ID =
      "SELECT count(*) FROM " + TABLE_SQ_LINK
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ?";

  /**********JOB TABLE **************/
  // DML: Insert new job
  public static final String STMT_INSERT_JOB =
      "INSERT INTO " + TABLE_SQ_JOB + " ("
          + COLUMN_SQB_NAME + ", "
          + COLUMN_SQB_FROM_LINK + ", "
          + COLUMN_SQB_TO_LINK + ", "
          + COLUMN_SQB_ENABLED + ", "
          + COLUMN_SQB_CREATION_USER + ", "
          + COLUMN_SQB_CREATION_DATE + ", "
          + COLUMN_SQB_UPDATE_USER + ", "
          + COLUMN_SQB_UPDATE_DATE
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new job inputs
  public static final String STMT_INSERT_JOB_INPUT =
      "INSERT INTO " + TABLE_SQ_JOB_INPUT + " ("
          + COLUMN_SQBI_JOB + ", "
          + COLUMN_SQBI_INPUT + ", "
          + COLUMN_SQBI_VALUE
          + ") VALUES (?, ?, ?)";

  public static final String STMT_UPDATE_JOB =
      "UPDATE " + TABLE_SQ_JOB + " SET "
          + COLUMN_SQB_NAME + " = ?, "
          + COLUMN_SQB_UPDATE_USER + " = ?, "
          + COLUMN_SQB_UPDATE_DATE + " = ? "
          + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Enable or disable job
  public static final String STMT_ENABLE_JOB =
      "UPDATE " + TABLE_SQ_JOB + " SET "
          + COLUMN_SQB_ENABLED + " = ? "
          + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Delete rows from job input table
  public static final String STMT_DELETE_JOB_INPUT =
      "DELETE FROM " + TABLE_SQ_JOB_INPUT
          + " WHERE " + COLUMN_SQBI_JOB + " = ?";

  // DML: Delete row from job table
  public static final String STMT_DELETE_JOB =
      "DELETE FROM " + TABLE_SQ_JOB
          + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if given job exists
  public static final String STMT_SELECT_JOB_CHECK_BY_ID =
      "SELECT count(*) FROM " + TABLE_SQ_JOB
          + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Check if there are jobs for given link
  public static final String STMT_SELECT_JOBS_FOR_LINK_CHECK =
      "SELECT"
          + " count(*)"
          + " FROM " + TABLE_SQ_JOB
          + " JOIN " + TABLE_SQ_LINK
          + " ON " + COLUMN_SQB_FROM_LINK + " = " + COLUMN_SQ_LNK_ID
          + " WHERE " + COLUMN_SQ_LNK_ID + " = ? ";

  //DML: Select all jobs
  public static final String STMT_SELECT_JOB =
      "SELECT "
          + "FROM_CONNECTOR." + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + "TO_CONNECTOR." + COLUMN_SQ_LNK_CONFIGURABLE + ", "
          + "JOB." + COLUMN_SQB_ID + ", "
          + "JOB." + COLUMN_SQB_NAME + ", "
          + "JOB." + COLUMN_SQB_FROM_LINK + ", "
          + "JOB." + COLUMN_SQB_TO_LINK + ", "
          + "JOB." + COLUMN_SQB_ENABLED + ", "
          + "JOB." + COLUMN_SQB_CREATION_USER + ", "
          + "JOB." + COLUMN_SQB_CREATION_DATE + ", "
          + "JOB." + COLUMN_SQB_UPDATE_USER + ", "
          + "JOB." + COLUMN_SQB_UPDATE_DATE
          + " FROM " + TABLE_SQ_JOB + " JOB"
          + " LEFT JOIN " + TABLE_SQ_LINK + " FROM_CONNECTOR"
          + " ON " + COLUMN_SQB_FROM_LINK + " = FROM_CONNECTOR." + COLUMN_SQ_LNK_ID
          + " LEFT JOIN " + TABLE_SQ_LINK + " TO_CONNECTOR"
          + " ON " + COLUMN_SQB_TO_LINK + " = TO_CONNECTOR." + COLUMN_SQ_LNK_ID;

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE_BY_ID =
      STMT_SELECT_JOB + " WHERE " + COLUMN_SQB_ID + " = ?";

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE_BY_NAME =
      STMT_SELECT_JOB + " WHERE " + COLUMN_SQB_NAME + " = ?";

  // DML: Select all jobs for a Connector
  public static final String STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE =
      STMT_SELECT_JOB
          + " WHERE FROM_LINK." + COLUMN_SQ_LNK_CONFIGURABLE + " = ? OR TO_LINK."
          + COLUMN_SQ_LNK_CONFIGURABLE + " = ?";

  /**********SUBMISSION TABLE **************/
  // DML: Insert new submission
  public static final String STMT_INSERT_SUBMISSION =
      "INSERT INTO " + TABLE_SQ_SUBMISSION + "("
          + COLUMN_SQS_JOB + ", "
          + COLUMN_SQS_STATUS + ", "
          + COLUMN_SQS_CREATION_USER + ", "
          + COLUMN_SQS_CREATION_DATE + ", "
          + COLUMN_SQS_UPDATE_USER + ", "
          + COLUMN_SQS_UPDATE_DATE + ", "
          + COLUMN_SQS_EXTERNAL_ID + ", "
          + COLUMN_SQS_EXTERNAL_LINK + ", "
          + COLUMN_SQS_EXCEPTION + ", "
          + COLUMN_SQS_EXCEPTION_TRACE + ") "
          + " VALUES(?, ?, ?, ?, ?, ?, ?, substr(?, 1, 150) , substr(?, 1, 150), substr(?, 1, 750))";

  // DML: Update existing submission
  public static final String STMT_UPDATE_SUBMISSION =
      "UPDATE " + TABLE_SQ_SUBMISSION + " SET "
          + COLUMN_SQS_STATUS + " = ?, "
          + COLUMN_SQS_UPDATE_USER + " = ?, "
          + COLUMN_SQS_UPDATE_DATE + " = ?, "
          + COLUMN_SQS_EXCEPTION + " = ?, "
          + COLUMN_SQS_EXCEPTION_TRACE + " = ?"
          + " WHERE " + COLUMN_SQS_ID + " = ?";

  // DML: Check if given submission exists
  public static final String STMT_SELECT_SUBMISSION_CHECK =
      "SELECT"
          + " count(*)"
          + " FROM " + TABLE_SQ_SUBMISSION
          + " WHERE " + COLUMN_SQS_ID + " = ?";

  // DML: Purge old entries
  public static final String STMT_PURGE_SUBMISSIONS =
      "DELETE FROM " + TABLE_SQ_SUBMISSION
          + " WHERE " + COLUMN_SQS_UPDATE_DATE + " < ?";

  // DML: Get unfinished
  public static final String STMT_SELECT_SUBMISSION_UNFINISHED =
      "SELECT "
          + COLUMN_SQS_ID + ", "
          + COLUMN_SQS_JOB + ", "
          + COLUMN_SQS_STATUS + ", "
          + COLUMN_SQS_CREATION_USER + ", "
          + COLUMN_SQS_CREATION_DATE + ", "
          + COLUMN_SQS_UPDATE_USER + ", "
          + COLUMN_SQS_UPDATE_DATE + ", "
          + COLUMN_SQS_EXTERNAL_ID + ", "
          + COLUMN_SQS_EXTERNAL_LINK + ", "
          + COLUMN_SQS_EXCEPTION + ", "
          + COLUMN_SQS_EXCEPTION_TRACE
          + " FROM " + TABLE_SQ_SUBMISSION
          + " WHERE " + COLUMN_SQS_STATUS + " = ?";

  // DML : Get all submissions
  public static final String STMT_SELECT_SUBMISSIONS =
      "SELECT "
          + COLUMN_SQS_ID + ", "
          + COLUMN_SQS_JOB + ", "
          + COLUMN_SQS_STATUS + ", "
          + COLUMN_SQS_CREATION_USER + ", "
          + COLUMN_SQS_CREATION_DATE + ", "
          + COLUMN_SQS_UPDATE_USER + ", "
          + COLUMN_SQS_UPDATE_DATE + ", "
          + COLUMN_SQS_EXTERNAL_ID + ", "
          + COLUMN_SQS_EXTERNAL_LINK + ", "
          + COLUMN_SQS_EXCEPTION + ", "
          + COLUMN_SQS_EXCEPTION_TRACE
          + " FROM " + TABLE_SQ_SUBMISSION
          + " ORDER BY " + COLUMN_SQS_UPDATE_DATE + " DESC";

  // DML: Get submissions for a job
  public static final String STMT_SELECT_SUBMISSIONS_FOR_JOB =
      "SELECT "
          + COLUMN_SQS_ID + ", "
          + COLUMN_SQS_JOB + ", "
          + COLUMN_SQS_STATUS + ", "
          + COLUMN_SQS_CREATION_USER + ", "
          + COLUMN_SQS_CREATION_DATE + ", "
          + COLUMN_SQS_UPDATE_USER + ", "
          + COLUMN_SQS_UPDATE_DATE + ", "
          + COLUMN_SQS_EXTERNAL_ID + ", "
          + COLUMN_SQS_EXTERNAL_LINK + ", "
          + COLUMN_SQS_EXCEPTION + ", "
          + COLUMN_SQS_EXCEPTION_TRACE
          + " FROM " + TABLE_SQ_SUBMISSION
          + " WHERE " + COLUMN_SQS_JOB + " = ?"
          + " ORDER BY " + COLUMN_SQS_UPDATE_DATE + " DESC";

  // DML: Select counter group
  public static final String STMT_SELECT_COUNTER_GROUP =
      "SELECT "
          + COLUMN_SQG_ID + ", "
          + COLUMN_SQG_NAME + " "
          + "FROM " + TABLE_SQ_COUNTER_GROUP + " "
          + "WHERE " + COLUMN_SQG_NAME + " = substr(?, 1, 75)";

  // DML: Insert new counter group
  public static final String STMT_INSERT_COUNTER_GROUP =
      "INSERT INTO " + TABLE_SQ_COUNTER_GROUP + " ("
          + COLUMN_SQG_NAME + ") "
          + "VALUES (substr(?, 1, 75))";

  // DML: Select counter
  public static final String STMT_SELECT_COUNTER =
      "SELECT "
          + COLUMN_SQR_ID + ", "
          + COLUMN_SQR_NAME + " "
          + "FROM " + TABLE_SQ_COUNTER + " "
          + "WHERE " + COLUMN_SQR_NAME + " = substr(?, 1, 75)";

  // DML: Insert new counter
  public static final String STMT_INSERT_COUNTER =
      "INSERT INTO " + TABLE_SQ_COUNTER + " ("
          + COLUMN_SQR_NAME + ") "
          + "VALUES (substr(?, 1, 75))";

  // DML: Insert new counter submission
  public static final String STMT_INSERT_COUNTER_SUBMISSION =
      "INSERT INTO " + TABLE_SQ_COUNTER_SUBMISSION + " ("
          + COLUMN_SQRS_GROUP + ", "
          + COLUMN_SQRS_COUNTER + ", "
          + COLUMN_SQRS_SUBMISSION + ", "
          + COLUMN_SQRS_VALUE + ") "
          + "VALUES (?, ?, ?, ?)";

  // DML: Select counter submission
  public static final String STMT_SELECT_COUNTER_SUBMISSION =
      "SELECT "
          + COLUMN_SQG_NAME + ", "
          + COLUMN_SQR_NAME + ", "
          + COLUMN_SQRS_VALUE + " "
          + "FROM " + TABLE_SQ_COUNTER_SUBMISSION + " "
          + "LEFT JOIN " + TABLE_SQ_COUNTER_GROUP
          + " ON " + COLUMN_SQRS_GROUP + " = " + COLUMN_SQG_ID + " "
          + "LEFT JOIN " + TABLE_SQ_COUNTER
          + " ON " + COLUMN_SQRS_COUNTER + " = " + COLUMN_SQR_ID + " "
          + "WHERE " + COLUMN_SQRS_SUBMISSION + " = ? ";

  // DML: Delete rows from counter submission table
  public static final String STMT_DELETE_COUNTER_SUBMISSION =
      "DELETE FROM " + TABLE_SQ_COUNTER_SUBMISSION
          + " WHERE " + COLUMN_SQRS_SUBMISSION + " = ?";

  /******* CONFIG and CONNECTOR DIRECTIONS ****/
  public static final String STMT_INSERT_SQ_CONNECTOR_DIRECTIONS =
      "INSERT INTO " + TABLE_SQ_CONNECTOR_DIRECTIONS + " "
          + "(" + COLUMN_SQCD_CONNECTOR + ", " + COLUMN_SQCD_DIRECTION + ")"
          + " VALUES (?, ?)";

  public static final String STMT_INSERT_SQ_CONFIG_DIRECTIONS =
      "INSERT INTO " + TABLE_SQ_CONFIG_DIRECTIONS + " "
          + "(" + COLUMN_SQ_CFG_DIR_CONFIG + ", " + COLUMN_SQ_CFG_DIR_DIRECTION + ")"
          + " VALUES (?, ?)";

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL =
      "SELECT " + COLUMN_SQCD_CONNECTOR + ", " + COLUMN_SQCD_DIRECTION
          + " FROM " + TABLE_SQ_CONNECTOR_DIRECTIONS;

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS =
      STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL + " WHERE "
          + COLUMN_SQCD_CONNECTOR + " = ?";

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL =
      "SELECT " + COLUMN_SQ_CFG_DIR_CONFIG + ", " + COLUMN_SQ_CFG_DIR_DIRECTION
          + " FROM " + TABLE_SQ_CONFIG_DIRECTIONS;

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS =
      STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL + " WHERE "
          + COLUMN_SQ_CFG_DIR_CONFIG + " = ?";
}
