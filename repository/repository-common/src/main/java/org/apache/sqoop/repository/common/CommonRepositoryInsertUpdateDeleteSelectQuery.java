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
  /**
   * ****DIRECTION TABLE *************
   */
  public static final String STMT_SELECT_SQD_ID_BY_SQD_NAME =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID)
              + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME)
              + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_NAME) + "=?";

  public static final String STMT_SELECT_SQD_NAME_BY_SQD_ID =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_NAME)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_DIRECTION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQD_ID) + "=?";

  /**
   * ******CONFIGURABLE TABLE **************
   */
  //DML: Get configurable by given name
  public static final String STMT_SELECT_FROM_CONFIGURABLE =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + " = ?";

  //DML: Get all configurables for a given type
  public static final String STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_TYPE) + " = ?";

  //DML: Insert into configurable
  public static final String STMT_INSERT_INTO_CONFIGURABLE =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_TYPE)
          + ") VALUES (?, ?, ?, ?)";

  //Delete all configs for a given configurable
  public static final String STMT_DELETE_CONFIGS_FOR_CONFIGURABLE =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + " = ?";

  //Delete all inputs for a given configurable
  public static final String STMT_DELETE_INPUTS_FOR_CONFIGURABLE =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG)
          + " IN (SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + " = ?)";

  public static final String STMT_DELETE_INPUT_RELATIONS_FOR_INPUT =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_RELATION_NAME)
          + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_PARENT)
          + " IN (SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG)
          + " IN (SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + " = ?))";

  //Update the configurable
  public static final String STMT_UPDATE_CONFIGURABLE =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIGURABLE_NAME)
          + " SET " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_NAME) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_CLASS) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_VERSION) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQC_TYPE) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQC_ID) + " = ?";

  /**
   * *******CONFIG TABLE *************
   */
  //DML: Get all configs for a given configurable
  public static final String STMT_SELECT_CONFIG_FOR_CONFIGURABLE =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_INDEX)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + " = ? "
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_INDEX);


  //DML: Insert into config
  public static final String STMT_INSERT_INTO_CONFIG =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_INDEX)
          + ") VALUES ( ?, ?, ?, ?)";

  /**
   * ******* INPUT TABLE *************
   */
  // DML: Get inputs for a given config
  public static final String STMT_SELECT_INPUT =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS) + ", "
          + "cast(null as varchar(100))"
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + " = ?"
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX);

   // DML get Input by Id
  public static final String STMT_SELECT_INPUT_BY_ID =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + " = ?";

  // DML get Input by name
  public static final String STMT_SELECT_INPUT_BY_NAME =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + " = ?";

  // DML: Insert into config input
  public static final String STMT_INSERT_INTO_INPUT =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS)
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

/********** INPUT-RELATIONSHIP TABLE **************/
  public static final String STMT_INSERT_INTO_INPUT_RELATION =
     "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_RELATION_NAME) + " ("
         + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_PARENT) + ", "
         + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_CHILD)
         + ") VALUES (?, ?)";

  public static final String STMT_FETCH_SQ_INPUT_OVERRIDES =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_CHILD)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_RELATION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQIR_PARENT) + " = ?";

  /**
   * *******LINK INPUT TABLE *************
   */
  //DML: Get inputs and values for a given link
  public static final String STMT_FETCH_LINK_INPUT =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ","
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_VALUE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " LEFT OUTER JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME)
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID)
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK) + " = ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + " = ?"
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX);

  /**
   * *******JOB INPUT TABLE *************
   */
  //DML: Fetch inputs and values for a given job
  public static final String STMT_FETCH_JOB_INPUT =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_TYPE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRMASK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_STRLENGTH) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_EDITABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ENUMVALS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_VALUE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_INPUT_NAME)
          + " LEFT OUTER JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME)
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_ID)
          + " AND  " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + " = ?"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_CONFIG) + " = ?"
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQI_INDEX);

  /**
   * *******LINK TABLE *************
   */
  // DML: Insert new link
  public static final String STMT_INSERT_LINK =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE)
          + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new link inputs
  public static final String STMT_INSERT_LINK_INPUT =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_VALUE)
          + ") VALUES (?, ?, ?)";

  // DML: Update link
  public static final String STMT_UPDATE_LINK =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ?";

  // DML: Enable or disable link
  public static final String STMT_ENABLE_LINK =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ?";


  // UPDATE the LINK Input
  public static final String STMT_UPDATE_LINK_INPUT =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_VALUE) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_INPUT) + " = ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK) + " = ?";

  // DML: Delete rows from link input table
  public static final String STMT_DELETE_LINK_INPUT =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_INPUT_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNKI_LINK) + " = ?";

  // DML: Delete row from link table
  public static final String STMT_DELETE_LINK =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ?";

  // DML: Select one specific link
  public static final String STMT_SELECT_LINK_SINGLE =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ?";

  // DML: Select one specific link by name
  public static final String STMT_SELECT_LINK_SINGLE_BY_NAME =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + " = ?";

  // DML: Select all links
  public static final String STMT_SELECT_LINK_ALL =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME);

  // DML: Select all links for a specific connector.
  public static final String STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_UPDATE_DATE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + " = ?";

  // DML: Check if given link exists
  public static final String STMT_SELECT_LINK_CHECK_BY_ID =
      "SELECT count(*) FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ?";

  /**
   * *******JOB TABLE *************
   */
  // DML: Insert new job
  public static final String STMT_INSERT_JOB =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ENABLED) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_DATE)
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

  // DML: Insert new job inputs
  public static final String STMT_INSERT_JOB_INPUT =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_VALUE)
          + ") VALUES (?, ?, ?)";

  public static final String STMT_UPDATE_JOB =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_USER) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_DATE) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  // DML: Enable or disable job
  public static final String STMT_ENABLE_JOB =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ENABLED) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  // UPDATE the JOB Input
  public static final String STMT_UPDATE_JOB_INPUT =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_VALUE) + " = ? "
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_INPUT) + " = ?"
          + " AND " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + " = ?";

  // DML: Delete rows from job input table
  public static final String STMT_DELETE_JOB_INPUT =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_INPUT_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQBI_JOB) + " = ?";

  // DML: Delete row from job table
  public static final String STMT_DELETE_JOB =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  // DML: Check if given job exists
  public static final String STMT_SELECT_JOB_CHECK_BY_ID =
      "SELECT count(*) FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  // DML: Check if there are jobs for given link
  public static final String STMT_SELECT_JOBS_FOR_LINK_CHECK =
      "SELECT"
          + " count(*)"
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME)
          + " JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME)
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK) + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID) + " = ? ";

  //DML: Select all jobs
  public static final String STMT_SELECT_JOB_ALL =
      "SELECT "
          + "FROM_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + "TO_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_LINK) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ENABLED) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_USER) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_CREATION_DATE) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_USER) + ", "
          + "JOB." + CommonRepoUtils.escapeColumnName(COLUMN_SQB_UPDATE_DATE)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_JOB_NAME) + " JOB"
          + " LEFT JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " FROM_CONNECTOR"
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_FROM_LINK) + " = FROM_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID)
          + " LEFT JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_LINK_NAME) + " TO_CONNECTOR"
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_TO_LINK) + " = TO_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_ID);

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE_BY_ID =
      STMT_SELECT_JOB_ALL +
          " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_ID) + " = ?";

  // DML: Select one specific job
  public static final String STMT_SELECT_JOB_SINGLE_BY_NAME =
      STMT_SELECT_JOB_ALL +
          " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQB_NAME) + " = ?";

  // DML: Select all jobs for a Connector
  public static final String STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE =
      STMT_SELECT_JOB_ALL +
          " WHERE FROM_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + " = ?" +
          " OR TO_CONNECTOR." + CommonRepoUtils.escapeColumnName(COLUMN_SQ_LNK_CONFIGURABLE) + " = ?";

  /**
   * *******SUBMISSION TABLE *************
   */
  // DML: Insert new submission
  public static final String STMT_INSERT_SUBMISSION =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + "("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS) + ") "
          + " VALUES(?, ?, ?, ?, ?, ?, ?, substr(?, 1, 150) , substr(?, 1, 150), substr(?, 1, 750))";

  // DML: Update existing submission
  public static final String STMT_UPDATE_SUBMISSION =
      "UPDATE " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME) + " SET "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + " = ?, "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY) + " = substr(?, 1, 150), "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS) + " = substr(?, 1, 750)"
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + " = ?";

  // DML: Check if given submission exists
  public static final String STMT_SELECT_SUBMISSION_CHECK =
      "SELECT"
          + " count(*)"
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + " = ?";

  // DML: Purge old entries
  public static final String STMT_PURGE_SUBMISSIONS =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + " < ?";

  // DML: Get unfinished
  public static final String STMT_SELECT_SUBMISSION_UNFINISHED =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + " = ?";

  // DML : Get all submissions
  public static final String STMT_SELECT_SUBMISSIONS =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + " DESC";

  // DML: Get submissions for a job
  public static final String STMT_SELECT_SUBMISSIONS_FOR_JOB =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_STATUS) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_CREATION_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_USER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_EXTERNAL_LINK) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_SUMMARY) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQS_ERROR_DETAILS)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_SUBMISSION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_JOB) + " = ?"
          + " ORDER BY " + CommonRepoUtils.escapeColumnName(COLUMN_SQS_UPDATE_DATE) + " DESC";

  // DML: Select counter group
  public static final String STMT_SELECT_COUNTER_GROUP =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + " "
          + "FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME) + " "
          + "WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + " = substr(?, 1, 75)";

  // DML: Insert new counter group
  public static final String STMT_INSERT_COUNTER_GROUP =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + ") "
          + "VALUES (substr(?, 1, 75))";

  // DML: Select counter
  public static final String STMT_SELECT_COUNTER =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + " "
          + "FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME) + " "
          + "WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + " = substr(?, 1, 75)";

  // DML: Insert new counter
  public static final String STMT_INSERT_COUNTER =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + ") "
          + "VALUES (substr(?, 1, 75))";

  // DML: Insert new counter submission
  public static final String STMT_INSERT_COUNTER_SUBMISSION =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME) + " ("
          + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_VALUE) + ") "
          + "VALUES (?, ?, ?, ?)";

  // DML: Select counter submission
  public static final String STMT_SELECT_COUNTER_SUBMISSION =
      "SELECT "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQG_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQR_NAME) + ", "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_VALUE) + " "
          + "FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME) + " "
          + "LEFT JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_GROUP_NAME)
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_GROUP) + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQG_ID) + " "
          + "LEFT JOIN " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_NAME)
          + " ON " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_COUNTER) + " = " + CommonRepoUtils.escapeColumnName(COLUMN_SQR_ID) + " "
          + "WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + " = ? ";

  // DML: Delete rows from counter submission table
  public static final String STMT_DELETE_COUNTER_SUBMISSION =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_COUNTER_SUBMISSION_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQRS_SUBMISSION) + " = ?";

  /**
   * **** CONFIG and CONNECTOR DIRECTIONS ***
   */
  public static final String STMT_INSERT_SQ_CONNECTOR_DIRECTIONS =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_DIRECTIONS_NAME) + " "
          + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_DIRECTION) + ")"
          + " VALUES (?, ?)";

  public static final String STMT_INSERT_SQ_CONFIG_DIRECTIONS =
      "INSERT INTO " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_DIRECTIONS_NAME) + " "
          + "(" + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_DIRECTION) + ")"
          + " VALUES (?, ?)";

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_DIRECTION)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONNECTOR_DIRECTIONS_NAME);

  public static final String STMT_SELECT_SQ_CONNECTOR_DIRECTIONS =
      STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQCD_CONNECTOR) + " = ?";

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL =
      "SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG) + ", " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_DIRECTION)
          + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_DIRECTIONS_NAME);

  public static final String STMT_SELECT_SQ_CONFIG_DIRECTIONS =
      STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL + " WHERE "
          + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG) + " = ?";

  // Delete the config directions for a connector
  public static final String STMT_DELETE_DIRECTIONS_FOR_CONFIGURABLE =
      "DELETE FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_DIRECTIONS_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_DIR_CONFIG)
          + " IN (SELECT " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_ID) + " FROM " + CommonRepoUtils.getTableName(SCHEMA_SQOOP, TABLE_SQ_CONFIG_NAME)
          + " WHERE " + CommonRepoUtils.escapeColumnName(COLUMN_SQ_CFG_CONFIGURABLE) + " = ?)";

  public String getStmtSelectSqdIdBySqdName() {
    return STMT_SELECT_SQD_ID_BY_SQD_NAME;
  }

  public String getStmtSelectSqConfigDirections() {
    return STMT_SELECT_SQ_CONFIG_DIRECTIONS;
  }

  public String getStmtDeleteInputsForConfigurable() {
    return STMT_DELETE_INPUTS_FOR_CONFIGURABLE;
  }

  public String getStmtUpdateConfigurable() {
    return STMT_UPDATE_CONFIGURABLE;
  }

  public String getStmtSelectConfigForConfigurable() {
    return STMT_SELECT_CONFIG_FOR_CONFIGURABLE;
  }

  public String getStmtInsertIntoConfig() {
    return STMT_INSERT_INTO_CONFIG;
  }

  public String getStmtSelectInput() {
    return STMT_SELECT_INPUT;
  }

  public String getStmtSelectInputById() {
    return STMT_SELECT_INPUT_BY_ID;
  }

  public String getStmtSelectInputByName() {
    return STMT_SELECT_INPUT_BY_NAME;
  }

  public String getStmtSelectInputOverrides() {
    return STMT_FETCH_SQ_INPUT_OVERRIDES;
  }

  public String getStmtInsertIntoInput() {
    return STMT_INSERT_INTO_INPUT;
  }

  public String getStmtFetchLinkInput() {
    return STMT_FETCH_LINK_INPUT;
  }

  public String getStmtFetchJobInput() {
    return STMT_FETCH_JOB_INPUT;
  }

  public String getStmtInsertLink() {
    return STMT_INSERT_LINK;
  }

  public String getStmtInsertLinkInput() {
    return STMT_INSERT_LINK_INPUT;
  }

  public String getStmtUpdateLink() {
    return STMT_UPDATE_LINK;
  }

  public String getStmtEnableLink() {
    return STMT_ENABLE_LINK;
  }

  public String getStmtUpdateLinkInput() {
    return STMT_UPDATE_LINK_INPUT;
  }

  public String getStmtDeleteLinkInput() {
    return STMT_DELETE_LINK_INPUT;
  }

  public String getStmtDeleteLink() {
    return STMT_DELETE_LINK;
  }

  public String getStmtSelectLinkSingle() {
    return STMT_SELECT_LINK_SINGLE;
  }

  public String getStmtSelectLinkSingleByName() {
    return STMT_SELECT_LINK_SINGLE_BY_NAME;
  }

  public String getStmtSelectLinkAll() {
    return STMT_SELECT_LINK_ALL;
  }

  public String getStmtSelectLinkForConnectorConfigurable() {
    return STMT_SELECT_LINK_FOR_CONNECTOR_CONFIGURABLE;
  }

  public String getStmtSelectLinkCheckById() {
    return STMT_SELECT_LINK_CHECK_BY_ID;
  }

  public String getStmtInsertJob() {
    return STMT_INSERT_JOB;
  }

  public String getStmtInsertJobInput() {
    return STMT_INSERT_JOB_INPUT;
  }

  public String getStmtUpdateJob() {
    return STMT_UPDATE_JOB;
  }

  public String getStmtEnableJob() {
    return STMT_ENABLE_JOB;
  }

  public String getStmtUpdateJobInput() {
    return STMT_UPDATE_JOB_INPUT;
  }

  public String getStmtDeleteJobInput() {
    return STMT_DELETE_JOB_INPUT;
  }

  public String getStmtDeleteJob() {
    return STMT_DELETE_JOB;
  }

  public String getStmtSelectJobCheckById() {
    return STMT_SELECT_JOB_CHECK_BY_ID;
  }

  public String getStmtSelectJobsForLinkCheck() {
    return STMT_SELECT_JOBS_FOR_LINK_CHECK;
  }

  public String getStmtSelectJobAll() {
    return STMT_SELECT_JOB_ALL;
  }

  public String getStmtSelectJobSingleById() {
    return STMT_SELECT_JOB_SINGLE_BY_ID;
  }

  public String getStmtSelectJobSingleByName() {
    return STMT_SELECT_JOB_SINGLE_BY_NAME;
  }

  public String getStmtSelectAllJobsForConnectorConfigurable() {
    return STMT_SELECT_ALL_JOBS_FOR_CONNECTOR_CONFIGURABLE;
  }

  public String getStmtInsertSubmission() {
    return STMT_INSERT_SUBMISSION;
  }

  public String getStmtUpdateSubmission() {
    return STMT_UPDATE_SUBMISSION;
  }

  public String getStmtSelectSubmissionCheck() {
    return STMT_SELECT_SUBMISSION_CHECK;
  }

  public String getStmtPurgeSubmissions() {
    return STMT_PURGE_SUBMISSIONS;
  }

  public String getStmtSelectSubmissionUnfinished() {
    return STMT_SELECT_SUBMISSION_UNFINISHED;
  }

  public String getStmtSelectSubmissions() {
    return STMT_SELECT_SUBMISSIONS;
  }

  public String getStmtSelectSubmissionsForJob() {
    return STMT_SELECT_SUBMISSIONS_FOR_JOB;
  }

  public String getStmtSelectCounterGroup() {
    return STMT_SELECT_COUNTER_GROUP;
  }

  public String getStmtInsertCounterGroup() {
    return STMT_INSERT_COUNTER_GROUP;
  }

  public String getStmtSelectCounter() {
    return STMT_SELECT_COUNTER;
  }

  public String getStmtInsertCounter() {
    return STMT_INSERT_COUNTER;
  }

  public String getStmtInsertCounterSubmission() {
    return STMT_INSERT_COUNTER_SUBMISSION;
  }

  public String getStmtSelectCounterSubmission() {
    return STMT_SELECT_COUNTER_SUBMISSION;
  }

  public String getStmtDeleteCounterSubmission() {
    return STMT_DELETE_COUNTER_SUBMISSION;
  }

  public String getStmtInsertSqConnectorDirections() {
    return STMT_INSERT_SQ_CONNECTOR_DIRECTIONS;
  }

  public String getStmtInsertSqConfigDirections() {
    return STMT_INSERT_SQ_CONFIG_DIRECTIONS;
  }

  public String getStmtSelectSqConnectorDirectionsAll() {
    return STMT_SELECT_SQ_CONNECTOR_DIRECTIONS_ALL;
  }

  public String getStmtSelectSqConnectorDirections() {
    return STMT_SELECT_SQ_CONNECTOR_DIRECTIONS;
  }

  public String getStmtSelectSqConfigDirectionsAll() {
    return STMT_SELECT_SQ_CONFIG_DIRECTIONS_ALL;
  }

  public String getStmtSelectSqdNameBySqdId() {
    return STMT_SELECT_SQD_NAME_BY_SQD_ID;
  }

  public String getStmtSelectFromConfigurable() {
    return STMT_SELECT_FROM_CONFIGURABLE;
  }

  public String getStmtSelectConfigurableAllForType() {
    return STMT_SELECT_CONFIGURABLE_ALL_FOR_TYPE;
  }

  public String getStmtInsertIntoConfigurable() {
    return STMT_INSERT_INTO_CONFIGURABLE;
  }

  public String getStmtDeleteConfigsForConfigurable() {
    return STMT_DELETE_CONFIGS_FOR_CONFIGURABLE;
  }

  public String getStmtDeleteDirectionsForConfigurable() {
    return STMT_DELETE_DIRECTIONS_FOR_CONFIGURABLE;
  }
}