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
package org.apache.sqoop.integration.repository.postgresql;

import org.testng.annotations.Test;

/**
 * Test connector methods on PostgreSQL repository.
 */
@Test(groups = "postgresql")
public class TestStructure extends PostgresqlTestCase {

  @Test
  public void testTables() throws Exception {
    utils.assertTableExists("SQOOP", "SQ_SYSTEM");
    utils.assertTableExists("SQOOP", "SQ_DIRECTION");
    utils.assertTableExists("SQOOP", "SQ_CONFIGURABLE");
    utils.assertTableExists("SQOOP", "SQ_CONNECTOR_DIRECTIONS");
    utils.assertTableExists("SQOOP", "SQ_CONFIG");
    utils.assertTableExists("SQOOP", "SQ_CONNECTOR_DIRECTIONS");
    utils.assertTableExists("SQOOP", "SQ_INPUT");
    utils.assertTableExists("SQOOP", "SQ_INPUT_RELATION");
    utils.assertTableExists("SQOOP", "SQ_LINK");
    utils.assertTableExists("SQOOP", "SQ_JOB");
    utils.assertTableExists("SQOOP", "SQ_LINK_INPUT");
    utils.assertTableExists("SQOOP", "SQ_JOB_INPUT");
    utils.assertTableExists("SQOOP", "SQ_SUBMISSION");
    utils.assertTableExists("SQOOP", "SQ_COUNTER_GROUP");
    utils.assertTableExists("SQOOP", "SQ_COUNTER");
    utils.assertTableExists("SQOOP", "SQ_COUNTER_SUBMISSION");
  }

  @Test
  public void testForeignKeys() throws Exception {
    utils.assertForeignKey("SQOOP", "SQ_CONFIGURABLE", "SQC_ID", "SQ_CONNECTOR_DIRECTIONS", "SQCD_CONNECTOR");
    utils.assertForeignKey("SQOOP", "SQ_DIRECTION", "SQD_ID", "SQ_CONNECTOR_DIRECTIONS", "SQCD_DIRECTION");
    utils.assertForeignKey("SQOOP", "SQ_CONFIGURABLE", "SQC_ID", "SQ_CONFIG", "SQ_CFG_CONFIGURABLE");
    utils.assertForeignKey("SQOOP", "SQ_CONFIG", "SQ_CFG_ID", "SQ_CONFIG_DIRECTIONS", "SQ_CFG_DIR_CONFIG");
    utils.assertForeignKey("SQOOP", "SQ_DIRECTION", "SQD_ID", "SQ_CONFIG_DIRECTIONS", "SQ_CFG_DIR_DIRECTION");
    utils.assertForeignKey("SQOOP", "SQ_CONFIG", "SQ_CFG_ID", "SQ_INPUT", "SQI_CONFIG");
    utils.assertForeignKey("SQOOP", "SQ_CONFIGURABLE", "SQC_ID", "SQ_LINK", "SQ_LNK_CONFIGURABLE");
    utils.assertForeignKey("SQOOP", "SQ_LINK", "SQ_LNK_ID", "SQ_JOB", "SQB_FROM_LINK");
    utils.assertForeignKey("SQOOP", "SQ_LINK", "SQ_LNK_ID", "SQ_JOB", "SQB_TO_LINK");
    utils.assertForeignKey("SQOOP", "SQ_LINK", "SQ_LNK_ID", "SQ_LINK_INPUT", "SQ_LNKI_LINK");
    utils.assertForeignKey("SQOOP", "SQ_INPUT", "SQI_ID", "SQ_LINK_INPUT", "SQ_LNKI_INPUT");
    utils.assertForeignKey("SQOOP", "SQ_INPUT", "SQI_ID", "SQ_INPUT_RELATION", "SQIR_PARENT_ID");
    utils.assertForeignKey("SQOOP", "SQ_INPUT", "SQI_ID", "SQ_INPUT_RELATION", "SQIR_CHILD_ID");
    utils.assertForeignKey("SQOOP", "SQ_JOB", "SQB_ID", "SQ_JOB_INPUT", "SQBI_JOB");
    utils.assertForeignKey("SQOOP", "SQ_INPUT", "SQI_ID", "SQ_JOB_INPUT", "SQBI_INPUT");
    utils.assertForeignKey("SQOOP", "SQ_JOB", "SQB_ID", "SQ_SUBMISSION", "SQS_JOB");
    utils.assertForeignKey("SQOOP", "SQ_COUNTER", "SQR_ID", "SQ_COUNTER_SUBMISSION", "SQRS_COUNTER");
    utils.assertForeignKey("SQOOP", "SQ_COUNTER_GROUP", "SQG_ID", "SQ_COUNTER_SUBMISSION", "SQRS_GROUP");
    utils.assertForeignKey("SQOOP", "SQ_SUBMISSION", "SQS_ID", "SQ_COUNTER_SUBMISSION", "SQRS_SUBMISSION");
  }

  @Test
  public void testUniqueConstraints() throws Exception {
    utils.assertUniqueConstraints("SQOOP", "SQ_CONFIGURABLE", "SQC_NAME");
    utils.assertUniqueConstraints("SQOOP", "SQ_LINK", "SQ_LNK_NAME");
    utils.assertUniqueConstraints("SQOOP", "SQ_JOB", "SQB_NAME");
    utils.assertUniqueConstraints("SQOOP", "SQ_CONFIG", "SQ_CFG_NAME", "SQ_CFG_CONFIGURABLE", "SQ_CFG_TYPE");
    utils.assertUniqueConstraints("SQOOP", "SQ_INPUT", "SQI_NAME", "SQI_TYPE", "SQI_CONFIG");
    utils.assertUniqueConstraints("SQOOP", "SQ_COUNTER", "SQR_NAME");
    utils.assertUniqueConstraints("SQOOP", "SQ_COUNTER_GROUP", "SQG_NAME");
  }
}
