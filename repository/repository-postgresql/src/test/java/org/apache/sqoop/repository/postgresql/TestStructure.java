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

import org.junit.Test;

/**
 * Test connector methods on PostgreSQL repository.
 */
public class TestStructure extends PostgresqlTestCase {

  @Test
  public void testTables() throws Exception {
    utils.assertTableExists("sqoop", "sq_system");
    utils.assertTableExists("sqoop", "sq_direction");
    utils.assertTableExists("sqoop", "sq_configurable");
    utils.assertTableExists("sqoop", "sq_connector_directions");
    utils.assertTableExists("sqoop", "sq_config");
    utils.assertTableExists("sqoop", "sq_connector_directions");
    utils.assertTableExists("sqoop", "sq_input");
    utils.assertTableExists("sqoop", "sq_link");
    utils.assertTableExists("sqoop", "sq_job");
    utils.assertTableExists("sqoop", "sq_link_input");
    utils.assertTableExists("sqoop", "sq_job_input");
    utils.assertTableExists("sqoop", "sq_submission");
    utils.assertTableExists("sqoop", "sq_counter_group");
    utils.assertTableExists("sqoop", "sq_counter");
    utils.assertTableExists("sqoop", "sq_counter_submission");
  }

  @Test
  public void testForeignKeys() throws Exception {
    utils.assertForeignKey("sqoop", "sq_configurable", "sqc_id", "sq_connector_directions", "sqcd_connector");
    utils.assertForeignKey("sqoop", "sq_direction", "sqd_id", "sq_connector_directions", "sqcd_direction");
    utils.assertForeignKey("sqoop", "sq_configurable", "sqc_id", "sq_config", "sq_cfg_configurable");
    utils.assertForeignKey("sqoop", "sq_config", "sq_cfg_id", "sq_config_directions", "sq_cfg_dir_config");
    utils.assertForeignKey("sqoop", "sq_direction", "sqd_id", "sq_config_directions", "sq_cfg_dir_direction");
    utils.assertForeignKey("sqoop", "sq_config", "sq_cfg_id", "sq_input", "sqi_config");
    utils.assertForeignKey("sqoop", "sq_configurable", "sqc_id", "sq_link", "sq_lnk_configurable");
    utils.assertForeignKey("sqoop", "sq_link", "sq_lnk_id", "sq_job", "sqb_from_link");
    utils.assertForeignKey("sqoop", "sq_link", "sq_lnk_id", "sq_job", "sqb_to_link");
    utils.assertForeignKey("sqoop", "sq_link", "sq_lnk_id", "sq_link_input", "sq_lnki_link");
    utils.assertForeignKey("sqoop", "sq_input", "sqi_id", "sq_link_input", "sq_lnki_input");
    utils.assertForeignKey("sqoop", "sq_job", "sqb_id", "sq_job_input", "sqbi_job");
    utils.assertForeignKey("sqoop", "sq_input", "sqi_id", "sq_job_input", "sqbi_input");
    utils.assertForeignKey("sqoop", "sq_job", "sqb_id", "sq_submission", "sqs_job");
    utils.assertForeignKey("sqoop", "sq_counter", "sqr_id", "sq_counter_submission", "sqrs_counter");
    utils.assertForeignKey("sqoop", "sq_counter_group", "sqg_id", "sq_counter_submission", "sqrs_group");
    utils.assertForeignKey("sqoop", "sq_submission", "sqs_id", "sq_counter_submission", "sqrs_submission");
  }

  @Test
  public void testUniqueConstraints() throws Exception {
    utils.assertUniqueConstraints("sqoop", "sq_configurable", "sqc_name");
    utils.assertUniqueConstraints("sqoop", "sq_link", "sq_lnk_name");
    utils.assertUniqueConstraints("sqoop", "sq_job", "sqb_name");
    utils.assertUniqueConstraints("sqoop", "sq_config", "sq_cfg_name", "sq_cfg_configurable", "sq_cfg_type");
    utils.assertUniqueConstraints("sqoop", "sq_input", "sqi_name", "sqi_type", "sqi_config");
    utils.assertUniqueConstraints("sqoop", "sq_counter", "sqr_name");
    utils.assertUniqueConstraints("sqoop", "sq_counter_group", "sqg_name");
  }
}
