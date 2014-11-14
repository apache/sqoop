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
package org.apache.sqoop.connector.jdbc;

import org.apache.sqoop.connector.jdbc.configuration.FromJobConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.LinkConfiguration;
import org.apache.sqoop.connector.jdbc.configuration.ToJobConfiguration;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MToConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

/**
 * Test upgrader.
 */
public class TestGenericJdbcConnectorUpgrader {

  private GenericJdbcConnectorUpgrader upgrader;

  @Before
  public void setUp() {
    upgrader = new GenericJdbcConnectorUpgrader();
  }

  @Test
  public void testFromConfig() {
    // No upgrade
    MFromConfig originalConfigs = new MFromConfig(ConfigUtils.toConfigs(FromJobConfiguration.class));
    MFromConfig newConfigs = new MFromConfig(ConfigUtils.toConfigs(FromJobConfiguration.class));
    originalConfigs.getInput("fromJobConfig.schemaName").setValue("test-schema");
    originalConfigs.getInput("fromJobConfig.tableName").setValue("test-tableName");
    originalConfigs.getInput("fromJobConfig.sql").setValue("test-sql");
    originalConfigs.getInput("fromJobConfig.columns").setValue("test-columns");
    originalConfigs.getInput("fromJobConfig.partitionColumn").setValue("test-partitionColumn");
    originalConfigs.getInput("fromJobConfig.allowNullValueInPartitionColumn").setValue("test-allowNullValueInPartitionColumn");
    upgrader.upgradeFromJobConfig(originalConfigs, newConfigs);
    assertEquals(originalConfigs, newConfigs);
    assertEquals("test-schema", newConfigs.getInput("fromJobConfig.schemaName").getValue());
    assertEquals("test-tableName", newConfigs.getInput("fromJobConfig.tableName").getValue());
    assertEquals("test-sql", newConfigs.getInput("fromJobConfig.sql").getValue());
    assertEquals("test-columns", newConfigs.getInput("fromJobConfig.columns").getValue());
    assertEquals("test-partitionColumn", newConfigs.getInput("fromJobConfig.partitionColumn").getValue());
    assertEquals("test-allowNullValueInPartitionColumn", newConfigs.getInput("fromJobConfig.allowNullValueInPartitionColumn").getValue());

    // 1.99.3 upgrade to 1.99.4
    originalConfigs = new MFromConfig(new LinkedList<MConfig>());
    newConfigs = new MFromConfig(ConfigUtils.toConfigs(FromJobConfiguration.class));
    originalConfigs.getConfigs().add(new MConfig("table", new LinkedList<MInput<?>>()));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.schemaName", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.tableName", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.sql", false, (short)2000));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.columns", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.partitionColumn", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MBooleanInput("table.partitionColumnNull", false));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.boundaryQuery", false, (short)50));
    originalConfigs.getInput("table.schemaName").setValue("test-schema");
    originalConfigs.getInput("table.tableName").setValue("test-tableName");
    originalConfigs.getInput("table.sql").setValue("test-sql");
    originalConfigs.getInput("table.columns").setValue("test-columns");
    originalConfigs.getInput("table.partitionColumn").setValue("test-partitionColumn");
    originalConfigs.getInput("table.partitionColumnNull").setValue("test-allowNullValueInPartitionColumn");
    upgrader.upgradeFromJobConfig(originalConfigs, newConfigs);
    assertEquals("test-schema", newConfigs.getInput("fromJobConfig.schemaName").getValue());
    assertEquals("test-tableName", newConfigs.getInput("fromJobConfig.tableName").getValue());
    assertEquals("test-sql", newConfigs.getInput("fromJobConfig.sql").getValue());
    assertEquals("test-columns", newConfigs.getInput("fromJobConfig.columns").getValue());
    assertEquals("test-partitionColumn", newConfigs.getInput("fromJobConfig.partitionColumn").getValue());
    assertEquals("test-allowNullValueInPartitionColumn", newConfigs.getInput("fromJobConfig.allowNullValueInPartitionColumn").getValue());
  }

  @Test
  public void testToConfig() {
    // No upgrade
    MToConfig originalConfigs = new MToConfig(ConfigUtils.toConfigs(ToJobConfiguration.class));
    MToConfig newConfigs = new MToConfig(ConfigUtils.toConfigs(ToJobConfiguration.class));
    originalConfigs.getInput("toJobConfig.schemaName").setValue("test-schema");
    originalConfigs.getInput("toJobConfig.tableName").setValue("test-tableName");
    originalConfigs.getInput("toJobConfig.sql").setValue("test-sql");
    originalConfigs.getInput("toJobConfig.columns").setValue("test-columns");
    originalConfigs.getInput("toJobConfig.stageTableName").setValue("test-stageTableName");
    originalConfigs.getInput("toJobConfig.shouldClearStageTable").setValue("test-shouldClearStageTable");
    upgrader.upgradeToJobConfig(originalConfigs, newConfigs);
    assertEquals(originalConfigs, newConfigs);
    assertEquals("test-schema", newConfigs.getInput("toJobConfig.schemaName").getValue());
    assertEquals("test-tableName", newConfigs.getInput("toJobConfig.tableName").getValue());
    assertEquals("test-sql", newConfigs.getInput("toJobConfig.sql").getValue());
    assertEquals("test-columns", newConfigs.getInput("toJobConfig.columns").getValue());
    assertEquals("test-stageTableName", newConfigs.getInput("toJobConfig.stageTableName").getValue());
    assertEquals("test-shouldClearStageTable", newConfigs.getInput("toJobConfig.shouldClearStageTable").getValue());

    // 1.99.3 upgrade to 1.99.4
    originalConfigs = new MToConfig(new LinkedList<MConfig>());
    newConfigs = new MToConfig(ConfigUtils.toConfigs(ToJobConfiguration.class));
    originalConfigs.getConfigs().add(new MConfig("table", new LinkedList<MInput<?>>()));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.schemaName", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.tableName", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.sql", false, (short)2000));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.columns", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("table.stageTableName", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MBooleanInput("table.clearStageTable", false));
    originalConfigs.getInput("table.schemaName").setValue("test-schema");
    originalConfigs.getInput("table.tableName").setValue("test-tableName");
    originalConfigs.getInput("table.sql").setValue("test-sql");
    originalConfigs.getInput("table.columns").setValue("test-columns");
    originalConfigs.getInput("table.stageTableName").setValue("test-stageTableName");
    originalConfigs.getInput("table.clearStageTable").setValue("test-shouldClearStageTable");
    upgrader.upgradeToJobConfig(originalConfigs, newConfigs);
    assertEquals("test-schema", newConfigs.getInput("toJobConfig.schemaName").getValue());
    assertEquals("test-tableName", newConfigs.getInput("toJobConfig.tableName").getValue());
    assertEquals("test-sql", newConfigs.getInput("toJobConfig.sql").getValue());
    assertEquals("test-columns", newConfigs.getInput("toJobConfig.columns").getValue());
    assertEquals("test-stageTableName", newConfigs.getInput("toJobConfig.stageTableName").getValue());
    assertEquals("test-shouldClearStageTable", newConfigs.getInput("toJobConfig.shouldClearStageTable").getValue());
  }

  @Test
  public void testLinkConfig() {
    // No upgrade
    MLinkConfig originalConfigs = new MLinkConfig(ConfigUtils.toConfigs(LinkConfiguration.class));
    MLinkConfig newConfigs = new MLinkConfig(ConfigUtils.toConfigs(LinkConfiguration.class));
    originalConfigs.getInput("linkConfig.jdbcDriver").setValue("test-jdbcDriver");
    originalConfigs.getInput("linkConfig.connectionString").setValue("test-connectionString");
    originalConfigs.getInput("linkConfig.username").setValue("test-username");
    originalConfigs.getInput("linkConfig.password").setValue("test-password");
    originalConfigs.getInput("linkConfig.jdbcProperties").setValue("test-jdbcProperties");
    upgrader.upgradeLinkConfig(originalConfigs, newConfigs);
    assertEquals(originalConfigs, newConfigs);
    assertEquals("test-jdbcDriver", newConfigs.getInput("linkConfig.jdbcDriver").getValue());
    assertEquals("test-connectionString", newConfigs.getInput("linkConfig.connectionString").getValue());
    assertEquals("test-username", newConfigs.getInput("linkConfig.username").getValue());
    assertEquals("test-password", newConfigs.getInput("linkConfig.password").getValue());
    assertEquals("test-jdbcProperties", newConfigs.getInput("linkConfig.jdbcProperties").getValue());

    // 1.99.3 upgrade to 1.99.4
    originalConfigs = new MLinkConfig(new LinkedList<MConfig>());
    newConfigs = new MLinkConfig(ConfigUtils.toConfigs(LinkConfiguration.class));
    originalConfigs.getConfigs().add(new MConfig("connection", new LinkedList<MInput<?>>()));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("connection.jdbcDriver", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("connection.connectionString", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("connection.username", false, (short)2000));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("connection.password", false, (short)50));
    originalConfigs.getConfigs().get(0).getInputs().add(new MStringInput("connection.jdbcProperties", false, (short)50));
    originalConfigs.getInput("connection.jdbcDriver").setValue("test-jdbcDriver");
    originalConfigs.getInput("connection.connectionString").setValue("test-connectionString");
    originalConfigs.getInput("connection.username").setValue("test-username");
    originalConfigs.getInput("connection.password").setValue("test-password");
    originalConfigs.getInput("connection.jdbcProperties").setValue("test-jdbcProperties");
    upgrader.upgradeLinkConfig(originalConfigs, newConfigs);
    assertEquals("test-jdbcDriver", newConfigs.getInput("linkConfig.jdbcDriver").getValue());
    assertEquals("test-connectionString", newConfigs.getInput("linkConfig.connectionString").getValue());
    assertEquals("test-username", newConfigs.getInput("linkConfig.username").getValue());
    assertEquals("test-password", newConfigs.getInput("linkConfig.password").getValue());
    assertEquals("test-jdbcProperties", newConfigs.getInput("linkConfig.jdbcProperties").getValue());
  }
}
